/* 
Derive entire hierarchies from UMLS Metathesaurus MRHIER Table  
DBMS: Postgres 

ptr_id is added as an identifier for each unique AUI-RELA-PTR 
(ptr: Path To Root). Note that unlike the identifiers provided 
by the UMLS, this one cannot be used across different Metathesaurus 
versions. 
*/


/*
The log table is setup if it does not already exist.  
*/
 
CREATE TABLE IF NOT EXISTS public.setup_umls_mrhier_log (
    sum_datetime timestamp without time zone,
    sum_mth_version character varying(255),
    sum_mth_release_dt character varying(255),
    sab character varying(255),
    mrhier_schema character varying(255),
    mrhier_table character varying(255),
    source_row_ct numeric,
    table_row_ct numeric
);

                                                             
/*
MRHIER table is copied to the `umls_mrhier` schema with the 
addition of a `ptr_id` for each row number.
*/    

DROP SCHEMA umls_mrhier CASCADE;
CREATE SCHEMA umls_mrhier; 
DROP TABLE IF EXISTS umls_mrhier.tmp_mrhier; 
CREATE TABLE umls_mrhier.tmp_mrhier AS (
	SELECT DISTINCT 
	  m.AUI,
	  c.CODE, 
	  c.SAB,
	  c.STR,
	  m.RELA, 
	  m.PTR
	 FROM mth.mrhier m 
	 INNER JOIN mth.mrconso c 
	 ON c.aui = m.aui 
);


DROP TABLE IF EXISTS umls_mrhier.mrhier; 
CREATE TABLE umls_mrhier.mrhier AS (
   SELECT ROW_NUMBER() OVER() AS ptr_id, m.* 
   FROM umls_mrhier.tmp_mrhier m 
)
;

ALTER TABLE umls_mrhier.mrhier 
ADD CONSTRAINT xpk_ptr 
PRIMARY KEY (ptr_id);

CREATE INDEX x_mrhier_sab ON umls_mrhier.mrhier(sab);
CREATE INDEX x_mrhier_aui ON umls_mrhier.mrhier(aui);
CREATE INDEX x_mrhier_code ON umls_mrhier.mrhier(code);

ANALYZE umls_mrhier.mrhier;

DROP TABLE umls_mrhier.tmp_mrhier; 


/*
Create lookup table between all the hierarchy vocabularies 
and a cleaned up version of their string representation as their 
destination table name to loop over. 
*/
   
DROP TABLE IF EXISTS umls_mrhier.lookup; 

CREATE TABLE umls_mrhier.lookup (
    hierarchy_sab character varying(40),
    hierarchy_table text,
    count bigint
);



WITH df as (                                                 
      SELECT 
	    h.sab AS hierarchy_sab, 
	    REGEXP_REPLACE(h.sab, '[[:punct:]]', '_', 'g') AS hierarchy_table,
	    COUNT(*) 
	  FROM mth.mrhier h
	  INNER JOIN mth.mrconso c 
	  ON c.aui = h.aui 
	  WHERE 
	    c.lat = 'ENG' 
	  GROUP BY h.sab
	  HAVING COUNT(*) > 1 
	  ORDER BY COUNT(*) 
)

INSERT INTO umls_mrhier.lookup (hierarchy_sab, hierarchy_table, count)
SELECT * 
FROM df
ORDER BY count
;

SELECT * FROM umls_mrhier.lookup;



/*-----------------------------------------------------------    
Parse the decimal-separated `ptr` string along with 
ordinality as `ptr_level` and join the resulting `ptr_aui` 
to MRCONSO to include the `ptr_code` and `ptr_str`. 
-----------------------------------------------------------*/
  
do
$$
declare
    f record;
    tbl varchar(255);
    sab varchar(255);
    ct integer;
    final_ct integer;
    start_time timestamp;
    end_time timestamp;
    iteration int;
    total_iterations int;
    log_datetime timestamp;
    log_mth_version varchar(25);
    log_mth_release_dt timestamp;
begin
	log_datetime := date_trunc('second', now()::timestamp);  
	SELECT sm_version 
	INTO log_mth_version
	FROM public.setup_mth_log 
	WHERE sm_datetime IN 
	  (SELECT MAX(sm_datetime) FROM public.setup_mth_log);
	  
	SELECT sm_release_date 
	INTO log_mth_release_dt
	FROM public.setup_mth_log 
	WHERE sm_datetime IN 
	  (SELECT MAX(sm_datetime) FROM public.setup_mth_log);


    SELECT COUNT(*) INTO total_iterations FROM umls_mrhier.lookup;
    for f in select ROW_NUMBER() OVER() AS iteration, l.* from umls_mrhier.lookup l    
    loop 
      iteration := f.iteration;
      tbl := f.hierarchy_table;
      sab := f.hierarchy_sab;
      ct  := f.count;
      start_time := date_trunc('second', timeofday()::timestamp);
      
      
      
	  raise notice '[%] %/% % (% ptrs)', start_time, iteration, total_iterations, sab, f.count;
	  EXECUTE
	   format(
		  '
		  DROP TABLE IF EXISTS umls_mrhier.%s; 
		  CREATE TABLE  umls_mrhier.%s (
		    ptr_id INTEGER NOT NULL,
		    ptr text NOT NULL,
		  	aui varchar(12), 
		  	code varchar(100), 
		  	str text, 
		  	rela varchar(100), 
		  	ptr_level INTEGER NOT NULL,
		  	ptr_aui varchar(12) NOT NULL,
		  	ptr_code varchar(100),
		  	ptr_str text
		  );
		  
		  WITH relatives0 AS (
			SELECT DISTINCT m.ptr_id, s1.aui, s1.code, s1.str, m.rela, m.ptr 
			FROM umls_mrhier.mrhier m
			INNER JOIN mth.mrconso s1 
			ON s1.aui = m.aui 
			WHERE m.sab = ''%s''  
		  ),
		  relatives1 AS (
		  	SELECT ptr_id, ptr, aui, code, str, rela, unnest(string_to_array(ptr, ''.'')) AS ptr_aui
		  	FROM relatives0 r0 
		  	ORDER BY ptr_id
		  ),
		  relatives2 AS (
		  	SELECT r1.*, ROW_NUMBER() OVER (PARTITION BY ptr_id) AS ptr_level
		  	FROM relatives1 r1 
		  ),
		  relatives3 AS (
		  	SELECT r2.*, m.code AS ptr_code, m.str AS ptr_str 
		  	FROM relatives2 r2
		  	LEFT JOIN mth.mrconso m 
		  	ON m.aui = r2.ptr_aui
		  )
		  
		  INSERT INTO umls_mrhier.%s  
		  SELECT DISTINCT
		    ptr_id,
		    ptr,
		  	aui, 
		  	code, 
		  	str, 
		  	rela, 
		  	ptr_level,
		  	ptr_aui,
		  	ptr_code,
		  	ptr_str 
		  FROM relatives3  
		  ORDER BY ptr_id, ptr_level
		  ;
		  
		  ALTER TABLE umls_mrhier.%s 
		  ADD CONSTRAINT xpk_%s PRIMARY KEY (ptr_id);
		  
		  CREATE UNIQUE INDEX idx_%s_ptr 
		  ON umls_mrhier.%s (ptr_id, ptr_level);
		  CLUSTER umls_mrhier.%s USING idx_%s_ptr;
		  
		  CREATE INDEX x_%s_aui ON umls_mrhier.%s(aui);
		  CREATE INDEX x_%s_code ON umls_mrhier.%s(code);
		  CREATE INDEX x_%s_ptr_aui ON umls_mrhier.%s(ptr_aui);	
		  CREATE INDEX x_%s_ptr_code ON umls_mrhier.%s(ptr_code);  
		  
		  ANALYZE VERBOSE umls_mrhier.%s;		    
		  ',
		  	tbl, 
		  	tbl, 
		  	sab,
		  	tbl,
		  	tbl,
		  	tbl,
		  	tbl,
		  	tbl, 
		  	tbl, 
		  	tbl,
		  	tbl,
		  	tbl,
		  	tbl,
		  	tbl,
		  	tbl,
		  	tbl,
		  	tbl,
		  	tbl,
		  	tbl
		  	);
  
	  EXECUTE format('SELECT count(*) FROM umls_mrhier.%s', tbl)  
	    INTO final_ct;
  
  
	  EXECUTE 
	  	format(
	  		'
	  		INSERT INTO public.setup_umls_mrhier_log  
	  		VALUES (''%s'', ''%s'', ''%s'', ''%s'', ''umls_mrhier'', ''%s'', ''%s'', %s); 
	  		',
	  			log_datetime, 
	  			log_mth_version, 
	  			log_mth_release_dt, 
	  			sab, 
	  			tbl, 
	  			ct, 
	  			final_ct);
  
   	  end_time := date_trunc('second', timeofday()::timestamp);
   
   	  raise notice '% complete (%)', tbl, end_time - start_time;
   
    end loop;
end;
$$
;

-- ANALYZE;

/*-----------------------------------------------------------    
Split SNOMEDCT By Level 2  
The SNOMEDCT_US table is too large to work with downstream 
and it is subset here by the 2nd level concept to make it 
more manageable.
-----------------------------------------------------------*/  

DROP TABLE IF EXISTS umls_mrhier.tmp_lookup; 
CREATE TABLE umls_mrhier.tmp_lookup (
    hierarchy_table text,
    ptr_aui varchar(12), 
    ptr_code varchar(255), 
    ptr_str varchar(255),
    updated_hierarchy_table varchar(255),
    level_2_count bigint
);

INSERT INTO umls_mrhier.tmp_lookup 
SELECT 
	'SNOMEDCT_US' AS hierarchy_table,
	ptr_aui, 
	ptr_code, 
	ptr_str, 
	SUBSTRING(
	  CONCAT(
	    'SNOMEDCT_US_', 
	    REGEXP_REPLACE(ptr_str, '[[:punct:]]| or | ', '', 'g')), 
	  1, 
	  -- Ensure that the tablename character count is 
	  -- within normal limits
	  60) AS updated_hierarchy_table, 
	COUNT(*) AS level_2_count
FROM umls_mrhier.snomedct_us 
WHERE ptr_level = 2 
GROUP BY ptr_aui, ptr_code, ptr_str 
ORDER BY COUNT(*)
;

SELECT * FROM umls_mrhier.tmp_lookup;


DO
$$
declare
    f record;
    aui varchar(255);
    h_tbl varchar(255);
    ct integer;
    final_ct integer;
    start_time timestamp;
    end_time timestamp;
    iteration int;
    total_iterations int;
    log_datetime timestamp;
    log_mth_version varchar(25);
    log_mth_release_dt timestamp;
begin
	log_datetime := date_trunc('second', now()::timestamp);  
	SELECT sm_version 
	INTO log_mth_version
	FROM public.setup_mth_log 
	WHERE sm_datetime IN 
	  (SELECT MAX(sm_datetime) FROM public.setup_mth_log);
	  
	SELECT sm_release_date 
	INTO log_mth_release_dt
	FROM public.setup_mth_log 
	WHERE sm_datetime IN 
	  (SELECT MAX(sm_datetime) FROM public.setup_mth_log);


    SELECT COUNT(*) INTO total_iterations FROM umls_mrhier.tmp_lookup;
    for f in select ROW_NUMBER() OVER() AS iteration, l.* from umls_mrhier.tmp_lookup l    
    loop 
      iteration := f.iteration;
      aui := f.ptr_aui;
      h_tbl := f.updated_hierarchy_table;
      ct  := f.level_2_count;
      start_time := date_trunc('second', timeofday()::timestamp);
      
      
      
	  raise notice '[%] %/% % (% ptrs)', start_time, iteration, total_iterations, h_tbl, ct;
	  EXECUTE
	   format(
		  '
		  DROP TABLE IF EXISTS umls_mrhier.%s;
		  CREATE TABLE umls_mrhier.%s (
		  	ptr_id BIGINT NOT NULL, 
		  	ptr text NOT NULL, 
		  	aui VARCHAR(12) NOT NULL,
		  	code VARCHAR(255), 
		  	str VARCHAR(255), 
		  	rela VARCHAR(10), 
		  	ptr_level INT NOT NULL, 
		  	ptr_aui VARCHAR(12) NOT NULL, 
		  	ptr_code VARCHAR(255) NOT NULL, 
		  	ptr_str VARCHAR(255) NOT NULL
		  )
		  ;
		  
		  INSERT INTO umls_mrhier.%s 
		  	SELECT * 
		  	FROM umls_mrhier.snomedct_us 
		  	WHERE ptr_id IN (
		  		SELECT DISTINCT ptr_id 
		  		FROM umls_mrhier.snomedct_us 
		  		WHERE 
		  			ptr_level = 2 
		  			AND ptr_aui = ''%s''
		  			)
		  ;
		  ',
		  h_tbl,
		  h_tbl,
		  h_tbl,
		  aui
		  );

  
   	  end_time := date_trunc('second', timeofday()::timestamp);
   	  
   	  EXECUTE format('SELECT count(*) FROM umls_mrhier.%s', h_tbl)  
	    INTO final_ct;
   	  
  	  EXECUTE 
  	format(
  		'
  		INSERT INTO public.setup_umls_mrhier_log  
  		VALUES (''%s'', ''%s'', ''%s'', ''SNOMEDCT_US'', ''umls_mrhier'', ''%s'', ''%s'', %s); 
  		',
  			log_datetime, 
  			log_mth_version, 
  			log_mth_release_dt, 
  			h_tbl, 
  			ct, 
  			final_ct);

      raise notice '% complete (%)', h_tbl, end_time - start_time;
   
    end loop;
end;
$$
;

SELECT * FROM public.setup_umls_mrhier_log;

DROP TABLE IF EXISTS umls_mrhier.tmp_lookup2;
CREATE TABLE umls_mrhier.tmp_lookup2 AS (
	SELECT 
	  lu.hierarchy_sab, 
	  tmp.ptr_aui, 
	  tmp.ptr_code, 
	  tmp.ptr_str, 
	  COALESCE(tmp.updated_hierarchy_table, lu.hierarchy_table) AS hierarchy_table, 
	  COALESCE(tmp.level_2_count, lu.count) AS count  
	FROM umls_mrhier.lookup lu  
	LEFT JOIN umls_mrhier.tmp_lookup tmp 
	ON lu.hierarchy_table = tmp.hierarchy_table
)
;

DROP TABLE umls_mrhier.lookup; 
DROP TABLE umls_mrhier.tmp_lookup;
ALTER TABLE umls_mrhier.tmp_lookup2 RENAME TO lookup;

SELECT * FROM umls_mrhier.lookup;
SELECT * FROM public.setup_umls_mrhier_log;

/*-----------------------------------------------------------
/ EXTEND PTR PATH TO LEAF
/ The leaf of the hierarchy is found within the source concept 
/ (`aui`, `code`, and `str`). These leafs are added at the end 
/ of the path to root.  
/-----------------------------------------------------------*/

ALTER TABLE umls_mrhier.lookup 
RENAME TO tmp_lookup;

CREATE TABLE umls_mrhier.lookup AS (
  SELECT 
  	*, 
  	SUBSTRING(CONCAT('ext_', hierarchy_table), 1, 60) AS extended_table
  FROM umls_mrhier.tmp_lookup
);

DROP TABLE umls_mrhier.tmp_lookup;

SELECT * FROM umls_mrhier.lookup;

do
$$
declare
    f record;
    tbl varchar(255);
    h_tbl varchar(255);
    ct integer;
    final_ct integer;
    start_time timestamp;
    end_time timestamp;
    iteration int;
    total_iterations int;
    log_datetime timestamp;
    log_mth_version varchar(25);
    log_mth_release_dt timestamp;
begin
	log_datetime := date_trunc('second', now()::timestamp);  
	SELECT sm_version 
	INTO log_mth_version
	FROM public.setup_mth_log 
	WHERE sm_datetime IN 
	  (SELECT MAX(sm_datetime) FROM public.setup_mth_log);
	  
	SELECT sm_release_date 
	INTO log_mth_release_dt
	FROM public.setup_mth_log 
	WHERE sm_datetime IN 
	  (SELECT MAX(sm_datetime) FROM public.setup_mth_log);


    SELECT COUNT(*) INTO total_iterations FROM umls_mrhier.lookup;
    for f in select ROW_NUMBER() OVER() AS iteration, l.* from umls_mrhier.lookup l    
    loop 
      iteration := f.iteration;
      tbl := f.extended_table;
      h_tbl := f.hierarchy_table;
      ct  := f.count;
      start_time := date_trunc('second', timeofday()::timestamp);
      
      
      
	  raise notice '[%] %/% % (% ptrs)', start_time, iteration, total_iterations, h_tbl, f.count;
	  EXECUTE
	   format(
		  '
		  DROP TABLE IF EXISTS umls_mrhier.%s; 
		  CREATE TABLE  umls_mrhier.%s (
		    ptr_id INTEGER NOT NULL,
		    ptr text NOT NULL,
		  	aui varchar(12), 
		  	code varchar(100), 
		  	str text, 
		  	rela varchar(100), 
		  	ptr_level INTEGER NOT NULL,
		  	ptr_aui varchar(12) NOT NULL,
		  	ptr_code varchar(100),
		  	ptr_str text
		  );
		  
		  WITH leafs AS (
			SELECT 
			  ptr_id, 
			  ptr, 
			  aui, 
			  code, 
			  str, 
			  rela, 
			  max(ptr_level)+1 AS ptr_level, 
			  aui AS ptr_aui, 
			  code AS ptr_code, 
			  str AS ptr_str 
			FROM umls_mrhier.%s 
			GROUP BY ptr_id, ptr, aui, code, str, rela
		  ),
		  with_leafs AS (
		  	SELECT * 
		  	FROM leafs 
		  	UNION 
		  	SELECT * 
		  	FROM umls_mrhier.%s
		  )
		  
		  INSERT INTO umls_mrhier.%s  
		  SELECT * 
		  FROM with_leafs
		  ORDER BY ptr_id, ptr_level
		  ;
		  ',
		  	tbl, 
		  	tbl, 
		  	h_tbl, 
		  	h_tbl, 
		  	tbl);
  
	  EXECUTE format('SELECT count(*) FROM umls_mrhier.%s', tbl)  
	    INTO final_ct;
  
	  EXECUTE 
	  	format(
	  		'
	  		INSERT INTO public.setup_umls_mrhier_log  
	  		VALUES (''%s'', ''%s'', ''%s'', ''%s'', ''umls_mrhier'', ''%s'', ''%s'', %s); 
	  		',
	  			log_datetime, 
	  			log_mth_version, 
	  			log_mth_release_dt, 
	  			h_tbl, 
	  			tbl, 
	  			ct, 
	  			final_ct);
  
   	  end_time := date_trunc('second', timeofday()::timestamp);
   
      raise notice '% complete (%)', tbl, end_time - start_time;
   
    end loop;
end;
$$
;






select * from umls_mrhier.lookup;
from umls_mrhier.lookup;





SELECT DISTINCT
  ptr_level, 
  ptr_aui,
  ptr_code,
  ptr_str 
FROM umls_mrhier.MED_RT 
WHERE ptr_level = 2;


