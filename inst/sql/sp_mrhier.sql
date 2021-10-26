/* 
Derive entire hierarchies from UMLS Metathesaurus MRHIER Table  
Clinical Informatics  
PostgreSQL 

ptr_id is added as an identifier for each unique AUI-RELA-PTR 
(ptr: Path To Root). Note that unlike the identifiers provided 
by the UMLS, this one cannot be used across different Metathesaurus 
versions. 
*/


-----
-- LOG  
----- 
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

                                                             
-----------------------------------------------------------
-- 1. Add `ptr_id` field  
-----------------------------------------------------------       

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

DROP TABLE umls_mrhier.tmp_mrhier; 


-----------------------------------------------------------
-- 2. Subset MRHIER with `ptr_id` by vocabulary  
-----------------------------------------------------------    
-- Create lookup table between all the hierarchy vocabularies 
-- and a cleaned up version of their string representation as their 
-- destination table name to loop over. 
-----------------------------------------------------------       
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
		  ',
		  	tbl, 
		  	tbl, 
		  	sab,
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


ALTER TABLE umls_mrhier.lookup 
RENAME TO tmp_lookup;

CREATE TABLE umls_mrhier.lookup AS (
  SELECT 
  	*, 
  	CONCAT('collapsed_', hierarchy_table) AS collapsed_table
  FROM umls_mrhier.tmp_lookup
);

DROP TABLE umls_mrhier.tmp_lookup;

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
      tbl := f.collapsed_table;
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
  ',tbl, tbl, h_tbl, h_tbl, tbl);
  
  EXECUTE format('SELECT count(*) FROM umls_mrhier.%s', tbl)  
    INTO final_ct;
  
  EXECUTE 
  	format(
  		'
  		INSERT INTO public.setup_umls_mrhier_log  
  		VALUES (''%s'', ''%s'', ''%s'', ''%s'', ''umls_mrhier'', ''%s'', ''%s'', %s); 
  		',
  			log_datetime, 
  			log_mth_version, log_mth_release_dt, h_tbl, tbl, ct, final_ct);
  
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


