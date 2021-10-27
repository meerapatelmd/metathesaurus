/**************************************************************************
* Derive entire hierarchies from UMLS Metathesaurus MRHIER Table  
* Authors: Meera Patel  
* Date: 2021-10-27 
*					
*					
* | MRHIER | --> | MRHIER | --> | MRHIER_STR | + | MRHIER_STR_EXCL |
*
*
* | MRHIER | --> | MRHIER |
* ptr_id is added to the source table. ptr_id is the source MRHIER's row number. 
* It is added as an identifier for each unique AUI-RELA-PTR (ptr: Path To Root). 
* Note that unlike the identifiers provided 
* by the UMLS, this one cannot be used across different Metathesaurus 
* versions. 
*
* | MRHIER | --> | MRHIER_STR | + | MRHIER_STR_EXCL |
* MRHIER is then processed to replace the decimal-separated `ptr` string into 
* individual atoms (`aui`) and mapped to the atom's `str` value. Any missing 
* `ptr` values in `MRHIER_STR` are accounted for in the `MRHIER_STR_EXCL` table.
**************************************************************************/


/**************************************************************************
LOG TABLES
Process log table logs the processing. 
Setup log table logs the final `MRHIER`, `MRHIER_STR`, and `MRHIER_STR_EXCL` tables.  
Both are setup if it does not already exist.  
**************************************************************************/


CREATE TABLE IF NOT EXISTS public.process_umls_mrhier_log (
    process_start_datetime timestamp without time zone,
    process_stop_datetime timestamp without time zone,
    mth_version character varying(255),
    mth_release_dt character varying(255),
    sab character varying(255),
    target_schema character varying(255),
    source_table character varying(255),
    target_table character varying(255),
    source_row_ct numeric,
    target_row_ct numeric
);

CREATE TABLE IF NOT EXISTS public.setup_umls_mrhier_log (
    sum_datetime timestamp without time zone,
    mth_version character varying(255),
    mth_release_dt character varying(255),
    target_schema character varying(255),
    target_table character varying(255),
    target_row_ct numeric
);

/**************************************************************************
Logging Functions
**************************************************************************/

create or replace function get_log_timestamp() 
returns timestamp without time zone 
language plpgsql
as
$$
declare 
  log_timestamp timestamp without time zone; 
begin 
  SELECT date_trunc('second', now()::timestamp) 
  INTO log_timestamp;
  
  RETURN log_timestamp;
END;  
$$;

create or replace function get_umls_mth_version() 
returns varchar 
language plpgsql
as
$$
declare 
	umls_mth_version varchar; 
begin 
	SELECT sm_version 
	INTO umls_mth_version
	FROM public.setup_mth_log 
	WHERE sm_datetime IN (SELECT MAX(sm_datetime) FROM public.setup_mth_log);
  
  	RETURN umls_mth_version;
END;  
$$;

create or replace function get_umls_mth_dt() 
returns varchar 
language plpgsql
as
$$
declare 
	umls_mth_dt varchar; 
begin 
	SELECT sm_release_date 
	INTO umls_mth_dt
	FROM public.setup_mth_log 
	WHERE sm_datetime IN (SELECT MAX(sm_datetime) FROM public.setup_mth_log);
  
  	RETURN umls_mth_dt;
END;  
$$;


create or replace function get_row_count(_tbl varchar) 
returns bigint 
language plpgsql 
AS 
$$
DECLARE 
  row_count bigint;
BEGIN 
  EXECUTE 
    format('
	  SELECT COUNT(*) 
	  FROM %s;
	 ',
	 _tbl)
  INTO row_count; 
  
  RETURN row_count;
END;
$$;



create or replace function check_if_requires_processing(umls_mth_version varchar, source_table varchar, target_table varchar) 
returns boolean 
language plpgsql
as
$$
declare 
    row_count integer;
	requires_processing boolean; 
begin 
	EXECUTE 
	  format(
	    '
		SELECT COUNT(*) 
		FROM public.process_umls_mrhier_log l 
		WHERE 
		  l.mth_version = ''%s'' AND 
		  l.source_table = ''%s'' AND 
		  l.target_table = ''%s'' AND 
		  l.process_stop_datetime IS NOT NULL
		  ;		
	    ',
	    umls_mth_version,
	    source_table, 
	    target_table
	  )
	  INTO row_count;

	  
	IF row_count = 0 
	  THEN requires_processing := TRUE;
	  ELSE requires_processing := FALSE;
	END IF;
  
  	RETURN requires_processing;
END;  
$$;

-- 	  raise notice '[%] %/% % (% ptrs)', start_time, iteration, total_iterations, sab, f.count;
create or replace function notify_iteration(iteration int, total_iterations int, report varchar, report_size int, report_size_units varchar) 
returns void
language plpgsql
as
$$
declare 
  notice_timestamp timestamp;
begin 
  SELECT get_log_timestamp() 
  INTO notice_timestamp
  ;
  
  RAISE NOTICE '[%] %/% % (% %)', notice_timestamp, iteration, total_iterations, report, report_size, report_size_units;
END;  
$$;

create or replace function notify_start(report varchar) 
returns void
language plpgsql
as
$$
declare 
  notice_timestamp timestamp;
begin 
  SELECT get_log_timestamp() 
  INTO notice_timestamp
  ;
  
  RAISE NOTICE '[%] Started %', notice_timestamp, report;
END;  
$$;

create or replace function notify_completion(report varchar) 
returns void
language plpgsql
as
$$
declare 
  notice_timestamp timestamp;
begin 
  SELECT get_log_timestamp() 
  INTO notice_timestamp
  ;
  
  RAISE NOTICE '[%] Completed %', notice_timestamp, report;
END;  
$$;


create or replace function notify_timediff(report varchar, start_timestamp timestamp, stop_timestamp timestamp) 
returns void 
language plpgsql 
as 
$$
begin 
	RAISE NOTICE '% required %s to complete.', report, stop_timestamp - start_timestamp;
end;
$$
;

create or replace function sab_to_tablename(sab varchar) 
returns varchar 
language plpgsql 
as 
$$
declare 
  tablename varchar;
begin 
	SELECT REGEXP_REPLACE(sab, '[[:punct:]]', '_', 'g') INTO tablename;
	
	RETURN tablename;
end;
$$
;
                                        
/**************************************************************************
If the current UMLS Metathesaurus version is not logged for 
the transfer of the MIRHIER table, it is copied to the 
`umls_mrhier` schema with the addition of a `ptr_id` for 
each row number.
**************************************************************************/  

DO
$$
DECLARE 
	requires_processing boolean;
	start_timestamp timestamp;
	stop_timestamp timestamp;
	mth_version varchar;
	mth_date varchar;
	source_rows bigint;
	target_rows bigint;
BEGIN  
	SELECT check_if_requires_processing('2021AA', 'MRHIER', 'MRHIER') 
	INTO requires_processing;
	
  	IF requires_processing THEN 
  	
  		SELECT get_log_timestamp() 
  		INTO start_timestamp
  		;
  		
  		PERFORM notify_start('processing MRHIER'); 
  		
  		
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
		ADD CONSTRAINT xpk_mrhier 
		PRIMARY KEY (ptr_id);
		
		CREATE INDEX x_mrhier_sab ON umls_mrhier.mrhier(sab);
		CREATE INDEX x_mrhier_aui ON umls_mrhier.mrhier(aui);
		CREATE INDEX x_mrhier_code ON umls_mrhier.mrhier(code);
		
		DROP TABLE umls_mrhier.tmp_mrhier; 
		
		PERFORM notify_completion('processing MRHIER'); 
		
		SELECT get_log_timestamp() 
		INTO stop_timestamp
		; 
		
		SELECT get_umls_mth_version()
		INTO mth_version
		;
		
		SELECT get_umls_mth_dt() 
		INTO mth_date
		;
		
		SELECT get_row_count('mth.mrhier') 
		INTO source_rows
		;
		
		SELECT get_row_count('umls_mrhier.mrhier') 
		INTO target_rows
		;
		
		EXECUTE 
		  format(
		    '
			INSERT INTO public.process_umls_mrhier_log 
			VALUES (
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  NULL, 
			  ''umls_mrhier'', 
			  ''MRHIER'', 
			  ''MRHIER'', 
			  ''%s'',
			  ''%s''
			',
			  start_timestamp, 
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  source_rows,
			  target_rows);
	END IF;
end;
$$
;


SELECT * 
FROM public.process_umls_mrhier_log;

/*-------------------------------------------------------------- 
CREATE INITIAL LOOKUP TABLEs
Create lookup table between all the hierarchy vocabularies (`sab` 
in MRHIER) and a cleaned up version of the `sab` value 
to be used as its tablename (some `sab` values could have 
punctuation that is forbidden in table names). 
--------------------------------------------------------------*/
DROP TABLE IF EXISTS umls_mrhier.lookup_eng; 
CREATE TABLE umls_mrhier.lookup_eng (
    sab character varying(40)
);

INSERT INTO umls_mrhier.lookup_eng 
SELECT DISTINCT sab FROM mth.mrconso WHERE lat = 'ENG' ORDER BY sab;


   
DROP TABLE IF EXISTS umls_mrhier.lookup; 

CREATE TABLE umls_mrhier.lookup (
    hierarchy_sab character varying(40),
    hierarchy_table text,
    count bigint
);



WITH df as (                                                 
      SELECT 
	    h.sab AS hierarchy_sab, 
	    sab_to_tablename(h.sab) AS hierarchy_table,
	    COUNT(*) 
	  FROM mth.mrhier h
	  INNER JOIN umls_mrhier.lookup_eng eng 
	  ON eng.aui = h.aui 
	  GROUP BY h.sab
	  HAVING COUNT(*) > 1 
	  ORDER BY COUNT(*) 
)

INSERT INTO umls_mrhier.lookup 
SELECT * 
FROM df
ORDER BY count -- ordered so that when writing tables later on, can see that the script is working fine over multiple small tables at first
;

SELECT * FROM umls_mrhier.lookup;



/*-----------------------------------------------------------    
PROCESS PTR
For each unique `sab` in the MRHIER table, 
the decimal-separated `ptr` string is parsed along with its 
ordinality as `ptr_level`. The parsed individual `ptr_aui` 
is joined to MRCONSO to add the `ptr_code` and `ptr_str`. 
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
	  		INSERT INTO public.process_umls_mrhier_log  
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

/*-----------------------------------------------------------    
SPLIT SNOMEDCT AT LEVEL 2  
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
  		INSERT INTO public.process_umls_mrhier_log  
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

/*----------------------------------------------------------- 
REFRESH LOOKUP
The lookup is updated with the SNOMEDCT subset tables 
and counts.
-----------------------------------------------------------*/

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
	  		INSERT INTO public.process_umls_mrhier_log  
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


/*-----------------------------------------------------------
/ PIVOT 
/ Each table is pivoted on ptr_id and its attributes to 
/ compile classifications in at the row level.
/-----------------------------------------------------------*/

ALTER TABLE umls_mrhier.lookup 
RENAME TO tmp_lookup;

CREATE TABLE umls_mrhier.lookup AS (
  SELECT 
  	*, 
  	SUBSTRING(CONCAT('pivot_', hierarchy_table), 1, 60) AS pivot_table
  FROM umls_mrhier.tmp_lookup
);

DROP TABLE umls_mrhier.tmp_lookup;
SELECT * FROM umls_mrhier.lookup;

/*-----------------------------------------------------------
/ A second pivot lookup is made to construct the crosstab function 
/ call
/-----------------------------------------------------------*/
DROP TABLE IF EXISTS umls_mrhier.pivot_lookup;
CREATE TABLE  umls_mrhier.pivot_lookup (	
  hierarchy_table varchar(255),
  pivot_table varchar(255),
  sql_statement text
)
;


/*-----------------------------------------------------------
/ A crosstab function call is created to pivot each table 
/ based on the maximum `ptr_level` in that table. This is 
/ required to pass the subsequent column names as the 
/ argument to the crosstab function.
/-----------------------------------------------------------*/
do
$$
declare
    f record;
    h_tbl varchar(255);
    p_tbl varchar(255);
    max_level int;
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
      h_tbl := f.hierarchy_table;
      p_tbl := f.pivot_table;
      ct  := f.count;
      start_time := date_trunc('second', timeofday()::timestamp);
      
      raise notice '[%] %/% %', start_time, iteration, total_iterations, h_tbl;
      
	  EXECUTE format('SELECT MAX(ptr_level) FROM umls_mrhier.%s', h_tbl)  
	    INTO max_level;
	    
	  
	  EXECUTE 
	    format(
	      '
	      WITH seq1 AS (SELECT generate_series(1,%s) AS series),
	      seq2 AS (
	      	SELECT 
	      		''%s'' AS hierarchy_table, 
	      		''%s'' AS pivot_table,
	      		STRING_AGG(CONCAT(''level_'', series, '' text''), '', '') AS crosstab_ddl
	      	FROM seq1 
	      	GROUP BY hierarchy_table, pivot_table),
	      seq3 AS (
	      	SELECT
	      		hierarchy_table,
	      		pivot_table,
	      		CONCAT(''ptr_id BIGINT, '', crosstab_ddl) AS crosstab_ddl 
	      	FROM seq2
	      ), 
	      seq4 AS (
	        SELECT 
	          hierarchy_table,
	          pivot_table, 
	          '''''''' || CONCAT(''SELECT ptr_id, ptr_level, ptr_str FROM umls_mrhier.'', hierarchy_table, '' ORDER BY 1,2'') || '''''''' AS crosstab_arg1,
	          '''''''' || CONCAT(''SELECT DISTINCT ptr_level FROM umls_mrhier.'', hierarchy_table, '' ORDER BY 1'') || '''''''' AS crosstab_arg2, 
	          crosstab_ddl
	         FROM seq3
	      ),
	      seq5 AS (
	      	SELECT 
	      	  hierarchy_table,
	      	  pivot_table,
	      	  ''DROP TABLE IF EXISTS umls_mrhier.'' || pivot_table || '';'' || '' CREATE TABLE umls_mrhier.'' || pivot_table || '' AS (SELECT * FROM CROSSTAB('' || crosstab_arg1 || '','' || crosstab_arg2 || '') AS ('' || crosstab_ddl || ''));'' AS sql_statement
	      	  FROM seq4
	      
	      )
	      
	      INSERT INTO umls_mrhier.pivot_lookup 
	      SELECT * FROM seq5
	      ;
	      ',
	      max_level,
	      h_tbl, 
	      p_tbl
	      
	    );
	
	  end_time := date_trunc('second', timeofday()::timestamp); 
      raise notice '% complete (%)', h_tbl, end_time - start_time;
   
    end loop;
end;
$$
;


SELECT * 
FROM umls_mrhier.pivot_lookup;



do 
$$
declare 
  	sql_statement text; 
  	f record; 
  	p_tbl varchar(255);
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


  	SELECT COUNT(*) INTO total_iterations FROM umls_mrhier.pivot_lookup;
  	for f in select ROW_NUMBER() OVER() AS iteration, pl.* from umls_mrhier.pivot_lookup pl
 	LOOP  
      iteration := f.iteration;
      p_tbl := f.pivot_table;
      h_tbl := f.hierarchy_table;
      start_time := date_trunc('second', timeofday()::timestamp);
      
      raise notice '[%] %/% %', start_time, iteration, total_iterations, p_tbl;
    sql_statement := f.sql_statement;
    EXECUTE sql_statement;
    
    
    EXECUTE 
      format(
      	'
      	ALTER TABLE umls_mrhier.%s RENAME TO tmp_%s; 
      	CREATE TABLE umls_mrhier.%s AS (
	      	SELECT DISTINCT
	      	  h.aui,
	      	  h.code,
	      	  h.str, 
	      	  t.*
	      	FROM umls_mrhier.%s h 
	      	LEFT JOIN umls_mrhier.tmp_%s t 
	      	ON t.ptr_id = h.ptr_id
      	);
      	DROP TABLE umls_mrhier.tmp_%s;
      	',
      		p_tbl, 
      		p_tbl, 
      		p_tbl,
      		h_tbl, 
      		p_tbl,
      		p_tbl);
    
    EXECUTE format('SELECT count(*) FROM umls_mrhier.%s', h_tbl)  
	    INTO ct;
    EXECUTE format('SELECT count(*) FROM umls_mrhier.%s', p_tbl)  
	    INTO final_ct;
  
	  EXECUTE 
	  	format(
	  		'
	  		INSERT INTO public.process_umls_mrhier_log  
	  		VALUES (''%s'', ''%s'', ''%s'', ''%s'', ''umls_mrhier'', ''%s'', ''%s'', %s); 
	  		',
	  			log_datetime, 
	  			log_mth_version, 
	  			log_mth_release_dt, 
	  			h_tbl, 
	  			p_tbl, 
	  			ct, 
	  			final_ct);
    
    	  end_time := date_trunc('second', timeofday()::timestamp); 
      raise notice '% complete (%)', p_tbl, end_time - start_time;
    
    end loop;
END;
$$
;


DROP TABLE IF EXISTS umls_mrhier.ext_lookup;
CREATE TABLE umls_mrhier.ext_lookup (
  extended_table varchar(255), 
  max_ptr_level int
);

do 
$$
declare 
  	sql_statement text; 
  	f record; 
  	e_tbl varchar(255);
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
  	for f in select ROW_NUMBER() OVER() AS iteration, pl.* from umls_mrhier.lookup pl
 	LOOP  
      iteration := f.iteration;
      e_tbl := f.extended_table;
      h_tbl := f.hierarchy_table;
      start_time := date_trunc('second', timeofday()::timestamp);
      
      raise notice '[%] %/% %', start_time, iteration, total_iterations, e_tbl;

    
    
    EXECUTE 
      format(
      	'
      	INSERT INTO umls_mrhier.ext_lookup 
      	SELECT 
      	 ''%s'' AS extended_table, 
      	 MAX(ptr_level) AS max_ptr_level 
      	 FROM umls_mrhier.%s
      	 ;
      	',
      		e_tbl, 
      		e_tbl);
    
    	  end_time := date_trunc('second', timeofday()::timestamp); 
      raise notice '% complete (%)', e_tbl, end_time - start_time;
    
    end loop;
END;
$$
;

DO 
$$
DECLARE 
  abs_max_ptr_level int;
  processed_mrhier_ddl text;
  pivot_table text;
  f record;
  	start_time timestamp;
	end_time timestamp;
	iteration int;
	total_iterations int;
	log_datetime timestamp;
	log_mth_version varchar(25);
	log_mth_release_dt timestamp;
BEGIN 
  SELECT max(max_ptr_level) 
  INTO abs_max_ptr_level
  FROM umls_mrhier.ext_lookup;
  
  EXECUTE 
  format('
  DROP TABLE IF EXISTS umls_mrhier.ddl_lookup;
  CREATE TABLE umls_mrhier.ddl_lookup (
     ddl text
  );
  
  WITH seq1 AS (SELECT generate_series(1, %s) AS series), 
  seq2 AS (
    SELECT 
      STRING_AGG(CONCAT(''level_'', series, '' text''), '', '') AS ddl 
      FROM seq1 
  )
   
  INSERT INTO umls_mrhier.ddl_lookup
  SELECT ddl 
  FROM seq2
  ;',
  abs_max_ptr_level);
  
  SELECT ddl
  INTO processed_mrhier_ddl
  FROM umls_mrhier.ddl_lookup;
  
  EXECUTE 
    format(
    '
    DROP TABLE IF EXISTS umls_mrhier.mrhier_str;
    CREATE TABLE umls_mrhier.mrhier_str (
      aui varchar(12),
      code text,
      str text,
      ptr_id bigint,
      %s
    );
    ',
    processed_mrhier_ddl
    );
    
  DROP TABLE umls_mrhier.ddl_lookup;
  
  SELECT COUNT(*) INTO total_iterations FROM umls_mrhier.pivot_lookup;
  for f in select ROW_NUMBER() OVER() AS iteration, pl.* from umls_mrhier.pivot_lookup pl
  loop 
    iteration := f.iteration;
    pivot_table := f.pivot_table; 
    
          start_time := date_trunc('second', timeofday()::timestamp);
      
      raise notice '[%] %/% %', start_time, iteration, total_iterations, pivot_table;
      
    EXECUTE 
      format('
      INSERT INTO umls_mrhier.mrhier_str 
      SELECT * FROM umls_mrhier.%s
      ',
      pivot_table
      ); 
      
          	  end_time := date_trunc('second', timeofday()::timestamp); 
      raise notice '% complete (%)', pivot_table, end_time - start_time;
   END loop;
  
  
END;
$$
;

ALTER TABLE umls_mrhier.mrhier_str
ADD CONSTRAINT xpk_mrhier_str 
PRIMARY KEY (ptr_id);

CREATE INDEX x_mrhier_str_aui ON umls_mrhier.mrhier_str(aui);
CREATE INDEX x_mrhier_str_code ON umls_mrhier.mrhier_str(code);



/*
MRHIER_EXCL Table  
Table that includes any source MRHIER `ptr` that did not make it 
to the `MRHIER_STR` table.

- Only vocabularies where `LAT = 'ENG'` and not 'SRC' in MRCONSO table 
*/

WITH a AS (
	SELECT m1.sab,m1.ptr_id, CASE WHEN m1.ptr IS NULL THEN TRUE ELSE FALSE END ptr_is_null  
	FROM umls_mrhier.mrhier m1 
	LEFT JOIN umls_mrhier.mrhier_str m2 
	ON m1.ptr_id = m2.ptr_id 
	WHERE 
	  m2.ptr_id IS NULL AND 
	  m1.sab IN (SELECT DISTINCT sab FROM mth.mrconso WHERE lat = 'ENG' AND sab <> 'SRC') -- 'SRC' concepts are basically the source vocabulary and have NULL `ptr` values
)

SELECT a.sab, a.ptr_is_null, COUNT(*)
FROM a 
GROUP BY a.sab, a.ptr_is_null
;


DROP TABLE IF EXISTS umls_mrhier.mrhier_str_excl; 
CREATE TABLE umls_mrhier.mrhier_str_excl AS (
	SELECT m1.*
	FROM umls_mrhier.mrhier m1 
	LEFT JOIN umls_mrhier.mrhier_str m2 
	ON m1.ptr_id = m2.ptr_id 
	WHERE 
	  m2.ptr_id IS NULL AND 
	  m1.ptr IS NOT NULL AND
	  m1.sab IN (SELECT DISTINCT sab FROM mth.mrconso WHERE lat = 'ENG' AND sab <> 'SRC') -- 'SRC' concepts are basically the source vocabulary and have NULL `ptr` values 
	ORDER BY m1.sab DESC -- Arbitrarily in descending order to include SNOMEDCT_US first
)
;


ALTER TABLE umls_mrhier.mrhier_str_excl
ADD CONSTRAINT xpk_mrhier_str_excl
PRIMARY KEY (ptr_id);

CREATE INDEX x_mrhier_str_excl_aui ON umls_mrhier.mrhier_str_excl(aui);
CREATE INDEX x_mrhier_str_excl_code ON umls_mrhier.mrhier_str_excl(code);
CREATE INDEX x_mrhier_str_excl_sab ON umls_mrhier.mrhier_str_excl(sab);
