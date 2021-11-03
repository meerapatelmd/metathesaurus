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
*
* To Do:  
* [ ] Cleanup scripts with functions from 1387 onward, including logging to 
*     the progress log and annotations
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


CREATE TABLE IF NOT EXISTS public.setup_umls_class_log (
    suc_datetime timestamp without time zone,
    mth_version character varying(255),
    mth_release_dt character varying(255),
    target_schema character varying(255),
    mrhier bigint,
    mrhier_str bigint, 
    mrhier_str_excl bigint
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
  SELECT date_trunc('second', timeofday()::timestamp) 
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


DROP FUNCTION check_if_requires_processing(character varying,character varying,character varying);
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


create or replace function notify_iteration(iteration int, total_iterations int, objectname varchar) 
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
  
  RAISE NOTICE '[%] %/% %', notice_timestamp, iteration, total_iterations, objectname;
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



ALTER SCHEMA umls_mrhier RENAME TO old_umls_mrhier;
CREATE SCHEMA umls_mrhier;

                                        
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
	SELECT get_umls_mth_version() 
	INTO mth_version;
	
	SELECT check_if_requires_processing(mth_version, 'MRHIER', 'MRHIER') 
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
			  ''%s'');
			',
			  start_timestamp, 
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  source_rows,
			  target_rows);
			  
		COMMIT;
	END IF;
end;
$$
;



SELECT * 
FROM public.process_umls_mrhier_log;

/*-------------------------------------------------------------- 
CREATE INITIAL LOOKUP TABLES
Create lookup table between all the hierarchy vocabularies (`sab` 
in MRHIER) and a cleaned up version of the `sab` value 
to be used as its tablename (some `sab` values could have 
punctuation that is forbidden in table names). 
--------------------------------------------------------------*/
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
	SELECT get_umls_mth_version() 
	INTO mth_version;
	
	SELECT check_if_requires_processing(mth_version, 'MRCONSO', 'LOOKUP_ENG') 
	INTO requires_processing;
	
  	IF requires_processing THEN  	
  	
  		SELECT get_log_timestamp() 
  		INTO start_timestamp
  		;
  	
  		PERFORM notify_start('processing LOOKUP_ENG');  
  		
		DROP TABLE IF EXISTS umls_mrhier.lookup_eng; 
		CREATE TABLE umls_mrhier.lookup_eng (
		    sab character varying(40)
		);
		
		INSERT INTO umls_mrhier.lookup_eng 
		SELECT DISTINCT sab 
		FROM mth.mrconso 
		WHERE lat = 'ENG' ORDER BY sab;
		
		PERFORM notify_completion('processing LOOKUP_ENG'); 
		
		SELECT get_log_timestamp() 
		INTO stop_timestamp
		; 
		
		SELECT get_umls_mth_version()
		INTO mth_version
		;
		
		SELECT get_umls_mth_dt() 
		INTO mth_date
		;
		
		SELECT get_row_count('mth.mrconso') 
		INTO source_rows
		;
		
		SELECT get_row_count('umls_mrhier.lookup_eng') 
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
			  ''MRCONSO'', 
			  ''LOOKUP_ENG'', 
			  ''%s'',
			  ''%s'');
			',
			  start_timestamp, 
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  source_rows,
			  target_rows);
			  
		COMMIT;

	END IF;
end;
$$
;


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
	SELECT get_umls_mth_version() 
	INTO mth_version;
	
	SELECT check_if_requires_processing(mth_version, 'MRHIER', 'LOOKUP_PARSE') 
	INTO requires_processing;
	
  	IF requires_processing THEN  	
  		SELECT get_log_timestamp() 
  		INTO start_timestamp
  		;
  	
  		PERFORM notify_start('processing LOOKUP_PARSE');  

   
		DROP TABLE IF EXISTS umls_mrhier.lookup_parse; 
		CREATE TABLE umls_mrhier.lookup_parse (
		    hierarchy_sab character varying(40),
		    hierarchy_table text,
		    count bigint
		);
		
		WITH df as (                                                 
		      SELECT 
			    h.sab AS hierarchy_sab, 
			    sab_to_tablename(h.sab) AS hierarchy_table,
			    COUNT(*) 
			  FROM umls_mrhier.mrhier h
			  INNER JOIN umls_mrhier.lookup_eng eng 
			  ON eng.sab = h.sab
			  GROUP BY h.sab
			  HAVING COUNT(*) > 1 
			  ORDER BY COUNT(*) 
		)
		
		INSERT INTO umls_mrhier.lookup_parse 
		SELECT * 
		FROM df
		ORDER BY count -- ordered so that when writing tables later on, can see that the script is working fine over multiple small tables at first
		;

		PERFORM notify_completion('processing LOOKUP_PARSE'); 
		
		SELECT get_log_timestamp() 
		INTO stop_timestamp
		; 
		
		SELECT get_umls_mth_version()
		INTO mth_version
		;
		
		SELECT get_umls_mth_dt() 
		INTO mth_date
		;
		
		SELECT get_row_count('umls_mrhier.mrhier') 
		INTO source_rows
		;
		
		SELECT get_row_count('umls_mrhier.lookup_parse') 
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
			  ''LOOKUP_PARSE'', 
			  ''%s'',
			  ''%s'');
			',
			  start_timestamp, 
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  source_rows,
			  target_rows);
		
		COMMIT;

	END IF;
end;
$$
;

select * from public.process_umls_mrhier_log;


/*-----------------------------------------------------------    
PROCESS PTR
For each unique `sab` in the MRHIER table, 
the decimal-separated `ptr` string is parsed along with its 
ordinality as `ptr_level`. The parsed individual `ptr_aui` 
is joined to MRCONSO to add the `ptr_code` and `ptr_str`. 
-----------------------------------------------------------*/
  
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
    f record;
    target_table varchar(255);
    source_sab varchar(255);
	iteration int;
    total_iterations int;
BEGIN  
	SELECT get_umls_mth_version() 
	INTO mth_version;
	
	SELECT COUNT(*) INTO total_iterations FROM umls_mrhier.lookup_parse;
    FOR f IN SELECT ROW_NUMBER() OVER() AS iteration, l.* FROM umls_mrhier.lookup_parse l    
    LOOP
		iteration := f.iteration;
		target_table := f.hierarchy_table;
		source_sab := f.hierarchy_sab;     
	
		SELECT check_if_requires_processing(mth_version, 'MRHIER', target_table) 
		INTO requires_processing;
	
  		IF requires_processing THEN  
  		
   			PERFORM notify_start(CONCAT('processing', ' ', source_sab, ' into table ', target_table));  
  			SELECT get_log_timestamp() 
  			INTO start_timestamp
  			;
  			
  			EXECUTE 
			format(
				'
				SELECT COUNT(*) 
				FROM umls_mrhier.mrhier 
				WHERE sab = ''%s'';
				',
					source_sab
			)
			INTO source_rows;
  	
  	  		PERFORM notify_iteration(iteration, total_iterations, source_sab || ' (' || source_rows || ' source rows)');

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
			  
			  CREATE UNIQUE INDEX idx_%s_ptr 
			  ON umls_mrhier.%s (ptr_id, ptr_level);
			  CLUSTER umls_mrhier.%s USING idx_%s_ptr;
			  
			  CREATE INDEX x_%s_aui ON umls_mrhier.%s(aui);
			  CREATE INDEX x_%s_code ON umls_mrhier.%s(code);
			  CREATE INDEX x_%s_ptr_aui ON umls_mrhier.%s(ptr_aui);	
			  CREATE INDEX x_%s_ptr_code ON umls_mrhier.%s(ptr_code);
			  CREATE INDEX x_%s_ptr_level ON umls_mrhier.%s(ptr_level);  	   	    
			  ',
			  	target_table, 
			  	target_table, 
			  	source_sab,
			  	target_table,
			  	target_table,
			  	target_table,
			  	target_table,
			  	target_table, 
			  	target_table, 
			  	target_table,
			  	target_table,
			  	target_table,
			  	target_table,
			  	target_table,
			  	target_table,
			  	target_table,
			  	target_table,
			  	target_table,
			  	target_table, 
			  	target_table
			  	);
			  	
	
  		PERFORM notify_completion(CONCAT('processing', ' ', source_sab, ' into table ', target_table));  
  			
  			
  		SELECT get_log_timestamp() 
		INTO stop_timestamp
		; 
		
		SELECT get_umls_mth_version()
		INTO mth_version
		;
		
		SELECT get_umls_mth_dt() 
		INTO mth_date
		;

		
		EXECUTE format('SELECT COUNT(*) FROM umls_mrhier.%s;', target_table) 
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
			  ''%s'', 
			  ''umls_mrhier'', 
			  ''MRHIER'', 
			  ''%s'', 
			  ''%s'',
			  ''%s'');
			',
			  start_timestamp, 
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  source_sab,
			  target_table,
			  source_rows,
			  target_rows);
		  COMMIT;
	END IF;
	END LOOP;
end;
$$
;


/*-----------------------------------------------------------    
SPLIT SNOMEDCT AT ROOT  
The SNOMEDCT_US table is too large to work with downstream 
and it is subset here by the 2nd level root concept to make it 
more manageable.
-----------------------------------------------------------*/  

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
	SELECT get_umls_mth_version() 
	INTO mth_version;
	
	SELECT check_if_requires_processing(mth_version, 'SNOMEDCT_US', 'LOOKUP_SNOMED') 
	INTO requires_processing;
	
  	IF requires_processing THEN  	
  	
  		SELECT get_log_timestamp() 
  		INTO start_timestamp
  		;
  	
  		PERFORM notify_start('processing LOOKUP_SNOMED');  
  		
  		DROP TABLE IF EXISTS umls_mrhier.lookup_snomed; 
		CREATE TABLE umls_mrhier.lookup_snomed (
		    hierarchy_table text,
		    root_aui varchar(12), 
		    root_code varchar(255), 
		    root_str varchar(255),
		    updated_hierarchy_table varchar(255),
		    root_count bigint
		);
		
		INSERT INTO umls_mrhier.lookup_snomed 
		SELECT 
			'SNOMEDCT_US' AS hierarchy_table,
			ptr_aui AS root_aui, 
			ptr_code AS root_code, 
			ptr_str AS root_str, 
			-- Ensure that the tablename character count is 
			-- within normal limits
			SUBSTRING(
			  CONCAT('SNOMEDCT_US_', REGEXP_REPLACE(ptr_str, '[[:punct:]]| or | ', '', 'g')), 
			  1, 
			  60) AS updated_hierarchy_table, 
			COUNT(*) AS root_count
		FROM umls_mrhier.snomedct_us 
		WHERE ptr_level = 2 
		GROUP BY ptr_aui, ptr_code, ptr_str 
		ORDER BY COUNT(*)
		;
		
		SELECT get_log_timestamp() 
		INTO stop_timestamp
		; 
		
		SELECT get_umls_mth_version()
		INTO mth_version
		;
		
		SELECT get_umls_mth_dt() 
		INTO mth_date
		;
		
		SELECT get_row_count('umls_mrhier.snomedct_us') 
		INTO source_rows
		;
		
		SELECT get_row_count('umls_mrhier.lookup_snomed') 
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
			  ''SNOMEDCT_US'', 
			  ''LOOKUP_SNOMED'', 
			  ''%s'',
			  ''%s'');
			',
			  start_timestamp, 
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  source_rows,
			  target_rows);
			  
			  
		PERFORM notify_completion('processing LOOKUP_SNOMED'); 

	END IF;
end;
$$
;


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
    f record;
    source_table varchar(255) := 'SNOMEDCT_US';
    target_table varchar(255);
    root_aui varchar(20);
    root_str text;
	iteration int;
    total_iterations int;
BEGIN  
	SELECT get_umls_mth_version() 
	INTO mth_version;
	
	SELECT COUNT(*) INTO total_iterations FROM umls_mrhier.lookup_snomed;
    FOR f IN SELECT ROW_NUMBER() OVER() AS iteration, l.* FROM umls_mrhier.lookup_snomed l    
    LOOP
		iteration    := f.iteration;
		target_table := f.updated_hierarchy_table;     
		source_rows  := f.root_count;
		root_str     := f.root_str;
		root_aui     := f.root_aui;
	
		SELECT check_if_requires_processing(mth_version, 'SNOMEDCT_US', target_table) 
		INTO requires_processing;
	
  		IF requires_processing THEN  
  		
   			PERFORM notify_start(CONCAT('processing table ', source_table, ' into table ', target_table));  
   			
  			SELECT get_log_timestamp() 
  			INTO start_timestamp
  			;
  			
  			PERFORM notify_iteration(iteration, total_iterations, root_str || ' (' || source_rows || ' rows)');
  			
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
				  target_table,
				  target_table,
				  target_table,
				  root_aui
				  );
				  
				  
			PERFORM notify_completion(CONCAT('processing table ', source_table, ' into table ', target_table));
			
			
			SELECT get_log_timestamp() 
			INTO stop_timestamp
			; 
			
			SELECT get_umls_mth_version()
			INTO mth_version
			;
			
			SELECT get_umls_mth_dt() 
			INTO mth_date
			;
	
			
			EXECUTE format('SELECT COUNT(*) FROM umls_mrhier.%s;', target_table) 
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
				  ''SNOMEDCT_US'', 
				  ''umls_mrhier'', 
				  ''SNOMEDCT_US'', 
				  ''%s'', 
				  ''%s'',
				  ''%s'');
				',
				  start_timestamp, 
				  stop_timestamp,
				  mth_version,
				  mth_date,
				  target_table,
				  source_rows,
				  target_rows);
			COMMIT;

    end if;
    end loop;
end;
$$
;


/*----------------------------------------------------------- 
REFRESH LOOKUP
The lookup is updated with the SNOMEDCT subset tables 
and counts.
-----------------------------------------------------------*/

DROP TABLE IF EXISTS umls_mrhier.tmp_lookup;
CREATE TABLE umls_mrhier.tmp_lookup AS (
	SELECT 
	  lu.hierarchy_sab, 
	  tmp.root_aui, 
	  tmp.root_code, 
	  tmp.root_str, 
	  COALESCE(tmp.updated_hierarchy_table, lu.hierarchy_table) AS hierarchy_table, 
	  COALESCE(tmp.root_count, lu.count) AS count  
	FROM umls_mrhier.lookup_parse lu  
	LEFT JOIN umls_mrhier.lookup_snomed tmp 
	ON lu.hierarchy_table = tmp.hierarchy_table
)
;

/*-----------------------------------------------------------
/ EXTEND PTR PATH TO LEAF
/ The leaf of the hierarchy is found within the source concept 
/ (`aui`, `code`, and `str`). These leafs are added at the end 
/ of the path to root.  
/-----------------------------------------------------------*/
DROP TABLE IF EXISTS umls_mrhier.lookup_ext;
CREATE TABLE umls_mrhier.lookup_ext AS (
  SELECT 
  	*, 
  	SUBSTRING(CONCAT('ext_', hierarchy_table), 1, 60) AS extended_table
  FROM umls_mrhier.tmp_lookup
);

DROP TABLE umls_mrhier.tmp_lookup;


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
    f record;
    sab varchar(100);
    source_table varchar(255);
    target_table varchar(255);
	iteration int;
    total_iterations int;
BEGIN  
	SELECT get_umls_mth_version() 
	INTO mth_version;
	
	SELECT COUNT(*) INTO total_iterations FROM umls_mrhier.lookup_ext;
    FOR f IN SELECT ROW_NUMBER() OVER() AS iteration, l.* FROM umls_mrhier.lookup_ext l    
    LOOP
		iteration    := f.iteration;
		source_table := f.hierarchy_table;
		target_table := f.extended_table;     
		source_rows  := f.count;
		sab          := f.hierarchy_sab;
	
		SELECT check_if_requires_processing(mth_version, source_table, target_table) 
		INTO requires_processing;
	
  		IF requires_processing THEN  


		PERFORM notify_start(CONCAT('processing table ', source_table, ' into table ', target_table));  
   			
  			SELECT get_log_timestamp() 
  			INTO start_timestamp
  			;
  			
  			PERFORM notify_iteration(iteration, total_iterations, source_table || ' (' || source_rows || ' rows)');
  			
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
			  	target_table, 
			  	target_table, 
			  	source_table, 
			  	source_table, 
			  	target_table);
				  
			PERFORM notify_completion(CONCAT('processing table ', source_table, ' into table ', target_table));
			
			
			SELECT get_log_timestamp() 
			INTO stop_timestamp
			; 
			
			SELECT get_umls_mth_version()
			INTO mth_version
			;
			
			SELECT get_umls_mth_dt() 
			INTO mth_date
			;
	
			
			EXECUTE format('SELECT COUNT(*) FROM umls_mrhier.%s;', target_table) 
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
				  ''%s'', 
				  ''umls_mrhier'', 
				  ''%s'', 
				  ''%s'', 
				  ''%s'',
				  ''%s'');
				',
				  start_timestamp, 
				  stop_timestamp,
				  mth_version,
				  mth_date,
				  sab,
				  source_table,
				  target_table,
				  source_rows,
				  target_rows);
			COMMIT;

    end if;
    end loop;
end;
$$
;



/*-----------------------------------------------------------
/ PIVOT 
/ Each table is pivoted on ptr_id and its attributes to 
/ compile classifications in at the row level.
/-----------------------------------------------------------*/

ALTER TABLE umls_mrhier.lookup_ext 
RENAME TO tmp_lookup;

CREATE TABLE umls_mrhier.lookup AS (
  SELECT 
  	*, 
  	SUBSTRING(CONCAT('tmp_pivot_', hierarchy_table), 1, 60) AS tmp_pivot_table, 
  	SUBSTRING(CONCAT('pivot_', hierarchy_table), 1, 60) AS pivot_table
  FROM umls_mrhier.tmp_lookup
);

DROP TABLE umls_mrhier.tmp_lookup;

/*-----------------------------------------------------------
/ A second pivot lookup is made to construct the crosstab function 
/ call
/-----------------------------------------------------------*/
DROP TABLE IF EXISTS umls_mrhier.lookup_crosstab_statement;
CREATE TABLE  umls_mrhier.lookup_crosstab_statement (	
  extended_table varchar(255),
  tmp_pivot_table varchar(255),
  pivot_table varchar(255),
  sql_statement text
)
;


SELECT * FROM umls_mrhier.lookup;

/*-----------------------------------------------------------
/ A crosstab function call is created to pivot each table 
/ based on the maximum `ptr_level` in that table. This is 
/ required to pass the subsequent column names as the 
/ argument to the crosstab function.
/-----------------------------------------------------------*/


DO
$$
DECLARE
	requires_processing boolean;
	start_timestamp timestamp;
	stop_timestamp timestamp;
	mth_version varchar;
	mth_date varchar;
	max_level int;
	source_rows bigint;
	target_rows bigint;
    f record;
    sab varchar(100);
    source_table varchar(255);
    target_table varchar(255);
    pivot_table varchar(255);
	iteration int;
    total_iterations int;
BEGIN  
	SELECT get_umls_mth_version() 
	INTO mth_version;
	
	SELECT COUNT(*) INTO total_iterations FROM umls_mrhier.lookup WHERE hierarchy_sab <> 'SRC';
    FOR f IN SELECT ROW_NUMBER() OVER() AS iteration, l.* FROM umls_mrhier.lookup l WHERE l.hierarchy_sab <> 'SRC'     
    LOOP
		iteration    := f.iteration;
		source_table := f.extended_table;
		target_table := f.tmp_pivot_table;   
		pivot_table  := f.pivot_table;  
		source_rows  := f.count;
		sab          := f.hierarchy_sab;
	
		SELECT check_if_requires_processing(mth_version, source_table, 'LOOKUP_CROSSTAB_STATEMENT') 
		INTO requires_processing;
	
  		IF requires_processing THEN  


		PERFORM notify_start(CONCAT('processing sql statement for ', source_table, ' into table ', target_table));  
   			
		SELECT get_log_timestamp() 
		INTO start_timestamp
		;
		
		PERFORM notify_iteration(iteration, total_iterations, source_table || ' (' || source_rows || ' rows)');
      
		EXECUTE format('SELECT MAX(ptr_level) FROM umls_mrhier.%s', source_table)  
		INTO max_level;
	    
	  
	  EXECUTE 
	    format(
	      '
	      WITH seq1 AS (SELECT generate_series(1,%s) AS series),
	      seq2 AS (
	      	SELECT 
	      		''%s'' AS extended_table, 
	      		''%s'' AS tmp_pivot_table,
	      		''%s'' AS pivot_table,
	      		STRING_AGG(CONCAT(''level_'', series, '' text''), '', '') AS crosstab_ddl
	      	FROM seq1 
	      	GROUP BY extended_table, tmp_pivot_table),
	      seq3 AS (
	      	SELECT
	      		extended_table,
	      		tmp_pivot_table,
	      		pivot_table,
	      		CONCAT(''ptr_id BIGINT, '', crosstab_ddl) AS crosstab_ddl 
	      	FROM seq2
	      ), 
	      seq4 AS (
	        SELECT 
	          extended_table,
	          tmp_pivot_table, 
	          pivot_table,
	          '''''''' || CONCAT(''SELECT ptr_id, ptr_level, ptr_str FROM umls_mrhier.'', extended_table, '' ORDER BY 1,2'') || '''''''' AS crosstab_arg1,
	          '''''''' || CONCAT(''SELECT DISTINCT ptr_level FROM umls_mrhier.'', extended_table, '' ORDER BY 1'') || '''''''' AS crosstab_arg2, 
	          crosstab_ddl
	         FROM seq3
	      ),
	      seq5 AS (
	      	SELECT 
	      	  extended_table,
	      	  tmp_pivot_table,
	      	  pivot_table,
	      	  ''DROP TABLE IF EXISTS umls_mrhier.'' || tmp_pivot_table || '';'' || '' CREATE TABLE umls_mrhier.'' || tmp_pivot_table || '' AS (SELECT * FROM CROSSTAB('' || crosstab_arg1 || '','' || crosstab_arg2 || '') AS ('' || crosstab_ddl || ''));'' AS sql_statement
	      	  FROM seq4
	      
	      )
	      
	      INSERT INTO umls_mrhier.lookup_crosstab_statement 
	      SELECT * FROM seq5
	      ;
	      ',
	      max_level,
	      source_table, 
	      target_table, 
	      pivot_table);
	
		PERFORM notify_completion(CONCAT('processing sql statement for ', source_table, ' into table ', target_table));
		
		
		SELECT get_log_timestamp() 
		INTO stop_timestamp
		; 
		
		SELECT get_umls_mth_version()
		INTO mth_version
		;
		
		SELECT get_umls_mth_dt() 
		INTO mth_date
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
			  ''%s'', 
			  ''umls_mrhier'', 
			  ''%s'', 
			  ''LOOKUP_CROSSTAB_STATEMENT'', 
			  ''%s'',
			   NULL);
			',
			  start_timestamp, 
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  sab,
			  source_table,
			  source_rows);
		COMMIT;
		

    end if;
    end loop;
end;
$$
;


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
    f record;
    sab varchar(100);
    source_table varchar(255);
    tmp_table varchar(255);
    target_table varchar(255);
	iteration int;
    total_iterations int;
    sql_statement text;
BEGIN  
	SELECT get_umls_mth_version() 
	INTO mth_version;
	
	SELECT COUNT(*) INTO total_iterations FROM umls_mrhier.lookup_crosstab_statement;
    FOR f IN SELECT ROW_NUMBER() OVER() AS iteration, l.* FROM umls_mrhier.lookup_crosstab_statement l   
    LOOP
		iteration    := f.iteration;
		source_table := f.extended_table;
		tmp_table    := f.tmp_pivot_table;
		target_table := f.pivot_table;   
	
		SELECT check_if_requires_processing(mth_version, source_table, target_table) 
		INTO requires_processing;
	
  		IF requires_processing THEN  
  		
  		SELECT get_log_timestamp() 
		INTO start_timestamp
		;
  		
  		    
		PERFORM notify_start(CONCAT('processing ', source_table, ' into table ', target_table));

		    sql_statement := f.sql_statement;
		    EXECUTE sql_statement;
    
    
	    EXECUTE 
	      format(
	      	'
	      	DROP TABLE IF EXISTS umls_mrhier.%s;
	      	CREATE TABLE umls_mrhier.%s AS (
		      	SELECT DISTINCT
		      	  h.aui,
		      	  h.code,
		      	  h.str, 
		      	  t.*
		      	FROM umls_mrhier.%s h 
		      	LEFT JOIN umls_mrhier.%s t 
		      	ON t.ptr_id = h.ptr_id
	      	);
	      	DROP TABLE umls_mrhier.%s;
	      	',
	      		target_table,
	      		target_table, 
	      		source_table, 
	      		tmp_table,
	      		tmp_table);
    
		PERFORM notify_completion(CONCAT('processing ', source_table, ' into table ', target_table));
		
		
		SELECT get_log_timestamp() 
		INTO stop_timestamp
		; 
		
		SELECT get_umls_mth_version()
		INTO mth_version
		;
		
		SELECT get_umls_mth_dt() 
		INTO mth_date
		;
		
		EXECUTE format('SELECT COUNT(*) FROM umls_mrhier.%s;', target_table) 
		INTO target_rows;
			
		EXECUTE format('SELECT COUNT(*) FROM umls_mrhier.%s;', source_table) 
		INTO source_rows;


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
			  ''%s'', 
			  ''%s'', 
			  ''%s'',
			   ''%s'');
			',
			  start_timestamp, 
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  source_table,
			  target_table,
			  source_rows, 
			  target_rows);
		COMMIT;
		

    end if;
    end loop;
end;
$$
;


SELECT * FROM umls_mrhier.pivot_snomedct_us_organism;

SELECT * FROM public.process_umls_mrhier_log;



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
	log_datetime := date_trunc('second', timeofday()::timestamp);  
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
  
  SELECT COUNT(*) INTO total_iterations FROM umls_mrhier.lookup_crosstab_statement;
  for f in select ROW_NUMBER() OVER() AS iteration, pl.* from umls_mrhier.lookup_crosstab_statement pl
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
	  m1.sab IN (SELECT sab FROM umls_mrhier.lookup_eng) AND 
	  m1.sab <> 'SRC') -- 'SRC' concepts are basically the source vocabulary and have NULL `ptr` values

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
	  m1.sab IN (SELECT sab FROM umls_mrhier.lookup_eng) AND 
	  m1.sab <> 'SRC'
	ORDER BY m1.sab DESC -- Arbitrarily in descending order to include SNOMEDCT_US first
	) 
;





SELECT * 
FROM public.setup_umls_class_log;

DO
$$
DECLARE 
	log_timestamp timestamp;
	mth_version varchar;
	mth_release_dt varchar;
	target_schema varchar := 'umls_class';
BEGIN 
	SELECT get_log_timestamp() 
	INTO log_timestamp;
	
	SELECT get_umls_mth_version() 
	INTO mth_version;
	
	SELECT get_umls_mth_dt() 
	INTO mth_release_dt;
	
	EXECUTE 
	 format('
	 INSERT INTO public.setup_umls_class_log(suc_datetime, mth_version, mth_release_dt, target_schema)
	 VALUES (''%s'', ''%s'', ''%s'', ''%s'')
	 ;
	 ', 
	 	log_timestamp, 
	 	mth_version,
	 	mth_release_dt, 
	 	target_schema
	 	);
END;
$$
;


DROP TABLE IF EXISTS umls_class.mrhier;
CREATE TABLE umls_class.mrhier AS (
SELECT * 
FROM umls_mrhier.mrhier
)
;

DO
$$
DECLARE 
  mrhier_rows bigint;
BEGIN 
  SELECT COUNT(*) 
  INTO mrhier_rows 
  FROM umls_class.mrhier;
  
  EXECUTE
    format(
    '
    UPDATE public.setup_umls_class_log 
    SET mrhier = %s 
    WHERE suc_datetime IN (SELECT MAX(suc_datetime) FROM public.setup_umls_class_log);
    ', 
    mrhier_rows
    )
    ;
END;
$$
;

DROP TABLE IF EXISTS umls_class.mrhier_str; 
CREATE TABLE umls_class.mrhier_str AS (
SELECT * 
FROM umls_mrhier.mrhier_str
)
;

DO
$$
DECLARE 
  mrhier_str_rows bigint;
BEGIN 
  SELECT COUNT(*) 
  INTO mrhier_str_rows 
  FROM umls_class.mrhier_str;
  
  EXECUTE
    format(
    '
    UPDATE public.setup_umls_class_log 
    SET mrhier_str = %s 
    WHERE suc_datetime IN (SELECT MAX(suc_datetime) FROM public.setup_umls_class_log);
    ', 
    mrhier_str_rows
    )
    ;
END;
$$
;

DROP TABLE IF EXISTS umls_class.mrhier_str_excl; 
CREATE TABLE umls_class.mrhier_str_excl AS (
SELECT * 
FROM umls_mrhier.mrhier_str_excl
)
;

DO
$$
DECLARE 
  mrhier_str_excl_rows bigint;
BEGIN 
  SELECT COUNT(*) 
  INTO mrhier_str_excl_rows 
  FROM umls_class.mrhier_str_excl;
  
  EXECUTE
    format(
    '
    UPDATE public.setup_umls_class_log 
    SET mrhier_str_excl = %s 
    WHERE suc_datetime IN (SELECT MAX(suc_datetime) FROM public.setup_umls_class_log);
    ', 
    mrhier_str_excl_rows
    )
    ;
END;
$$
;


ALTER TABLE umls_class.mrhier_str
ADD CONSTRAINT xpk_mrhier_str 
PRIMARY KEY (ptr_id);

CREATE INDEX x_mrhier_str_aui ON umls_class.mrhier_str(aui);
CREATE INDEX x_mrhier_str_code ON umls_class.mrhier_str(code);


ALTER TABLE umls_class.mrhier_str_excl
ADD CONSTRAINT xpk_mrhier_str_excl
PRIMARY KEY (ptr_id);

CREATE INDEX x_mrhier_str_excl_aui ON umls_class.mrhier_str_excl(aui);
CREATE INDEX x_mrhier_str_excl_code ON umls_class.mrhier_str_excl(code);
CREATE INDEX x_mrhier_str_excl_sab ON umls_class.mrhier_str_excl(sab);


DROP SCHEMA old_umls_mrhier CASCADE;