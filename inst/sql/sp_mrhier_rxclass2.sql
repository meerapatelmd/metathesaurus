/* 
Derive RxClass hierarchies from UMLS Metathesaurus MRHIER Table  
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
CREATE TABLE IF NOT EXISTS public.setup_rxclass_log (
    sr_datetime timestamp without time zone,
    sr_mth_version character varying(255),
    sr_mth_release_dt character varying(255),
    sabs character varying(255),
    rxclass_schema character varying(255),
    rxclass_abbr character varying(255),
    rxclass_code character varying(255),
    table_row_ct numeric
);


SELECT * FROM public.setup_rxclass_log;                                                             
-----------------------------------------------------------
-- 
-----------------------------------------------------------       

DROP SCHEMA rxclass CASCADE;
CREATE SCHEMA rxclass; 

DROP TABLE IF EXISTS rxclass.lookup;
CREATE TABLE rxclass.lookup (
mrhier_table varchar(255) NOT NULL,
rxclass_abbr varchar(255) NOT NULL, 
rxclass_code varchar(255) NOT NULL
);

INSERT INTO rxclass.lookup 
VALUES 
  ('MED_RT', 'EPC', 'N0000189939'), 
  ('MSH', 'MeSHPA', 'D020228'), 
  ('MED_RT', 'MoA', 'N0000000223'),
  ('MED_RT', 'PE', 'N0000009802'), 
  ('MED_RT', 'PK', 'N0000000003'), 
  ('MED_RT', 'TC', 'N0000178293'),
  ('MSH', 'Diseases', 'U000006'), 
  ('MSH', 'AgeGroups', 'D009273'), 
  ('MSH', 'Behavior', 'D001520'), 
  ('MSH', 'Reproductive', 'D055703'), 
  ('MSH', 'Substances', 'U000005'),
  ('SNOMEDCT_US', 'DISPOS', '766779001'),
  ('SNOMEDCT_US', 'STRUCT', '763760008');
  


do
$$
declare
    f record;
    tbl varchar(255);
    rxclass varchar(255);
    rxclass_code varchar(255);
    start_time timestamp;
    end_time timestamp;
    iteration int;
    total_iterations int;
    log_datetime timestamp;
    log_mth_version varchar(25);
    log_mth_release_dt timestamp;
    final_ct int;
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


    SELECT COUNT(*) INTO total_iterations FROM rxclass.lookup;
    for f in select ROW_NUMBER() OVER() AS iteration, l.* from rxclass.lookup l    
    loop 
      iteration := f.iteration;
      tbl := f.mrhier_table;
      rxclass := f.rxclass_abbr;
      rxclass_code  := f.rxclass_code;
      start_time := date_trunc('second', timeofday()::timestamp);
      
      
      
	  raise notice '[%] %/% %', start_time, iteration, total_iterations, rxclass;
	  EXECUTE
	   format(
		  '
		  DROP TABLE IF EXISTS rxclass.%s; 
		  CREATE TABLE  rxclass.%s AS (
		    SELECT * 
		    FROM umls_mrhier.%s 
		    WHERE ptr_id IN (SELECT DISTINCT ptr_id FROM umls_mrhier.%s WHERE ptr_code = ''%s'') 
		    ORDER BY ptr_id, ptr_level
		  );
		  ', 
		  	rxclass, 
		  	rxclass, 
		  	tbl, 
		  	tbl, 
		  	rxclass_code);
  
  EXECUTE format('SELECT count(*) FROM rxclass.%s', rxclass)  
    INTO final_ct;
  
  EXECUTE 
  	format(
  		'
  		INSERT INTO public.setup_rxclass_log  
  		VALUES (''%s'', ''%s'', ''%s'', ''%s'', ''rxclass'', ''%s'', ''%s'', %s); 
  		',
  			log_datetime, 
  			log_mth_version, 
  			log_mth_release_dt, 
  			tbl, 
  			rxclass, 
  			rxclass_code,
  			final_ct);
  
   end_time := date_trunc('second', timeofday()::timestamp);
   
   raise notice '% complete (%)', tbl, end_time - start_time;
   
   
  
    end loop;
end;
$$
;


select * from public.setup_rxclass_log;
