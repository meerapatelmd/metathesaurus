/**************************************************************************
/ III. PARSE PTR BY SAB ('parse')
/ -------------------------------------------------------------------------
/ For each unique SAB in the MRHIER table,
/ the decimal-separated PTR string is parsed along with its
/ ordinality as PTR_LEVEL. The parsed individual PTR_AUI
/ is joined to MRCONSO to add the PTR_CODE and PTR. Each SAB is written to
/ its own table referenced in the `lookup_parse` table.
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
    f record;
    target_table varchar(255);
    source_sab varchar(255);
	iteration int;
    total_iterations int;
BEGIN
	SELECT get_umls_mth_version()
	INTO mth_version;

	SELECT COUNT(*) INTO total_iterations FROM {postprocess_schema}.lookup_parse;
    FOR f IN SELECT ROW_NUMBER() OVER() AS iteration, l.* FROM {postprocess_schema}.lookup_parse l
    LOOP
		iteration := f.iteration;
		target_table := f.hierarchy_table;
		source_sab := f.hierarchy_sab;

		SELECT {postprocess_schema}.check_if_requires_processing(mth_version, 'MRHIER', target_table)
		INTO requires_processing;

  		IF requires_processing THEN

   			PERFORM {postprocess_schema}.notify_start(CONCAT('processing', ' ', source_sab, ' into table ', target_table));
  			SELECT get_log_timestamp()
  			INTO start_timestamp
  			;

  			EXECUTE
			format(
				'
				SELECT COUNT(*)
				FROM {postprocess_schema}.mrhier
				WHERE sab = ''%s'';
				',
					source_sab
			)
			INTO source_rows;

  	  		PERFORM {postprocess_schema}.notify_iteration(iteration, total_iterations, source_sab || ' (' || source_rows || ' source rows)');

			EXECUTE
			format(
			  '
			  DROP TABLE IF EXISTS {postprocess_schema}.%s;
			  CREATE TABLE  {postprocess_schema}.%s (
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
				FROM {postprocess_schema}.mrhier m
				INNER JOIN {schema}.mrconso s1
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
			  	LEFT JOIN {schema}.mrconso m
			  	ON m.aui = r2.ptr_aui
			  )

			  INSERT INTO {postprocess_schema}.%s
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
			  ON {postprocess_schema}.%s (ptr_id, ptr_level);
			  CLUSTER {postprocess_schema}.%s USING idx_%s_ptr;

			  CREATE INDEX x_%s_aui ON {postprocess_schema}.%s(aui);
			  CREATE INDEX x_%s_code ON {postprocess_schema}.%s(code);
			  CREATE INDEX x_%s_ptr_aui ON {postprocess_schema}.%s(ptr_aui);
			  CREATE INDEX x_%s_ptr_code ON {postprocess_schema}.%s(ptr_code);
			  CREATE INDEX x_%s_ptr_level ON {postprocess_schema}.%s(ptr_level);
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

			 -- COMMIT;



  		PERFORM {postprocess_schema}.notify_completion(CONCAT('processing', ' ', source_sab, ' into table ', target_table));


  		SELECT get_log_timestamp()
		INTO stop_timestamp
		;

		SELECT get_umls_mth_version()
		INTO mth_version
		;

		SELECT get_umls_mth_dt()
		INTO mth_date
		;


		EXECUTE format('SELECT COUNT(*) FROM {postprocess_schema}.%s;', target_table)
		INTO target_rows
		;

		EXECUTE
		  format(
		    '
			INSERT INTO public.process_{postprocess_schema}_log
			VALUES (
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''{postprocess_schema}'',
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

		-- COMMIT;

		PERFORM {postprocess_schema}.notify_timediff(CONCAT('processing', ' ', source_sab, ' into table ', target_table), start_timestamp, stop_timestamp);
	END IF;
	END LOOP;
end;
$$
;
