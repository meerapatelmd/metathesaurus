/**************************************************************************
/ V. EXTEND PATH TO ROOT WITH LEAF ('ext')
/ -------------------------------------------------------------------------
/ The leaf of the hierarchy is represented by the AUI, CODE, and STR.
/ These leafs are added at the end of the path to root to get a complete
/ representation of the classification.
/ Note that the RxClass subset are derived from this step.
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

	SELECT check_if_requires_processing(mth_version, 'LOOKUP_PARSE', 'LOOKUP_EXT')
	INTO requires_processing;

  	IF requires_processing THEN

  		SELECT get_log_timestamp()
  		INTO start_timestamp
  		;

  		PERFORM {postprocess_schema}.notify_start('processing LOOKUP_EXT');

		DROP TABLE IF EXISTS {postprocess_schema}.tmp_lookup_ext;
		CREATE TABLE {postprocess_schema}.tmp_lookup_ext AS (
			SELECT
			  lu.hierarchy_sab,
			  tmp.root_aui,
			  tmp.root_code,
			  tmp.root_str,
			  COALESCE(tmp.updated_hierarchy_table, lu.hierarchy_table) AS hierarchy_table,
			  COALESCE(tmp.root_count, lu.count) AS count
			FROM {postprocess_schema}.lookup_parse lu
			LEFT JOIN {postprocess_schema}.lookup_snomed tmp
			ON lu.hierarchy_table = tmp.hierarchy_table
		)
		;
		DROP TABLE IF EXISTS {postprocess_schema}.lookup_ext;
		CREATE TABLE {postprocess_schema}.lookup_ext AS (
		  SELECT
		  	*,
		  	SUBSTRING(CONCAT('ext_', hierarchy_table), 1, 60) AS extended_table
		  FROM {postprocess_schema}.tmp_lookup_ext
		);
		DROP TABLE {postprocess_schema}.tmp_lookup_ext;

		-- COMMIT;



		SELECT get_log_timestamp()
		INTO stop_timestamp
		;

		SELECT get_umls_mth_version()
		INTO mth_version
		;

		SELECT get_umls_mth_dt()
		INTO mth_date
		;

		SELECT get_row_count('{postprocess_schema}.lookup_parse')
		INTO source_rows
		;

		SELECT get_row_count('{postprocess_schema}.lookup_ext')
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
			  NULL,
			  ''{postprocess_schema}'',
			  ''LOOKUP_PARSE'',
			  ''LOOKUP_EXT'',
			  ''%s'',
			  ''%s'');
			',
			  start_timestamp,
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  source_rows,
			  target_rows);


		PERFORM {postprocess_schema}.notify_completion('processing LOOKUP_EXT');


		PERFORM {postprocess_schema}.notify_timediff('processing LOOKUP_EXT', start_timestamp, stop_timestamp);

	END IF;


END;
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
    target_table varchar(255);
	iteration int;
    total_iterations int;
BEGIN
	SELECT get_umls_mth_version()
	INTO mth_version;

	SELECT COUNT(*) INTO total_iterations FROM {postprocess_schema}.lookup_ext;
    FOR f IN SELECT ROW_NUMBER() OVER() AS iteration, l.* FROM {postprocess_schema}.lookup_ext l
    LOOP
		iteration    := f.iteration;
		source_table := f.hierarchy_table;
		target_table := f.extended_table;
		source_rows  := f.count;
		sab          := f.hierarchy_sab;

		SELECT check_if_requires_processing(mth_version, source_table, target_table)
		INTO requires_processing;

  		IF requires_processing THEN


		PERFORM {postprocess_schema}.notify_start(CONCAT('processing table ', source_table, ' into table ', target_table));

  			SELECT get_log_timestamp()
  			INTO start_timestamp
  			;

  			PERFORM {postprocess_schema}.notify_iteration(iteration, total_iterations, source_table || ' (' || source_rows || ' rows)');

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
				FROM {postprocess_schema}.%s
				GROUP BY ptr_id, ptr, aui, code, str, rela
			  ),
			  with_leafs AS (
			  	SELECT *
			  	FROM leafs
			  	UNION
			  	SELECT *
			  	FROM {postprocess_schema}.%s
			  )

			  INSERT INTO {postprocess_schema}.%s
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

			-- COMMIT;

			PERFORM {postprocess_schema}.notify_completion(CONCAT('processing table ', source_table, ' into table ', target_table));


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


		-- COMMIT;

		PERFORM {postprocess_schema}.notify_timediff(CONCAT('processing table ', source_table, ' into table ', target_table), start_timestamp, stop_timestamp);


    end if;
    end loop;
end;
$$
;
