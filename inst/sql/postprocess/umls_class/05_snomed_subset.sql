/**************************************************************************
/ IV. SPLIT SNOMEDCT_US TABLE BY ROOT
/ -------------------------------------------------------------------------
/ The SNOMEDCT_US table is too large to work with downstream
/ and it is subset here by the 2nd level root concept to make it
/ more manageable.
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

	SELECT check_if_requires_processing(mth_version, 'SNOMEDCT_US', 'LOOKUP_SNOMED')
	INTO requires_processing;

  	IF requires_processing THEN

  		SELECT get_log_timestamp()
  		INTO start_timestamp
  		;

  		PERFORM {postprocess_schema}.notify_start('processing LOOKUP_SNOMED');

  		DROP TABLE IF EXISTS {postprocess_schema}.lookup_snomed;
		CREATE TABLE {postprocess_schema}.lookup_snomed (
		    hierarchy_table text,
		    root_aui varchar(12),
		    root_code varchar(255),
		    root_str varchar(255),
		    updated_hierarchy_table varchar(255),
		    root_count bigint
		);

		INSERT INTO {postprocess_schema}.lookup_snomed
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
		FROM {postprocess_schema}.snomedct_us
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

		SELECT get_row_count('{postprocess_schema}.snomedct_us')
		INTO source_rows
		;

		SELECT get_row_count('{postprocess_schema}.lookup_snomed')
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


		PERFORM {postprocess_schema}.notify_completion('processing LOOKUP_SNOMED');

		-- COMMIT;

		PERFORM {postprocess_schema}.notify_timediff('processing LOOKUP_SNOMED', start_timestamp, stop_timestamp);

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

	SELECT COUNT(*) INTO total_iterations FROM {postprocess_schema}.lookup_snomed;
    FOR f IN SELECT ROW_NUMBER() OVER() AS iteration, l.* FROM {postprocess_schema}.lookup_snomed l
    LOOP
		iteration    := f.iteration;
		target_table := f.updated_hierarchy_table;
		source_rows  := f.root_count;
		root_str     := f.root_str;
		root_aui     := f.root_aui;

		SELECT check_if_requires_processing(mth_version, 'SNOMEDCT_US', target_table)
		INTO requires_processing;

  		IF requires_processing THEN

   			PERFORM {postprocess_schema}.notify_start(CONCAT('processing table ', source_table, ' into table ', target_table));

  			SELECT get_log_timestamp()
  			INTO start_timestamp
  			;

  			PERFORM {postprocess_schema}.notify_iteration(iteration, total_iterations, root_str || ' (' || source_rows || ' rows)');

			EXECUTE
			format(
			  '
			  DROP TABLE IF EXISTS {postprocess_schema}.%s;
			  CREATE TABLE {postprocess_schema}.%s (
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

			  INSERT INTO {postprocess_schema}.%s
			  	SELECT *
			  	FROM {postprocess_schema}.snomedct_us
			  	WHERE ptr_id IN (
			  		SELECT DISTINCT ptr_id
			  		FROM {postprocess_schema}.snomedct_us
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
				  ''SNOMEDCT_US'',
				  ''{postprocess_schema}'',
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

		-- COMMIT;

		PERFORM {postprocess_schema}.notify_timediff(CONCAT('processing table ', source_table, ' into table ', target_table), start_timestamp, stop_timestamp);


    end if;
    end loop;
end;
$$
;
