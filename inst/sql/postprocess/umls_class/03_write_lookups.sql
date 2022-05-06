/**************************************************************************
/ II. CREATE LOOKUP TABLES `LOOKUP_ENG` and `LOOKUP_PARSE`
/ -------------------------------------------------------------------------
/ `LOOKUP_ENG` is a single field of all SAB that have a LAT value of 'ENG'
/ in the MRCONSO table.
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

	SELECT check_if_requires_processing(mth_version, 'MRCONSO', 'LOOKUP_ENG')
	INTO requires_processing;

  	IF requires_processing THEN

  		SELECT get_log_timestamp()
  		INTO start_timestamp
  		;

  		PERFORM {postprocess_schema}.notify_start('processing LOOKUP_ENG');

		DROP TABLE IF EXISTS {postprocess_schema}.lookup_eng;
		CREATE TABLE {postprocess_schema}.lookup_eng (
		    sab character varying(40)
		);

		INSERT INTO {postprocess_schema}.lookup_eng
		SELECT DISTINCT sab
		FROM {schema}.mrconso
		WHERE lat = 'ENG' ORDER BY sab;

		PERFORM {postprocess_schema}.notify_completion('processing LOOKUP_ENG');

		SELECT get_log_timestamp()
		INTO stop_timestamp
		;

		SELECT get_umls_mth_version()
		INTO mth_version
		;

		SELECT get_umls_mth_dt()
		INTO mth_date
		;

		SELECT get_row_count('{schema}.mrconso')
		INTO source_rows
		;

		SELECT get_row_count('{postprocess_schema}.lookup_eng')
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

		-- COMMIT;


		PERFORM {postprocess_schema}.notify_timediff('processing LOOKUP_ENG', start_timestamp, stop_timestamp);



	END IF;
end;
$$
;

