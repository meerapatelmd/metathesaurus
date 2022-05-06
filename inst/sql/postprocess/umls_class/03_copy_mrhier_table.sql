/**************************************************************************
/ I. Transfer MRHIER to `{postprocess_schema}` Schema
/ -------------------------------------------------------------------------
/ If the current UMLS Metathesaurus version is not logged for
/ the transfer of the MRHIER table, the `{postprocess_schema}` schema is dropped.
/ The unique AUI-RELA-PTR from the MRHIER table in the `mth` schema
/ is then copied to the along with the AUI's CODE, SAB and STR in the MRCONSO
/ table. A `ptr_id` to serve as a unique identifier each row number, which
/ represents a unique classification for the given AUI.
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

	SELECT {postprocess_schema}.check_if_requires_processing(mth_version, 'MRHIER', 'MRHIER')
	INTO requires_processing;

  	IF requires_processing THEN

  		DROP SCHEMA IF EXISTS {postprocess_schema} CASCADE;
		CREATE SCHEMA {postprocess_schema};


  		SELECT get_log_timestamp()
  		INTO start_timestamp
  		;

  		-- PERFORM {postprocess_schema}.notify_start('processing MRHIER');


		DROP TABLE IF EXISTS {postprocess_schema}.tmp_mrhier;
		CREATE TABLE {postprocess_schema}.tmp_mrhier AS (
			SELECT DISTINCT
			  m.AUI,
			  c.CODE,
			  c.SAB,
			  c.STR,
			  m.RELA,
			  m.PTR
			 FROM {schema}.mrhier m
			 INNER JOIN {schema}.mrconso c
			 ON c.aui = m.aui
		);


		DROP TABLE IF EXISTS {postprocess_schema}.mrhier;
		CREATE TABLE {postprocess_schema}.mrhier AS (
		   SELECT ROW_NUMBER() OVER() AS ptr_id, m.*
		   FROM {postprocess_schema}.tmp_mrhier m
		)
		;

		ALTER TABLE {postprocess_schema}.mrhier
		ADD CONSTRAINT xpk_mrhier
		PRIMARY KEY (ptr_id);

		CREATE INDEX x_mrhier_sab ON {postprocess_schema}.mrhier(sab);
		CREATE INDEX x_mrhier_aui ON {postprocess_schema}.mrhier(aui);
		CREATE INDEX x_mrhier_code ON {postprocess_schema}.mrhier(code);

		DROP TABLE {postprocess_schema}.tmp_mrhier;

		-- COMMIT;

		PERFORM {postprocess_schema}.notify_completion('processing MRHIER');

		SELECT {postprocess_schema}.get_log_timestamp()
		INTO stop_timestamp
		;

		SELECT {postprocess_schema}.get_umls_mth_version()
		INTO mth_version
		;

		SELECT {postprocess_schema}.get_umls_mth_dt()
		INTO mth_date
		;

		SELECT {postprocess_schema}.get_row_count('{schema}.mrhier')
		INTO source_rows
		;

		SELECT {postprocess_schema}.get_row_count('{postprocess_schema}.mrhier')
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

		PERFORM {postprocess_schema}.notify_timediff('processing MRHIER', start_timestamp, stop_timestamp);

	END IF;
end;
$$
;
