/**************************************************************************
/ VI. PIVOT CLASSIFICATIONS ('pivot')
/ -------------------------------------------------------------------------
/ Each table is pivoted on ptr_id to compile classifications in at
/ the row level.
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

	SELECT check_if_requires_processing(mth_version, 'LOOKUP_EXT', 'LOOKUP_PIVOT_TABLES')
	INTO requires_processing;

  	IF requires_processing THEN

  		SELECT get_log_timestamp()
  		INTO start_timestamp
  		;

  		PERFORM {postprocess_schema}.notify_start('processing LOOKUP_PIVOT_TABLES');


		DROP TABLE IF EXISTS {postprocess_schema}.lookup_pivot_tables;
		CREATE TABLE {postprocess_schema}.lookup_pivot_tables AS (
		  SELECT
		  	*,
		  	SUBSTRING(CONCAT('tmp_pivot_', hierarchy_table), 1, 60) AS tmp_pivot_table,
		  	SUBSTRING(CONCAT('pivot_', hierarchy_table), 1, 60) AS pivot_table
		  FROM {postprocess_schema}.lookup_ext
		);
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

		SELECT get_row_count('{postprocess_schema}.lookup_ext')
		INTO source_rows
		;

		SELECT get_row_count('{postprocess_schema}.lookup_pivot_tables')
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
			  ''LOOKUP_EXT'',
			  ''LOOKUP_PIVOT_TABLES'',
			  ''%s'',
			  ''%s'');
			',
			  start_timestamp,
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  source_rows,
			  target_rows);


		PERFORM {postprocess_schema}.notify_completion('processing LOOKUP_PIVOT_TABLES');

		-- COMMIT;

		PERFORM {postprocess_schema}.notify_timediff('processing LOOKUP_PIVOT_TABLES', start_timestamp, stop_timestamp);

	END IF;
END;
$$
;


-- A second pivot lookup is made to construct the crosstab
-- function call
-- A crosstab function call is created to pivot each table
-- based on the maximum `ptr_level` in that table. This is
-- required to pass the subsequent column names as the
-- argument to the crosstab function.
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

	SELECT check_if_requires_processing(mth_version, 'LOOKUP_PIVOT_TABLES', 'LOOKUP_PIVOT_CROSSTAB')
	INTO requires_processing;

	IF requires_processing THEN
		SELECT get_log_timestamp()
		INTO start_timestamp
		;

		DROP TABLE IF EXISTS {postprocess_schema}.lookup_pivot_crosstab;
		CREATE TABLE  {postprocess_schema}.lookup_pivot_crosstab (
		  extended_table varchar(255),
		  tmp_pivot_table varchar(255),
		  pivot_table varchar(255),
		  sql_statement text
		)
		;

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

		SELECT get_row_count('{postprocess_schema}.lookup_pivot_tables')
		INTO source_rows
		;

		SELECT get_row_count('{postprocess_schema}.lookup_pivot_crosstab')
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
			  ''LOOKUP_PIVOT_TABLES'',
			  ''LOOKUP_PIVOT_CROSSTAB'',
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



	END IF;

	SELECT COUNT(*) INTO total_iterations FROM {postprocess_schema}.lookup_pivot_tables WHERE hierarchy_sab <> 'SRC';
    FOR f IN SELECT ROW_NUMBER() OVER() AS iteration, l.* FROM {postprocess_schema}.lookup_pivot_tables l WHERE l.hierarchy_sab <> 'SRC'
    LOOP
		iteration    := f.iteration;
		source_table := f.extended_table;
		target_table := f.tmp_pivot_table;
		pivot_table  := f.pivot_table;
		source_rows  := f.count;
		sab          := f.hierarchy_sab;

		SELECT check_if_requires_processing(mth_version, source_table, 'LOOKUP_PIVOT_CROSSTAB')
		INTO requires_processing;

  		IF requires_processing THEN


		PERFORM {postprocess_schema}.notify_start(CONCAT('processing sql statement for ', source_table, ' into table ', target_table));

		SELECT get_log_timestamp()
		INTO start_timestamp
		;

		PERFORM {postprocess_schema}.notify_iteration(iteration, total_iterations, source_table || ' (' || source_rows || ' rows)');

		EXECUTE format('SELECT MAX(ptr_level) FROM {postprocess_schema}.%s', source_table)
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
	          '''''''' || CONCAT(''SELECT ptr_id, ptr_level, ptr_str FROM {postprocess_schema}.'', extended_table, '' ORDER BY 1,2'') || '''''''' AS crosstab_arg1,
	          '''''''' || CONCAT(''SELECT DISTINCT ptr_level FROM {postprocess_schema}.'', extended_table, '' ORDER BY 1'') || '''''''' AS crosstab_arg2,
	          crosstab_ddl
	         FROM seq3
	      ),
	      seq5 AS (
	      	SELECT
	      	  extended_table,
	      	  tmp_pivot_table,
	      	  pivot_table,
	      	  ''DROP TABLE IF EXISTS {postprocess_schema}.'' || tmp_pivot_table || '';'' || '' CREATE TABLE {postprocess_schema}.'' || tmp_pivot_table || '' AS (SELECT * FROM CROSSTAB('' || crosstab_arg1 || '','' || crosstab_arg2 || '') AS ('' || crosstab_ddl || ''));'' AS sql_statement
	      	  FROM seq4

	      )

	      INSERT INTO {postprocess_schema}.lookup_pivot_crosstab
	      SELECT * FROM seq5
	      ;
	      ',
	      max_level,
	      source_table,
	      target_table,
	      pivot_table);

	      -- COMMIT;

		PERFORM {postprocess_schema}.notify_completion(CONCAT('processing sql statement for ', source_table, ' into table ', target_table));


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
			INSERT INTO public.process_{postprocess_schema}_log
			VALUES (
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''{postprocess_schema}'',
			  ''%s'',
			  ''LOOKUP_PIVOT_CROSSTAB'',
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

		-- COMMIT;

		PERFORM {postprocess_schema}.notify_timediff(CONCAT('processing sql statement for ', source_table, ' into table ', target_table), start_timestamp, stop_timestamp);



    end if;
    end loop;
end;
$$
;

-- The sql statements are executed from
-- 'LOOKUP_PIVOT_CROSSTAB'
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

	SELECT COUNT(*) INTO total_iterations FROM {postprocess_schema}.lookup_pivot_crosstab;
    FOR f IN SELECT ROW_NUMBER() OVER() AS iteration, l.* FROM {postprocess_schema}.lookup_pivot_crosstab l
    LOOP
		iteration    := f.iteration;
		source_table := f.extended_table;
		tmp_table    := f.tmp_pivot_table;
		target_table := f.pivot_table;

		PERFORM {postprocess_schema}.notify_iteration(iteration, total_iterations, source_table || ' --> ' || target_table);

		SELECT check_if_requires_processing(mth_version, source_table, target_table)
		INTO requires_processing;

  		IF requires_processing THEN

  		SELECT get_log_timestamp()
		INTO start_timestamp
		;


		PERFORM {postprocess_schema}.notify_start(CONCAT('processing ', source_table, ' into table ', target_table));

		    sql_statement := f.sql_statement;
		    EXECUTE sql_statement;


	    EXECUTE
	      format(
	      	'
	      	DROP TABLE IF EXISTS {postprocess_schema}.%s;
	      	CREATE TABLE {postprocess_schema}.%s AS (
		      	SELECT DISTINCT
		      	  h.aui,
		      	  h.code,
		      	  h.str,
		      	  t.*
		      	FROM {postprocess_schema}.%s h
		      	LEFT JOIN {postprocess_schema}.%s t
		      	ON t.ptr_id = h.ptr_id
	      	);
	      	DROP TABLE {postprocess_schema}.%s;
	      	',
	      		target_table,
	      		target_table,
	      		source_table,
	      		tmp_table,
	      		tmp_table);

	    -- COMMIT;

		PERFORM {postprocess_schema}.notify_completion(CONCAT('processing ', source_table, ' into table ', target_table));


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
		INTO target_rows;

		EXECUTE format('SELECT COUNT(*) FROM {postprocess_schema}.%s;', source_table)
		INTO source_rows;


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


		-- COMMIT;


		PERFORM {postprocess_schema}.notify_timediff(CONCAT('processing ', source_table, ' into table ', target_table), start_timestamp, stop_timestamp);



    end if;
    end loop;
end;
$$
;


/**************************************************************************
/ VIb. PIVOT CLASSIFICATIONS BY CODE ('pivot')
/ -------------------------------------------------------------------------
/ Each table is pivoted on ptr_id to compile classifications in at
/ the row level.
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

	SELECT check_if_requires_processing(mth_version, 'LOOKUP_EXT', 'LOOKUP_PIVOT_TABLES_CODE')
	INTO requires_processing;

  	IF requires_processing THEN

  		SELECT get_log_timestamp()
  		INTO start_timestamp
  		;

  		PERFORM {postprocess_schema}.notify_start('processing LOOKUP_PIVOT_TABLES_CODE');


		DROP TABLE IF EXISTS {postprocess_schema}.lookup_pivot_tables_code;
		CREATE TABLE {postprocess_schema}.lookup_pivot_tables_code AS (
		  SELECT
		  	*,
		  	SUBSTRING(CONCAT('tmp_pivot_code_', hierarchy_table), 1, 60) AS tmp_pivot_code_table,
		  	SUBSTRING(CONCAT('pivot_code_', hierarchy_table), 1, 60) AS pivot_code_table
		  FROM {postprocess_schema}.lookup_ext
		);
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

		SELECT get_row_count('{postprocess_schema}.lookup_ext')
		INTO source_rows
		;

		SELECT get_row_count('{postprocess_schema}.lookup_pivot_tables_code')
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
			  ''LOOKUP_EXT'',
			  ''LOOKUP_PIVOT_TABLES_CODE'',
			  ''%s'',
			  ''%s'');
			',
			  start_timestamp,
			  stop_timestamp,
			  mth_version,
			  mth_date,
			  source_rows,
			  target_rows);


		PERFORM {postprocess_schema}.notify_completion('processing LOOKUP_PIVOT_TABLES_CODE');

		PERFORM {postprocess_schema}.notify_timediff('processing LOOKUP_PIVOT_TABLES_CODE', start_timestamp, stop_timestamp);

	END IF;
END;
$$
;


-- A second pivot lookup is made to construct the crosstab
-- function call
-- A crosstab function call is created to pivot each table
-- based on the maximum `ptr_level` in that table. This is
-- required to pass the subsequent column names as the
-- argument to the crosstab function.
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

	SELECT check_if_requires_processing(mth_version, 'LOOKUP_PIVOT_TABLES_CODE', 'LOOKUP_PIVOT_CROSSTAB_CODE')
	INTO requires_processing;

	IF requires_processing THEN
		SELECT get_log_timestamp()
		INTO start_timestamp
		;

		DROP TABLE IF EXISTS {postprocess_schema}.lookup_pivot_crosstab_code;
		CREATE TABLE  {postprocess_schema}.lookup_pivot_crosstab_code (
		  extended_table varchar(255),
		  tmp_pivot_code_table varchar(255),
		  pivot_code_table varchar(255),
		  sql_statement text
		)
		;

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

		SELECT get_row_count('{postprocess_schema}.lookup_pivot_tables_code')
		INTO source_rows
		;

		SELECT get_row_count('{postprocess_schema}.lookup_pivot_crosstab_code')
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
			  ''LOOKUP_PIVOT_TABLES_CODE'',
			  ''LOOKUP_PIVOT_CROSSTAB_CODE'',
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



	END IF;

	SELECT COUNT(*) INTO total_iterations FROM {postprocess_schema}.lookup_pivot_tables_code WHERE hierarchy_sab <> 'SRC';
    FOR f IN SELECT ROW_NUMBER() OVER() AS iteration, l.* FROM {postprocess_schema}.lookup_pivot_tables_code l WHERE l.hierarchy_sab <> 'SRC'
    LOOP
		iteration    := f.iteration;
		source_table := f.extended_table;
		target_table := f.tmp_pivot_code_table;
		pivot_table  := f.pivot_code_table;
		source_rows  := f.count;
		sab          := f.hierarchy_sab;

		SELECT check_if_requires_processing(mth_version, source_table, 'LOOKUP_PIVOT_CROSSTAB_CODE')
		INTO requires_processing;

  		IF requires_processing THEN


		PERFORM {postprocess_schema}.notify_start(CONCAT('processing sql statement for ', source_table, ' into table ', target_table));

		SELECT get_log_timestamp()
		INTO start_timestamp
		;

		PERFORM {postprocess_schema}.notify_iteration(iteration, total_iterations, source_table || ' (' || source_rows || ' rows)');

		EXECUTE format('SELECT MAX(ptr_level) FROM {postprocess_schema}.%s', source_table)
		INTO max_level;


	  EXECUTE
	    format(
	      '
	      WITH seq1 AS (SELECT generate_series(1,%s) AS series),
	      seq2 AS (
	      	SELECT
	      		''%s'' AS extended_table,
	      		''%s'' AS tmp_pivot_code_table,
	      		''%s'' AS pivot_code_table,
	      		STRING_AGG(CONCAT(''level_'', series, ''_code text''), '', '') AS crosstab_ddl
	      	FROM seq1
	      	GROUP BY extended_table, tmp_pivot_code_table),
	      seq3 AS (
	      	SELECT
	      		extended_table,
	      		tmp_pivot_code_table,
	      		pivot_code_table,
	      		CONCAT(''ptr_id BIGINT, '', crosstab_ddl) AS crosstab_ddl
	      	FROM seq2
	      ),
	      seq4 AS (
	        SELECT
	          extended_table,
	          tmp_pivot_code_table,
	          pivot_code_table,
	          '''''''' || CONCAT(''SELECT ptr_id, ptr_level, ptr_code FROM {postprocess_schema}.'', extended_table, '' ORDER BY 1,2'') || '''''''' AS crosstab_arg1,
	          '''''''' || CONCAT(''SELECT DISTINCT ptr_level FROM {postprocess_schema}.'', extended_table, '' ORDER BY 1'') || '''''''' AS crosstab_arg2,
	          crosstab_ddl
	         FROM seq3
	      ),
	      seq5 AS (
	      	SELECT
	      	  extended_table,
	      	  tmp_pivot_code_table,
	      	  pivot_code_table,
	      	  ''DROP TABLE IF EXISTS {postprocess_schema}.'' || tmp_pivot_code_table || '';'' || '' CREATE TABLE {postprocess_schema}.'' || tmp_pivot_code_table || '' AS (SELECT * FROM CROSSTAB('' || crosstab_arg1 || '','' || crosstab_arg2 || '') AS ('' || crosstab_ddl || ''));'' AS sql_statement
	      	  FROM seq4

	      )

	      INSERT INTO {postprocess_schema}.lookup_pivot_crosstab_code
	      SELECT * FROM seq5
	      ;
	      ',
	      max_level,
	      source_table,
	      target_table,
	      pivot_table);

	      -- COMMIT;

		PERFORM {postprocess_schema}.notify_completion(CONCAT('processing sql statement for ', source_table, ' into table ', target_table));


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
			INSERT INTO public.process_{postprocess_schema}_log
			VALUES (
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''%s'',
			  ''{postprocess_schema}'',
			  ''%s'',
			  ''LOOKUP_PIVOT_CROSSTAB_CODE'',
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

		-- COMMIT;

		PERFORM {postprocess_schema}.notify_timediff(CONCAT('processing sql statement for ', source_table, ' into table ', target_table), start_timestamp, stop_timestamp);



    end if;
    end loop;
end;
$$
;


-- The sql statements are executed from
-- 'LOOKUP_PIVOT_CROSSTAB_CODE'
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

	SELECT COUNT(*) INTO total_iterations FROM {postprocess_schema}.lookup_pivot_crosstab_code;
    FOR f IN SELECT ROW_NUMBER() OVER() AS iteration, l.* FROM {postprocess_schema}.lookup_pivot_crosstab_code l
    LOOP
		iteration    := f.iteration;
		source_table := f.extended_table;
		tmp_table    := f.tmp_pivot_code_table;
		target_table := f.pivot_code_table;

		PERFORM {postprocess_schema}.notify_iteration(iteration, total_iterations, source_table || ' --> ' || target_table);

		SELECT check_if_requires_processing(mth_version, source_table, target_table)
		INTO requires_processing;

  		IF requires_processing THEN

  		SELECT get_log_timestamp()
		INTO start_timestamp
		;


		PERFORM {postprocess_schema}.notify_start(CONCAT('processing ', source_table, ' into table ', target_table));

	    sql_statement := f.sql_statement;
	    EXECUTE sql_statement;

	    -- COMMIT;


	    EXECUTE
	      format(
	      	'
	      	DROP TABLE IF EXISTS {postprocess_schema}.%s;
	      	CREATE TABLE {postprocess_schema}.%s AS (
		      	SELECT DISTINCT
		      	  h.aui,
		      	  h.code,
		      	  h.str,
		      	  t.*
		      	FROM {postprocess_schema}.%s h
		      	LEFT JOIN {postprocess_schema}.%s t
		      	ON t.ptr_id = h.ptr_id
	      	);
	      	DROP TABLE {postprocess_schema}.%s;
	      	',
	      		target_table,
	      		target_table,
	      		source_table,
	      		tmp_table,
	      		tmp_table);

	    -- COMMIT;

		PERFORM {postprocess_schema}.notify_completion(CONCAT('processing ', source_table, ' into table ', target_table));


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
		INTO target_rows;

		EXECUTE format('SELECT COUNT(*) FROM {postprocess_schema}.%s;', source_table)
		INTO source_rows;


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


		-- COMMIT;


		PERFORM {postprocess_schema}.notify_timediff(CONCAT('processing ', source_table, ' into table ', target_table), start_timestamp, stop_timestamp);



    end if;
    end loop;
end;
$$
;
