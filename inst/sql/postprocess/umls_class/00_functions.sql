/**************************************************************************
Logging Functions
**************************************************************************/

create or replace function {postprocess_schema}.get_log_timestamp()
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

create or replace function {postprocess_schema}.get_umls_mth_version()
returns varchar
language plpgsql
as
$$
declare
	umls_mth_version varchar;
begin
	SELECT sm_version
	INTO umls_mth_version
	FROM public.setup_{schema}_log
	WHERE sm_datetime IN (SELECT MAX(sm_datetime) FROM public.setup_{schema}_log);

  	RETURN umls_mth_version;
END;
$$;

create or replace function {postprocess_schema}.get_umls_mth_dt()
returns varchar
language plpgsql
as
$$
declare
	umls_mth_dt varchar;
begin
	SELECT sm_release_date
	INTO umls_mth_dt
	FROM public.setup_{schema}_log
	WHERE sm_datetime IN (SELECT MAX(sm_datetime) FROM public.setup_{schema}_log);

  	RETURN umls_mth_dt;
END;
$$;


create or replace function {postprocess_schema}.get_row_count(_tbl varchar)
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


create or replace function {postprocess_schema}.check_if_requires_processing(umls_mth_version varchar, source_table varchar, target_table varchar)
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
		FROM public.process_{postprocess_schema}_log l
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


create or replace function {postprocess_schema}.notify_iteration(iteration int, total_iterations int, objectname varchar)
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

create or replace function {postprocess_schema}.notify_start(report varchar)
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

create or replace function {postprocess_schema}.notify_completion(report varchar)
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


create or replace function {postprocess_schema}.notify_timediff(report varchar, start_timestamp timestamp, stop_timestamp timestamp)
returns void
language plpgsql
as
$$
begin
	RAISE NOTICE '% required %s to complete.', report, stop_timestamp - start_timestamp;
end;
$$
;

create or replace function {postprocess_schema}.sab_to_tablename(sab varchar)
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
