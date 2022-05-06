/**************************************************************************
LOG TABLES
Process log table logs the processing.
Setup log table logs the final `MRHIER`, `MRHIER_STR`, and `MRHIER_STR_EXCL` tables.
Both are setup if it does not already exist.
**************************************************************************/

CREATE TABLE IF NOT EXISTS public.process_{postprocess_schema}_log (
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

CREATE TABLE IF NOT EXISTS public.setup_{postprocess_schema}_log (
    suc_datetime timestamp without time zone,
    mth_version character varying(255),
    mth_release_dt character varying(255),
    target_schema character varying(255),
    mrhier bigint,
    mrhier_str bigint,
    mrhier_str_excl bigint,
    mrhier_code bigint
);
