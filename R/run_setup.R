#' @title
#' Instantiate Postgres
#' @inherit setup description
#' @inheritParams pkg_args
#' @rdname run_setup
#' @export
#' @importFrom pg13 lsSchema send lsTables dropTable
#' @importFrom tibble as_tibble_col
#' @importFrom dplyr mutate select distinct filter
#' @importFrom stringr str_remove_all
#' @importFrom progress progress_bar
#' @importFrom glue glue


run_setup <-
  function(conn,
           conn_fun = "pg13::local_connect()",
           schema = "umls",
           path_to_rrfs,
           steps = c(
             "reset_schema",
             "ddl_tables",
             "copy_rrfs",
             "add_indexes",
             "log",
             "setup_crosswalk"),
           postprocess = TRUE,
           mrconso_only = "DEPRECATED",
           omop_only = "DEPRECATED",
           english_only = TRUE,
           log_schema = "public",
           log_table_name = "setup_umls_log",
           log_version,
           log_release_date,
           verbose = TRUE,
           render_sql = TRUE,
           render_only = FALSE) {


    schema <- schema

    if (missing(log_version) | missing(log_release_date)) {
      stop("`log_version` and `log_release_date` are required.")
    }

    if (missing(conn)) {
      conn <- eval(rlang::parse_expr(conn_fun))
      on.exit(expr = pg13::dc(conn = conn), add = TRUE,
              after = TRUE)
    }


    path_to_rrfs <- normalizePath(
      path = path_to_rrfs,
      mustWork = TRUE
    )

    ##### Setup Objects
    errors <- vector()

    tables <-
      c(
        "AMBIGLUI",
        "AMBIGSUI",
        "DELETEDCUI",
        "DELETEDLUI",
        "DELETEDSUI",
        "MERGEDCUI",
        "MERGEDLUI",
        "MRAUI",
        "MRCOLS",
        "MRCONSO",
        "MRCUI",
        "MRCXT",
        "MRDEF",
        "MRDOC",
        "MRFILES",
        "MRHIER",
        "MRHIST",
        "MRMAP",
        "MRRANK",
        "MRREL",
        "MRSAB",
        "MRSAT",
        "MRSMAP",
        "MRSTY",
        "MRXNS_ENG",
        "MRXNW_ENG",
        "MRXW_BAQ",
        "MRXW_CHI",
        "MRXW_CZE",
        "MRXW_DAN",
        "MRXW_DUT",
        "MRXW_ENG",
        "MRXW_EST",
        "MRXW_FIN",
        "MRXW_FRE",
        "MRXW_GER",
        "MRXW_GRE",
        "MRXW_HEB",
        "MRXW_HUN",
        "MRXW_ITA",
        "MRXW_JPN",
        "MRXW_KOR",
        "MRXW_LAV",
        "MRXW_NOR",
        "MRXW_POL",
        "MRXW_POR",
        "MRXW_RUS",
        "MRXW_SCR",
        "MRXW_SPA",
        "MRXW_SWE",
        "MRXW_TUR"
      )



    if (english_only) {
      tables <-
        tables[!(tables %in%  c(
                              'MRXW_BAQ',
                              'MRXW_CHI',
                              'MRXW_CZE',
                              'MRXW_DAN',
                              'MRXW_DUT',
                              'MRXW_EST',
                              'MRXW_FIN',
                              'MRXW_FRE',
                              'MRXW_GER',
                              'MRXW_GRE',
                              'MRXW_HEB',
                              'MRXW_HUN',
                              'MRXW_ITA',
                              'MRXW_JPN',
                              'MRXW_KOR',
                              'MRXW_LAV',
                              'MRXW_NOR',
                              'MRXW_POL',
                              'MRXW_POR',
                              'MRXW_RUS',
                              'MRXW_SCR',
                              'MRXW_SPA',
                              'MRXW_SWE',
                              'MRXW_TUR'
                            ))]

    }

    if ("reset_schema" %in% steps) {


      if (
      pg13::schema_exists(conn = conn,
                          schema = schema)) {

      pg13::send(
        conn = conn,
        sql_statement =
          glue::glue("DROP SCHEMA {schema} CASCADE;"),
        verbose = verbose,
        render_sql = render_sql
      )

      }

      pg13::send(
        conn = conn,
        sql_statement =
          glue::glue("CREATE SCHEMA {schema};"),
        verbose = verbose,
        render_sql = render_sql
      )

    }


    if ("ddl_tables" %in% steps) {

ddl <-
list(
MRCOLS =
'DROP TABLE IF EXISTS {schema}.MRCOLS;
CREATE TABLE {schema}.MRCOLS (
COL	varchar(40),
DES	varchar(200),
REF	varchar(40),
MIN	integer,
AV	numeric(5,2),
MAX	integer,
FIL	varchar(50),
DTY	varchar(40),
FILLER_COL INTEGER
)',
MRCONSO =
'DROP TABLE IF EXISTS {schema}.MRCONSO;
CREATE TABLE {schema}.MRCONSO (
CUI	char(8) NOT NULL,
LAT	char(3) NOT NULL,
TS	char(1) NOT NULL,
LUI	varchar(10) NOT NULL,
STT	varchar(3) NOT NULL,
SUI	varchar(10) NOT NULL,
ISPREF	char(1) NOT NULL,
AUI	varchar(9) NOT NULL,
SAUI	varchar(50),
SCUI	varchar(100),
SDUI	varchar(100),
SAB	varchar(40) NOT NULL,
TTY	varchar(40) NOT NULL,
CODE	varchar(100) NOT NULL,
STR	text NOT NULL,
SRL	integer NOT NULL,
SUPPRESS	char(1) NOT NULL,
CVF	integer,
FILLER_COL INTEGER
)',
MRCUI =
'DROP TABLE IF EXISTS {schema}.MRCUI;
CREATE TABLE {schema}.MRCUI (
CUI1	char(8) NOT NULL,
VER	varchar(10) NOT NULL,
REL	varchar(4) NOT NULL,
RELA	varchar(100),
MAPREASON	text,
CUI2	char(8),
MAPIN	char(1),
FILLER_COL INTEGER
)',
MRCXT =
'DROP TABLE IF EXISTS {schema}.MRCXT;
CREATE TABLE {schema}.MRCXT (
CUI	char(8),
SUI	varchar(10),
AUI	varchar(9),
SAB	varchar(40),
CODE	varchar(100),
CXN	integer,
CXL	char(3),
MRCXTRANK	integer,
CXS	text,
CUI2	char(8),
AUI2	varchar(9),
HCD	varchar(100),
RELA	varchar(100),
XC	varchar(1),
CVF	integer,
FILLER_COL INTEGER
)',
MRDEF =
'DROP TABLE IF EXISTS {schema}.MRDEF;
CREATE TABLE {schema}.MRDEF (
CUI	char(8) NOT NULL,
AUI	varchar(9) NOT NULL,
ATUI	varchar(11) NOT NULL,
SATUI	varchar(50),
SAB	varchar(40) NOT NULL,
DEF	text NOT NULL,
SUPPRESS	char(1) NOT NULL,
CVF	integer,
FILLER_COL INTEGER
)',
MRDOC =
'DROP TABLE IF EXISTS {schema}.MRDOC;
CREATE TABLE {schema}.MRDOC (
DOCKEY	varchar(50) NOT NULL,
VALUE	varchar(200),
TYPE	varchar(50) NOT NULL,
EXPL	text,
FILLER_COL INTEGER
)',
MRFILES =
'DROP TABLE IF EXISTS {schema}.MRFILES;
CREATE TABLE {schema}.MRFILES (
FIL	varchar(50),
DES	varchar(200),
FMT	text,
CLS	integer,
RWS	integer,
BTS	bigint,
FILLER_COL INTEGER
)',
MRHIER =
'DROP TABLE IF EXISTS {schema}.MRHIER;
CREATE TABLE {schema}.MRHIER (
CUI	char(8) NOT NULL,
AUI	varchar(9) NOT NULL,
CXN	integer NOT NULL,
PAUI	varchar(10),
SAB	varchar(40) NOT NULL,
RELA	varchar(100),
PTR	text,
HCD	varchar(100),
CVF	integer,
FILLER_COL INTEGER
)',
MRHIST =
'DROP TABLE IF EXISTS {schema}.MRHIST;
CREATE TABLE {schema}.MRHIST (
CUI	char(8),
SOURCEUI	varchar(100),
SAB	varchar(40),
SVER	varchar(40),
CHANGETYPE	text,
CHANGEKEY	text,
CHANGEVAL	text,
REASON	text,
CVF	integer,
FILLER_COL INTEGER
)',
MRMAP =
'DROP TABLE IF EXISTS {schema}.MRMAP;
CREATE TABLE {schema}.MRMAP (
MAPSETCUI	char(8) NOT NULL,
MAPSETSAB	varchar(40) NOT NULL,
MAPSUBSETID	varchar(10),
MAPRANK	integer,
MAPID	varchar(50) NOT NULL,
MAPSID	varchar(50),
FROMID	varchar(50) NOT NULL,
FROMSID	varchar(50),
FROMEXPR	text NOT NULL,
FROMTYPE	varchar(50) NOT NULL,
FROMRULE	text,
FROMRES	text,
REL	varchar(4) NOT NULL,
RELA	varchar(100),
TOID	varchar(50),
TOSID	varchar(50),
TOEXPR	text,
TOTYPE	varchar(50),
TORULE	text,
TORES	text,
MAPRULE	text,
MAPRES	text,
MAPTYPE	varchar(50),
MAPATN	varchar(100),
MAPATV	text,
CVF	integer,
FILLER_COL INTEGER
)',
MRRANK =
'DROP TABLE IF EXISTS {schema}.MRRANK;
CREATE TABLE {schema}.MRRANK (
MRRANK_RANK	integer NOT NULL,
SAB	varchar(40) NOT NULL,
TTY	varchar(40) NOT NULL,
SUPPRESS	char(1) NOT NULL,
FILLER_COL INTEGER
)',
MRREL =
'DROP TABLE IF EXISTS {schema}.MRREL;
CREATE TABLE {schema}.MRREL (
CUI1	char(8) NOT NULL,
AUI1	varchar(9),
STYPE1	varchar(50) NOT NULL,
REL	varchar(4) NOT NULL,
CUI2	char(8) NOT NULL,
AUI2	varchar(9),
STYPE2	varchar(50) NOT NULL,
RELA	varchar(100),
RUI	varchar(10) NOT NULL,
SRUI	varchar(50),
SAB	varchar(40) NOT NULL,
SL	varchar(40) NOT NULL,
RG	varchar(10),
DIR	varchar(1),
SUPPRESS	char(1) NOT NULL,
CVF	integer,
FILLER_COL INTEGER
)',
MRSAB =
'DROP TABLE IF EXISTS {schema}.MRSAB;
CREATE TABLE {schema}.MRSAB (
VCUI	char(8),
RCUI	char(8),
VSAB	varchar(40) NOT NULL,
RSAB	varchar(40) NOT NULL,
SON	text NOT NULL,
SF	varchar(40) NOT NULL,
SVER	varchar(40),
VSTART	char(8),
VEND	char(8),
IMETA	varchar(10) NOT NULL,
RMETA	varchar(10),
SLC	text,
SCC	text,
SRL	integer NOT NULL,
TFR	integer,
CFR	integer,
CXTY	varchar(50),
TTYL	varchar(400),
ATNL	text,
LAT	char(3),
CENC	varchar(40) NOT NULL,
CURVER	char(1) NOT NULL,
SABIN	char(1) NOT NULL,
SSN	text NOT NULL,
SCIT	text NOT NULL,
FILLER_COL INTEGER
)',
MRSAT =
'DROP TABLE IF EXISTS {schema}.MRSAT;
CREATE TABLE {schema}.MRSAT (
CUI	char(8) NOT NULL,
LUI	varchar(10),
SUI	varchar(10),
METAUI	varchar(100),
STYPE	varchar(50) NOT NULL,
CODE	varchar(100),
ATUI	varchar(11) NOT NULL,
SATUI	varchar(50),
ATN	varchar(100) NOT NULL,
SAB	varchar(40) NOT NULL,
ATV	text,
SUPPRESS	char(1) NOT NULL,
CVF	integer,
FILLER_COL INTEGER
)',
MRSMAP =
'DROP TABLE IF EXISTS {schema}.MRSMAP;
CREATE TABLE {schema}.MRSMAP (
MAPSETCUI	char(8) NOT NULL,
MAPSETSAB	varchar(40) NOT NULL,
MAPID	varchar(50) NOT NULL,
MAPSID	varchar(50),
FROMEXPR	text NOT NULL,
FROMTYPE	varchar(50) NOT NULL,
REL	varchar(4) NOT NULL,
RELA	varchar(100),
TOEXPR	text,
TOTYPE	varchar(50),
CVF	integer,
FILLER_COL INTEGER
)',
MRSTY =
'DROP TABLE IF EXISTS {schema}.MRSTY;
CREATE TABLE {schema}.MRSTY (
CUI	char(8) NOT NULL,
TUI	char(4) NOT NULL,
STN	varchar(100) NOT NULL,
STY	varchar(50) NOT NULL,
ATUI	varchar(11) NOT NULL,
CVF	integer,
FILLER_COL INTEGER
)',
MRXNS_ENG =
'DROP TABLE IF EXISTS {schema}.MRXNS_ENG;
CREATE TABLE {schema}.MRXNS_ENG (
LAT	char(3) NOT NULL,
NSTR	text NOT NULL,
CUI	char(8) NOT NULL,
LUI	varchar(10) NOT NULL,
SUI	varchar(10) NOT NULL,
FILLER_COL INTEGER
)',
MRXNW_ENG =
'DROP TABLE IF EXISTS {schema}.MRXNW_ENG;
CREATE TABLE {schema}.MRXNW_ENG (
LAT	char(3) NOT NULL,
NWD	varchar(100) NOT NULL,
CUI	char(8) NOT NULL,
LUI	varchar(10) NOT NULL,
SUI	varchar(10) NOT NULL,
FILLER_COL INTEGER
)',
MRAUI =
'DROP TABLE IF EXISTS {schema}.MRAUI;
CREATE TABLE {schema}.MRAUI (
AUI1	varchar(9) NOT NULL,
CUI1	char(8) NOT NULL,
VER	varchar(10) NOT NULL,
REL	varchar(4),
RELA	varchar(100),
MAPREASON	text NOT NULL,
AUI2	varchar(9) NOT NULL,
CUI2	char(8) NOT NULL,
MAPIN	char(1) NOT NULL,
FILLER_COL INTEGER
)',
MRXW_BAQ =
'DROP TABLE IF EXISTS {schema}.MRXW_BAQ;
CREATE TABLE {schema}.MRXW_BAQ (
LAT	char(3) NOT NULL,
WD	varchar(200) NOT NULL,
CUI	char(8) NOT NULL,
LUI	varchar(10) NOT NULL,
SUI	varchar(10) NOT NULL,
FILLER_COL INTEGER
)',
MRXW_CHI =
'DROP TABLE IF EXISTS {schema}.MRXW_CHI;
CREATE TABLE {schema}.MRXW_CHI (
LAT	char(3) NOT NULL,
WD	varchar(200) NOT NULL,
CUI	char(8) NOT NULL,
LUI	varchar(10) NOT NULL,
SUI	varchar(10) NOT NULL,
FILLER_COL INTEGER
)',
MRXW_CZE =
'DROP TABLE IF EXISTS {schema}.MRXW_CZE;
CREATE TABLE {schema}.MRXW_CZE (
LAT	char(3) NOT NULL,
WD	varchar(200) NOT NULL,
CUI	char(8) NOT NULL,
LUI	varchar(10) NOT NULL,
SUI	varchar(10) NOT NULL,
FILLER_COL INTEGER
)',
MRXW_DAN =
'DROP TABLE IF EXISTS {schema}.MRXW_DAN;
CREATE TABLE {schema}.MRXW_DAN (
LAT	char(3) NOT NULL,
WD	varchar(200) NOT NULL,
CUI	char(8) NOT NULL,
LUI	varchar(10) NOT NULL,
SUI	varchar(10) NOT NULL,
FILLER_COL INTEGER
)',
MRXW_DUT =
'DROP TABLE IF EXISTS {schema}.MRXW_DUT;
CREATE TABLE {schema}.MRXW_DUT (
LAT	char(3) NOT NULL,
WD	varchar(200) NOT NULL,
CUI	char(8) NOT NULL,
LUI	varchar(10) NOT NULL,
SUI	varchar(10) NOT NULL,
FILLER_COL INTEGER
)',
MRXW_ENG =
'DROP TABLE IF EXISTS {schema}.MRXW_ENG;
CREATE TABLE {schema}.MRXW_ENG (
LAT	char(3) NOT NULL,
WD	varchar(200) NOT NULL,
CUI	char(8) NOT NULL,
LUI	varchar(10) NOT NULL,
SUI	varchar(10) NOT NULL,
FILLER_COL INTEGER
)',
MRXW_EST =
'DROP TABLE IF EXISTS {schema}.MRXW_EST;
CREATE TABLE {schema}.MRXW_EST (
LAT	char(3) NOT NULL,
WD	varchar(200) NOT NULL,
CUI	char(8) NOT NULL,
LUI	varchar(10) NOT NULL,
SUI	varchar(10) NOT NULL,
FILLER_COL INTEGER
)',
MRXW_FIN =
'DROP TABLE IF EXISTS {schema}.MRXW_FIN;
CREATE TABLE {schema}.MRXW_FIN (
LAT	char(3) NOT NULL,
WD	varchar(200) NOT NULL,
CUI	char(8) NOT NULL,
LUI	varchar(10) NOT NULL,
SUI	varchar(10) NOT NULL,
FILLER_COL INTEGER
)',
MRXW_FRE =
'DROP TABLE IF EXISTS {schema}.MRXW_FRE;
CREATE TABLE {schema}.MRXW_FRE (
LAT	char(3) NOT NULL,
WD	varchar(200) NOT NULL,
CUI	char(8) NOT NULL,
LUI	varchar(10) NOT NULL,
SUI	varchar(10) NOT NULL,
FILLER_COL INTEGER
)',
MRXW_GER =
'DROP TABLE IF EXISTS {schema}.MRXW_GER;
CREATE TABLE {schema}.MRXW_GER (
LAT	char(3) NOT NULL,
WD	varchar(200) NOT NULL,
CUI	char(8) NOT NULL,
LUI	varchar(10) NOT NULL,
SUI	varchar(10) NOT NULL,
FILLER_COL INTEGER
)',
MRXW_GRE =
'DROP TABLE IF EXISTS {schema}.MRXW_GRE;
CREATE TABLE {schema}.MRXW_GRE (
LAT	char(3) NOT NULL,
WD	varchar(200) NOT NULL,
CUI	char(8) NOT NULL,
LUI	varchar(10) NOT NULL,
SUI	varchar(10) NOT NULL,
FILLER_COL INTEGER
)',
MRXW_HEB =
'DROP TABLE IF EXISTS {schema}.MRXW_HEB;
CREATE TABLE {schema}.MRXW_HEB (
LAT	char(3) NOT NULL,
WD	varchar(200) NOT NULL,
CUI	char(8) NOT NULL,
LUI	varchar(10) NOT NULL,
SUI	varchar(10) NOT NULL,
FILLER_COL INTEGER
)',
MRXW_HUN =
'DROP TABLE IF EXISTS {schema}.MRXW_HUN;
CREATE TABLE {schema}.MRXW_HUN (
LAT	char(3) NOT NULL,
WD	varchar(200) NOT NULL,
CUI	char(8) NOT NULL,
LUI	varchar(10) NOT NULL,
SUI	varchar(10) NOT NULL,
FILLER_COL INTEGER
)',
MRXW_ITA =
'DROP TABLE IF EXISTS {schema}.MRXW_ITA;
CREATE TABLE {schema}.MRXW_ITA (
LAT	char(3) NOT NULL,
WD	varchar(200) NOT NULL,
CUI	char(8) NOT NULL,
LUI	varchar(10) NOT NULL,
SUI	varchar(10) NOT NULL,
FILLER_COL INTEGER
)',
MRXW_JPN =
'DROP TABLE IF EXISTS {schema}.MRXW_JPN;
CREATE TABLE {schema}.MRXW_JPN (
LAT char(3) NOT NULL,
WD  varchar(500) NOT NULL,
CUI char(8) NOT NULL,
LUI varchar(10) NOT NULL,
SUI varchar(10) NOT NULL,
FILLER_COL INTEGER
)',
MRXW_KOR =
'DROP TABLE IF EXISTS {schema}.MRXW_KOR;
CREATE TABLE {schema}.MRXW_KOR (
LAT char(3) NOT NULL,
WD  varchar(500) NOT NULL,
CUI char(8) NOT NULL,
LUI varchar(10) NOT NULL,
SUI varchar(10) NOT NULL,
FILLER_COL INTEGER
)',
MRXW_LAV =
'DROP TABLE IF EXISTS {schema}.MRXW_LAV;
CREATE TABLE {schema}.MRXW_LAV (
LAT char(3) NOT NULL,
WD  varchar(200) NOT NULL,
CUI char(8) NOT NULL,
LUI varchar(10) NOT NULL,
SUI varchar(10) NOT NULL,
FILLER_COL INTEGER
)',
MRXW_NOR =
'DROP TABLE IF EXISTS {schema}.MRXW_NOR;
CREATE TABLE {schema}.MRXW_NOR (
LAT	char(3) NOT NULL,
WD	varchar(200) NOT NULL,
CUI	char(8) NOT NULL,
LUI	varchar(10) NOT NULL,
SUI	varchar(10) NOT NULL,
FILLER_COL INTEGER
)',
MRXW_POL =
'DROP TABLE IF EXISTS {schema}.MRXW_POL;
CREATE TABLE {schema}.MRXW_POL (
LAT	char(3) NOT NULL,
WD	varchar(200) NOT NULL,
CUI	char(8) NOT NULL,
LUI	varchar(10) NOT NULL,
SUI	varchar(10) NOT NULL,
FILLER_COL INTEGER
)',
MRXW_POR =
'DROP TABLE IF EXISTS {schema}.MRXW_POR;
CREATE TABLE {schema}.MRXW_POR (
LAT	char(3) NOT NULL,
WD	varchar(200) NOT NULL,
CUI	char(8) NOT NULL,
LUI	varchar(10) NOT NULL,
SUI	varchar(10) NOT NULL,
FILLER_COL INTEGER
)',
MRXW_RUS =
'DROP TABLE IF EXISTS {schema}.MRXW_RUS;
CREATE TABLE {schema}.MRXW_RUS (
LAT char(3) NOT NULL,
WD  varchar(200) NOT NULL,
CUI char(8) NOT NULL,
LUI varchar(10) NOT NULL,
SUI varchar(10) NOT NULL,
FILLER_COL INTEGER
)',
MRXW_SCR =
'DROP TABLE IF EXISTS {schema}.MRXW_SCR;
CREATE TABLE {schema}.MRXW_SCR (
LAT char(3) NOT NULL,
WD  varchar(200) NOT NULL,
CUI char(8) NOT NULL,
LUI varchar(10) NOT NULL,
SUI varchar(10) NOT NULL,
FILLER_COL INTEGER
)',
MRXW_SPA =
'DROP TABLE IF EXISTS {schema}.MRXW_SPA;
CREATE TABLE {schema}.MRXW_SPA (
LAT	char(3) NOT NULL,
WD	varchar(200) NOT NULL,
CUI	char(8) NOT NULL,
LUI	varchar(10) NOT NULL,
SUI	varchar(10) NOT NULL,
FILLER_COL INTEGER
)',
MRXW_SWE =
'DROP TABLE IF EXISTS {schema}.MRXW_SWE;
CREATE TABLE {schema}.MRXW_SWE (
LAT	char(3) NOT NULL,
WD	varchar(200) NOT NULL,
CUI	char(8) NOT NULL,
LUI	varchar(10) NOT NULL,
SUI	varchar(10) NOT NULL,
FILLER_COL INTEGER
)',
MRXW_TUR =
'DROP TABLE IF EXISTS {schema}.MRXW_TUR;
CREATE TABLE {schema}.MRXW_TUR (
LAT	char(3) NOT NULL,
WD	varchar(200) NOT NULL,
CUI	char(8) NOT NULL,
LUI	varchar(10) NOT NULL,
SUI	varchar(10) NOT NULL,
FILLER_COL INTEGER
)',
AMBIGSUI =
'DROP TABLE IF EXISTS {schema}.AMBIGSUI;
CREATE TABLE {schema}.AMBIGSUI (
SUI	varchar(10) NOT NULL,
CUI	char(8) NOT NULL,
FILLER_COL INTEGER
)',
AMBIGLUI =
'DROP TABLE IF EXISTS {schema}.AMBIGLUI;
CREATE TABLE {schema}.AMBIGLUI (
LUI	varchar(10) NOT NULL,
CUI	char(8) NOT NULL,
FILLER_COL INTEGER
)',
DELETEDCUI =
'DROP TABLE IF EXISTS {schema}.DELETEDCUI;
CREATE TABLE {schema}.DELETEDCUI (
PCUI	char(8) NOT NULL,
PSTR	text NOT NULL,
FILLER_COL INTEGER
)',
DELETEDLUI =
'DROP TABLE IF EXISTS {schema}.DELETEDLUI;
CREATE TABLE {schema}.DELETEDLUI (
PLUI	varchar(10) NOT NULL,
PSTR	text NOT NULL,
FILLER_COL INTEGER
)',
DELETEDSUI =
'DROP TABLE IF EXISTS {schema}.DELETEDSUI;
CREATE TABLE {schema}.DELETEDSUI (
PSUI	varchar(10) NOT NULL,
LAT	char(3) NOT NULL,
PSTR	text NOT NULL,
FILLER_COL INTEGER
)',
MERGEDCUI =
'DROP TABLE IF EXISTS {schema}.MERGEDCUI;
CREATE TABLE {schema}.MERGEDCUI (
PCUI	char(8) NOT NULL,
CUI	char(8) NOT NULL,
FILLER_COL INTEGER
)',
MERGEDLUI =
'DROP TABLE IF EXISTS {schema}.MERGEDLUI;
CREATE TABLE {schema}.MERGEDLUI (
PLUI	varchar(10),
LUI	varchar(10),
FILLER_COL INTEGER
)')

ddl <- ddl[tables]

for (i in seq_along(ddl)) {

  pg13::send(
    conn = conn,
    sql_statement = glue::glue(ddl[[i]]),
    verbose = verbose,
    render_sql = render_sql
  )


}
    }

    if ("copy_rrfs" %in% steps) {

      rrf_files <-
        list.files(
          path = path_to_rrfs,
          recursive = TRUE,
          pattern = "[.]RRF$",
          full.names = TRUE
        ) %>%
        tibble::as_tibble_col("filePaths") %>%
        dplyr::mutate(baseNames = basename(filePaths)) %>%
        dplyr::mutate(tableNames = stringr::str_remove_all(baseNames, "[.]RRF$")) %>%
        dplyr::filter(tableNames %in% tables) %>%
        dplyr::select(filePaths) %>%
        dplyr::distinct() %>%
        unlist()

      pb <- progress::progress_bar$new(
        format = "    :what [:bar] :current/:total :percent :elapsedfull",
        total = length(tables),
        clear = FALSE,
        width = 60
      )

      pb$tick(0)
      Sys.sleep(0.2)


      errors <- vector()
      for (i in 1:length(tables)) {
        table <- tables[i]
        rrf_path <- path.expand(
          grep(
            pattern = table,
            rrf_files,
            value = TRUE
          )
        )

        pb$tick(tokens = list(what = table))
        Sys.sleep(1)
        cat("\n")

        sql <- glue::glue(
          "COPY {schema}.{table} FROM '{rrf_path}' WITH DELIMITER E'|' CSV QUOTE E'\b';")


        output <-
          tryCatch(
            pg13::send(
              conn = conn,
              sql_statement = sql,
              verbose = verbose,
              render_sql = render_sql
            ),
            error = function(e) "Error"
          )

        if (identical(output, "Error")) {
            errors <-
              c(
                errors,
                table
              )
        }
      }

      if (length(errors) > 0) {
        typewrite("Tables:")
        tables %>%
          purrr::map(~ typewrite(.))

        typewrite_warning("Some tables did not copy:")
        errors %>%
          purrr::map(~ typewrite(italicize(.)))
      } else {
        typewrite("All tables copied:")
        tables %>%
          purrr::map(~ typewrite(.))
      }
    }


    if ("add_indexes" %in% steps) {


      indexes <-
        c(
          "CREATE INDEX X_MRCOC_CUI1 ON {schema}.MRCOC(CUI1);",
          "CREATE INDEX X_MRCOC_AUI1 ON {schema}.MRCOC(AUI1);",
          "CREATE INDEX X_MRCOC_CUI2 ON {schema}.MRCOC(CUI2);",
          "CREATE INDEX X_MRCOC_AUI2 ON {schema}.MRCOC(AUI2);",
          "CREATE INDEX X_MRCOC_SAB ON {schema}.MRCOC(SAB);",
          "CREATE INDEX X_MRCONSO_CUI ON {schema}.MRCONSO(CUI);",
          "ALTER TABLE {schema}.MRCONSO ADD CONSTRAINT X_MRCONSO_PK PRIMARY KEY (AUI);",
          "CREATE INDEX X_MRCONSO_SUI ON {schema}.MRCONSO(SUI);",
          "CREATE INDEX X_MRCONSO_LUI ON {schema}.MRCONSO(LUI);",
          "CREATE INDEX X_MRCONSO_CODE ON {schema}.MRCONSO(CODE);",
          "CREATE INDEX X_MRCONSO_SAB_TTY ON {schema}.MRCONSO(SAB,TTY);",
          "CREATE INDEX X_MRCONSO_SCUI ON {schema}.MRCONSO(SCUI);",
          "CREATE INDEX X_MRCONSO_STR ON {schema}.MRCONSO(STR);",
          "CREATE INDEX X_MRCXT_CUI ON {schema}.MRCXT(CUI);",
          "CREATE INDEX X_MRCXT_AUI ON {schema}.MRCXT(AUI);",
          "CREATE INDEX X_MRCXT_SAB ON {schema}.MRCXT(SAB);",
          "CREATE INDEX X_MRDEF_CUI ON {schema}.MRDEF(CUI);",
          "CREATE INDEX X_MRDEF_AUI ON {schema}.MRDEF(AUI);",
          "ALTER TABLE {schema}.MRDEF ADD CONSTRAINT X_MRDEF_PK PRIMARY KEY (ATUI);",
          "CREATE INDEX X_MRDEF_SAB ON {schema}.MRDEF(SAB);",
          "CREATE INDEX X_MRHIER_CUI ON {schema}.MRHIER(CUI);",
          "CREATE INDEX X_MRHIER_AUI ON {schema}.MRHIER(AUI);",
          "CREATE INDEX X_MRHIER_SAB ON {schema}.MRHIER(SAB);",
          "CREATE INDEX X_MRHIER_PTR ON {schema}.MRHIER(PTR);",
          "CREATE INDEX X_MRHIER_PAUI ON {schema}.MRHIER(PAUI);",
          "CREATE INDEX X_MRHIST_CUI ON {schema}.MRHIST(CUI);",
          "CREATE INDEX X_MRHIST_SOURCEUI ON {schema}.MRHIST(SOURCEUI);",
          "CREATE INDEX X_MRHIST_SAB ON {schema}.MRHIST(SAB);",
          "ALTER TABLE {schema}.MRRANK ADD CONSTRAINT X_MRRANK_PK PRIMARY KEY (SAB,TTY);",
          "CREATE INDEX X_MRREL_CUI1 ON {schema}.MRREL(CUI1);",
          "CREATE INDEX X_MRREL_AUI1 ON {schema}.MRREL(AUI1);",
          "CREATE INDEX X_MRREL_CUI2 ON {schema}.MRREL(CUI2);",
          "CREATE INDEX X_MRREL_AUI2 ON {schema}.MRREL(AUI2);",
          "ALTER TABLE {schema}.MRREL ADD CONSTRAINT X_MRREL_PK PRIMARY KEY (RUI);",
          "CREATE INDEX X_MRREL_SAB ON {schema}.MRREL(SAB);",
          "ALTER TABLE {schema}.MRSAB ADD CONSTRAINT X_MRSAB_PK PRIMARY KEY (VSAB);",
          "CREATE INDEX X_MRSAB_RSAB ON {schema}.MRSAB(RSAB);",
          "CREATE INDEX X_MRSAT_CUI ON {schema}.MRSAT(CUI);",
          "CREATE INDEX X_MRSAT_METAUI ON {schema}.MRSAT(METAUI);",
          "ALTER TABLE {schema}.MRSAT ADD CONSTRAINT X_MRSAT_PK PRIMARY KEY (ATUI);",
          "CREATE INDEX X_MRSAT_SAB ON {schema}.MRSAT(SAB);",
          "CREATE INDEX X_MRSAT_ATN ON {schema}.MRSAT(ATN);",
          "CREATE INDEX X_MRSTY_CUI ON {schema}.MRSTY(CUI);",
          "ALTER TABLE {schema}.MRSTY ADD CONSTRAINT X_MRSTY_PK PRIMARY KEY (ATUI);",
          "CREATE INDEX X_MRSTY_STY ON {schema}.MRSTY(STY);",
          "CREATE INDEX X_MRXNS_ENG_NSTR ON {schema}.MRXNS_ENG(NSTR);",
          "CREATE INDEX X_MRXNW_ENG_NWD ON {schema}.MRXNW_ENG(NWD);",
          "CREATE INDEX X_MRXW_BAQ_WD ON {schema}.MRXW_BAQ(WD);",
          "CREATE INDEX X_MRXW_CZE_WD ON {schema}.MRXW_CZE(WD);",
          "CREATE INDEX X_MRXW_DAN_WD ON {schema}.MRXW_DAN(WD);",
          "CREATE INDEX X_MRXW_DUT_WD ON {schema}.MRXW_DUT(WD);",
          "CREATE INDEX X_MRXW_ENG_WD ON {schema}.MRXW_ENG(WD);",
          "CREATE INDEX X_MRXW_FIN_WD ON {schema}.MRXW_FIN(WD);",
          "CREATE INDEX X_MRXW_FRE_WD ON {schema}.MRXW_FRE(WD);",
          "CREATE INDEX X_MRXW_GER_WD ON {schema}.MRXW_GER(WD);",
          "CREATE INDEX X_MRXW_HEB_WD ON {schema}.MRXW_HEB(WD);",
          "CREATE INDEX X_MRXW_HUN_WD ON {schema}.MRXW_HUN(WD);",
          "CREATE INDEX X_MRXW_ITA_WD ON {schema}.MRXW_ITA(WD);",
          "CREATE INDEX X_MRXW_JPN_WD ON {schema}.MRXW_JPN(WD);",
          "CREATE INDEX X_MRXW_NOR_WD ON {schema}.MRXW_NOR(WD);",
          "CREATE INDEX X_MRXW_POR_WD ON {schema}.MRXW_POR(WD);",
          "CREATE INDEX X_MRXW_RUS_WD ON {schema}.MRXW_RUS(WD);",
          "CREATE INDEX X_MRXW_SPA_WD ON {schema}.MRXW_SPA(WD);",
          "CREATE INDEX X_MRXW_SWE_WD ON {schema}.MRXW_SWE(WD);",
          "CREATE INDEX X_AMBIGSUI_SUI ON {schema}.AMBIGSUI(SUI);",
          "CREATE INDEX X_AMBIGLUI_LUI ON {schema}.AMBIGLUI(LUI);"
        )


      pb <- progress::progress_bar$new(
        format = "    [:bar] :current/:total :percent :elapsedfull",
        total = length(indexes),
        clear = FALSE,
        width = 60
      )

      pb$tick(0)
      Sys.sleep(0.2)


      errors <- vector()
      for (i in 1:length(indexes)) {
        index <- glue::glue(indexes[i])

        pb$tick()
        Sys.sleep(1)
        cat("\n")

        output <-
          tryCatch(
            pg13::send(
              conn = conn,
              sql_statement = index,
              verbose = verbose,
              render_sql = render_sql
            ),
            error = function(e) "Error"
          )


        if (identical(output, "Error")) {
            errors <-
              c(
                errors,
                index
              )
        }
      }

      if (length(errors) > 0) {
        typewrite_warning("Some indexes failed:")
        errors %>%
          purrr::map(~ typewrite(italicize(.)))
      }


    }

    # Log
    if ("log" %in% steps) {

      sql_statement <-
        glue::glue(
      "
      CREATE TABLE IF NOT EXISTS {log_schema}.{log_table_name} (
          sm_datetime timestamp without time zone NOT NULL,
          sm_version varchar(100) NOT NULL,
          sm_release_date varchar(25) NOT NULL,
          sm_schema varchar(25) NOT NULL,
          ambiglui int,
          ambigsui int,
          deletedcui int,
          deletedlui int,
          deletedsui int,
          mergedcui int,
          mergedlui int,
          mraui int,
          mrcols int,
          mrconso int,
          mrcui int,
          mrcxt int,
          mrdef int,
          mrdoc int,
          mrfiles int,
          mrhier int,
          mrhist int,
          mrmap int,
          mrrank int,
          mrrel int,
          mrsab int,
          mrsat int,
          mrsmap int,
          mrsty int,
          mrxns_eng int,
          mrxnw_eng int,
          mrxw_eng int
      );
      ")

      pg13::send(conn = conn,
                 sql_statement = sql_statement,
                 verbose = verbose,
                 render_sql = render_sql)


      table_names <-
        pg13::ls_tables(
          conn = conn,
          schema = schema,
          verbose = verbose,
          render_sql = render_sql
        )

      current_row_count <-
        table_names %>%
        purrr::map(function(x) {
          pg13::query(
            conn = conn,
            sql_statement = pg13::render_row_count(
              schema = schema,
              tableName = x
            )
          )
        }) %>%
        purrr::set_names(tolower(table_names)) %>%
        dplyr::bind_rows(.id = "Table") %>%
        dplyr::rename(Rows = count) %>%
        tidyr::pivot_wider(
          names_from = "Table",
          values_from = "Rows"
        ) %>%
        dplyr::mutate(
          sm_datetime = as.character(Sys.time()),
          sm_version = log_version,
          sm_release_date = log_release_date,
          sm_schema = schema
        ) %>%
        dplyr::select(
          sm_datetime,
          sm_version,
          sm_release_date,
          sm_schema,
          dplyr::all_of(c(
            'ambiglui',
            'ambigsui',
            'deletedcui',
            'deletedlui',
            'deletedsui',
            'mergedcui',
            'mergedlui',
            'mraui',
            'mrcols',
            'mrconso',
            'mrcui',
            'mrcxt',
            'mrdef',
            'mrdoc',
            'mrfiles',
            'mrhier',
            'mrhist',
            'mrmap',
            'mrrank',
            'mrrel',
            'mrsab',
            'mrsat',
            'mrsmap',
            'mrsty',
            'mrxns_eng',
            'mrxnw_eng',
            'mrxw_eng'
          ))
        )


      current_row_count <-
        unname(unlist(current_row_count))

      sql_statement <-
        glue::glue(
          "
          INSERT INTO {log_schema}.{log_table_name}
          VALUES ({glue::glue_collapse(glue::single_quote(current_row_count), sep = ', ')})
          ;
          "
        )

      pg13::send(conn = conn,
                 sql_statement = sql_statement,
                 verbose = verbose,
                 render_sql = render_sql)


      cli::cat_line()
      cli::cat_boxx("Log Results",
        float = "center"
      )

      sql_statement <-
        glue::glue(
          "
          SELECT *
          FROM {log_schema}.{log_table_name}
          WHERE sm_datetime IN (SELECT MAX(sm_datetime) FROM {log_schema}.{log_table_name});
          "
        )

      updated_log <-
      pg13::query(
        conn = conn,
        sql_statement = sql_statement,
        verbose = verbose,
        render_sql = render_sql,
        checks = ''
      )


      print(tibble::as_tibble(updated_log))
      cli::cat_line()
    }

    if ("setup_crosswalk" %in% steps) {

      setup_crosswalk_schema(
        conn = conn,
        crosswalk_schema = "umls_crosswalk"
      )

    }

    if (postprocess) {

      run_postprocessing(
        conn = conn,
        umls_version = log_version,
        umls_release_dt = log_release_date,
        verbose = verbose,
        render_sql = render_sql,
        render_only = render_only
      )

    }


  }

#' @title
#' Run Postprocessing Only
#' @rdname run_postprocessing
#' @export
#' @importFrom rlang parse_expr
#' @importFrom pg13 dc send

run_postprocessing <-
  function(conn,
           conn_fun = "pg13::local_connect()",
           umls_version,
           umls_release_dt,
           verbose = TRUE,
           render_sql = TRUE,
           render_only = FALSE) {

    if (missing(umls_version)|missing(umls_release_dt)) {
      stop("`umls_version` and `umls_release_dt` are required.")
    }


    if (missing(conn)) {
      conn <- eval(rlang::parse_expr(conn_fun))
      on.exit(expr = pg13::dc(conn = conn), add = TRUE,
              after = TRUE)
    }

    postprocess_sqls <-
      list.files(system.file(package = "metathesaurus",
                             "sql",
                             "Hierarchy"),
                 full.names = TRUE,
                 pattern = "[.]{1}sql$")

    for (postprocess_sql in postprocess_sqls) {

      sql_statement <-
        paste(readLines(con = postprocess_sql),
              collapse = "\n")


      sql_statement <-
        glue::glue(sql_statement)


      pg13::send(
        conn = conn,
        sql_statement = sql_statement,
        verbose = verbose,
        render_sql = render_sql,
        checks = ''
      )


    }
  }
