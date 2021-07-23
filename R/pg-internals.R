#' @title
#' Drop Cascade and Recreate Schema
#' @seealso
#'  \code{\link[pg13]{lsSchema}},\code{\link[pg13]{send}}
#'  \code{\link[SqlRender]{render}}
#' @rdname reset_schema
#' @export
#' @importFrom pg13 lsSchema send
#' @importFrom SqlRender render

reset_schema <-
  function(conn,
           schema,
           verbose = TRUE,
           render_sql = TRUE) {
    if (schema %in% pg13::lsSchema(conn = conn)) {
      pg13::send(
        conn = conn,
        sql_statement = SqlRender::render("DROP SCHEMA @schema CASCADE;",
          schema = schema
        ),
        verbose = verbose,
        render_sql = render_sql
      )
    }

    pg13::send(
      conn = conn,
      sql_statement = SqlRender::render("CREATE SCHEMA @schema;",
        schema = schema
      ),
      verbose = verbose,
      render_sql = render_sql
    )
  }


#' @title
#' DDL Tables
#' @seealso
#'  \code{\link[pg13]{send}}
#' @rdname ddl_tables
#' @export
#' @importFrom pg13 send
#' @importFrom SqlRender render

ddl_tables <-
  function(conn,
           schema,
           tables,
           verbose = TRUE,
           render_sql = TRUE) {
    ddl <-
      list(
        MRCOLS = "
                                        DROP TABLE IF EXISTS @schema.MRCOLS;
                                        CREATE TABLE @schema.MRCOLS (
                                                COL	varchar(40),
                                                DES	varchar(200),
                                                REF	varchar(40),
                                                MIN	integer,
                                                AV	numeric(5,2),
                                                MAX	integer,
                                                FIL	varchar(50),
                                                DTY	varchar(40),
                                                FILLER_COL INTEGER
                                        )",
        MRCONSO = "
                                        DROP TABLE IF EXISTS @schema.MRCONSO;
                                        CREATE TABLE @schema.MRCONSO (
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
                                        )",
        MRCUI = "
                                        DROP TABLE IF EXISTS @schema.MRCUI;
                                        CREATE TABLE @schema.MRCUI (
                                                CUI1	char(8) NOT NULL,
                                                VER	varchar(10) NOT NULL,
                                                REL	varchar(4) NOT NULL,
                                                RELA	varchar(100),
                                                MAPREASON	text,
                                                CUI2	char(8),
                                                MAPIN	char(1),
                                                FILLER_COL INTEGER
                                        )",
        MRCXT = "
                                        DROP TABLE IF EXISTS @schema.MRCXT;
                                        CREATE TABLE @schema.MRCXT (
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
                                        )",
        MRDEF = "
                                        DROP TABLE IF EXISTS @schema.MRDEF;
                                        CREATE TABLE @schema.MRDEF (
                                                CUI	char(8) NOT NULL,
                                                AUI	varchar(9) NOT NULL,
                                                ATUI	varchar(11) NOT NULL,
                                                SATUI	varchar(50),
                                                SAB	varchar(40) NOT NULL,
                                                DEF	text NOT NULL,
                                                SUPPRESS	char(1) NOT NULL,
                                                CVF	integer,
                                                FILLER_COL INTEGER
                                        )",
        MRDOC = "
                                        DROP TABLE IF EXISTS @schema.MRDOC;
                                        CREATE TABLE @schema.MRDOC (
                                                DOCKEY	varchar(50) NOT NULL,
                                                VALUE	varchar(200),
                                                TYPE	varchar(50) NOT NULL,
                                                EXPL	text,
                                                FILLER_COL INTEGER
                                        )",
        MRFILES = "
                                        DROP TABLE IF EXISTS @schema.MRFILES;
                                        CREATE TABLE @schema.MRFILES (
                                                FIL	varchar(50),
                                                DES	varchar(200),
                                                FMT	text,
                                                CLS	integer,
                                                RWS	integer,
                                                BTS	bigint,
                                                FILLER_COL INTEGER
                                        )",
        MRHIER = "
                                        DROP TABLE IF EXISTS @schema.MRHIER;
                                        CREATE TABLE @schema.MRHIER (
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
                                        )",
        MRHIST = "
                                        DROP TABLE IF EXISTS @schema.MRHIST;
                                        CREATE TABLE @schema.MRHIST (
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
                                        )",
        MRMAP = "
                                        DROP TABLE IF EXISTS @schema.MRMAP;
                                        CREATE TABLE @schema.MRMAP (
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
                                        )",
        MRRANK = "
                                        DROP TABLE IF EXISTS @schema.MRRANK;
                                        CREATE TABLE @schema.MRRANK (
                                                MRRANK_RANK	integer NOT NULL,
                                                SAB	varchar(40) NOT NULL,
                                                TTY	varchar(40) NOT NULL,
                                                SUPPRESS	char(1) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MRREL = "
                                        DROP TABLE IF EXISTS @schema.MRREL;
                                        CREATE TABLE @schema.MRREL (
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
                                        )",
        MRSAB = "
                                        DROP TABLE IF EXISTS @schema.MRSAB;
                                        CREATE TABLE @schema.MRSAB (
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
                                        )",
        MRSAT = "
                                        DROP TABLE IF EXISTS @schema.MRSAT;
                                        CREATE TABLE @schema.MRSAT (
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
                                        )",
        MRSMAP = "
                                        DROP TABLE IF EXISTS @schema.MRSMAP;
                                        CREATE TABLE @schema.MRSMAP (
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
                                        )",
        MRSTY = "
                                        DROP TABLE IF EXISTS @schema.MRSTY;
                                        CREATE TABLE @schema.MRSTY (
                                                CUI	char(8) NOT NULL,
                                                TUI	char(4) NOT NULL,
                                                STN	varchar(100) NOT NULL,
                                                STY	varchar(50) NOT NULL,
                                                ATUI	varchar(11) NOT NULL,
                                                CVF	integer,
                                                FILLER_COL INTEGER
                                        )",
        MRXNS_ENG = "
                                        DROP TABLE IF EXISTS @schema.MRXNS_ENG;
                                        CREATE TABLE @schema.MRXNS_ENG (
                                                LAT	char(3) NOT NULL,
                                                NSTR	text NOT NULL,
                                                CUI	char(8) NOT NULL,
                                                LUI	varchar(10) NOT NULL,
                                                SUI	varchar(10) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MRXNW_ENG = "
                                        DROP TABLE IF EXISTS @schema.MRXNW_ENG;
                                        CREATE TABLE @schema.MRXNW_ENG (
                                                LAT	char(3) NOT NULL,
                                                NWD	varchar(100) NOT NULL,
                                                CUI	char(8) NOT NULL,
                                                LUI	varchar(10) NOT NULL,
                                                SUI	varchar(10) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MRAUI = "
                                        DROP TABLE IF EXISTS @schema.MRAUI;
                                        CREATE TABLE @schema.MRAUI (
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
                                        )",
        MRXW_BAQ = "
                                        DROP TABLE IF EXISTS @schema.MRXW_BAQ;
                                        CREATE TABLE @schema.MRXW_BAQ (
                                                LAT	char(3) NOT NULL,
                                                WD	varchar(200) NOT NULL,
                                                CUI	char(8) NOT NULL,
                                                LUI	varchar(10) NOT NULL,
                                                SUI	varchar(10) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MRXW_CHI = "
                                        DROP TABLE IF EXISTS @schema.MRXW_CHI;
                                        CREATE TABLE @schema.MRXW_CHI (
                                                LAT	char(3) NOT NULL,
                                                WD	varchar(200) NOT NULL,
                                                CUI	char(8) NOT NULL,
                                                LUI	varchar(10) NOT NULL,
                                                SUI	varchar(10) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MRXW_CZE = "
                                        DROP TABLE IF EXISTS @schema.MRXW_CZE;
                                        CREATE TABLE @schema.MRXW_CZE (
                                                LAT	char(3) NOT NULL,
                                                WD	varchar(200) NOT NULL,
                                                CUI	char(8) NOT NULL,
                                                LUI	varchar(10) NOT NULL,
                                                SUI	varchar(10) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MRXW_DAN = "
                                        DROP TABLE IF EXISTS @schema.MRXW_DAN;
                                        CREATE TABLE @schema.MRXW_DAN (
                                                LAT	char(3) NOT NULL,
                                                WD	varchar(200) NOT NULL,
                                                CUI	char(8) NOT NULL,
                                                LUI	varchar(10) NOT NULL,
                                                SUI	varchar(10) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MRXW_DUT = "
                                        DROP TABLE IF EXISTS @schema.MRXW_DUT;
                                        CREATE TABLE @schema.MRXW_DUT (
                                                LAT	char(3) NOT NULL,
                                                WD	varchar(200) NOT NULL,
                                                CUI	char(8) NOT NULL,
                                                LUI	varchar(10) NOT NULL,
                                                SUI	varchar(10) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MRXW_ENG = "
                                        DROP TABLE IF EXISTS @schema.MRXW_ENG;
                                        CREATE TABLE @schema.MRXW_ENG (
                                                LAT	char(3) NOT NULL,
                                                WD	varchar(200) NOT NULL,
                                                CUI	char(8) NOT NULL,
                                                LUI	varchar(10) NOT NULL,
                                                SUI	varchar(10) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MRXW_EST = "
                                        DROP TABLE IF EXISTS @schema.MRXW_EST;
                                        CREATE TABLE @schema.MRXW_EST (
                                                LAT	char(3) NOT NULL,
                                                WD	varchar(200) NOT NULL,
                                                CUI	char(8) NOT NULL,
                                                LUI	varchar(10) NOT NULL,
                                                SUI	varchar(10) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MRXW_FIN = "
                                        DROP TABLE IF EXISTS @schema.MRXW_FIN;
                                        CREATE TABLE @schema.MRXW_FIN (
                                                LAT	char(3) NOT NULL,
                                                WD	varchar(200) NOT NULL,
                                                CUI	char(8) NOT NULL,
                                                LUI	varchar(10) NOT NULL,
                                                SUI	varchar(10) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MRXW_FRE = "
                                        DROP TABLE IF EXISTS @schema.MRXW_FRE;
                                        CREATE TABLE @schema.MRXW_FRE (
                                                LAT	char(3) NOT NULL,
                                                WD	varchar(200) NOT NULL,
                                                CUI	char(8) NOT NULL,
                                                LUI	varchar(10) NOT NULL,
                                                SUI	varchar(10) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MRXW_GER = "
                                        DROP TABLE IF EXISTS @schema.MRXW_GER;
                                        CREATE TABLE @schema.MRXW_GER (
                                                LAT	char(3) NOT NULL,
                                                WD	varchar(200) NOT NULL,
                                                CUI	char(8) NOT NULL,
                                                LUI	varchar(10) NOT NULL,
                                                SUI	varchar(10) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MRXW_GRE = "

                                        DROP TABLE IF EXISTS @schema.MRXW_GRE;
                                        CREATE TABLE @schema.MRXW_GRE (
                                                LAT	char(3) NOT NULL,
                                                WD	varchar(200) NOT NULL,
                                                CUI	char(8) NOT NULL,
                                                LUI	varchar(10) NOT NULL,
                                                SUI	varchar(10) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MRXW_HEB = "

                                        DROP TABLE IF EXISTS @schema.MRXW_HEB;
                                        CREATE TABLE @schema.MRXW_HEB (
                                                LAT	char(3) NOT NULL,
                                                WD	varchar(200) NOT NULL,
                                                CUI	char(8) NOT NULL,
                                                LUI	varchar(10) NOT NULL,
                                                SUI	varchar(10) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MRXW_HUN = "

                                        DROP TABLE IF EXISTS @schema.MRXW_HUN;
                                        CREATE TABLE @schema.MRXW_HUN (
                                                LAT	char(3) NOT NULL,
                                                WD	varchar(200) NOT NULL,
                                                CUI	char(8) NOT NULL,
                                                LUI	varchar(10) NOT NULL,
                                                SUI	varchar(10) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MRXW_ITA = "

                                        DROP TABLE IF EXISTS @schema.MRXW_ITA;
                                        CREATE TABLE @schema.MRXW_ITA (
                                                LAT	char(3) NOT NULL,
                                                WD	varchar(200) NOT NULL,
                                                CUI	char(8) NOT NULL,
                                                LUI	varchar(10) NOT NULL,
                                                SUI	varchar(10) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MRXW_JPN = "

                                        DROP TABLE IF EXISTS @schema.MRXW_JPN;
                                        CREATE TABLE @schema.MRXW_JPN (
                                                LAT char(3) NOT NULL,
                                                WD  varchar(500) NOT NULL,
                                                CUI char(8) NOT NULL,
                                                LUI varchar(10) NOT NULL,
                                                SUI varchar(10) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MRXW_KOR = "

                                        DROP TABLE IF EXISTS @schema.MRXW_KOR;
                                        CREATE TABLE @schema.MRXW_KOR (
                                                LAT char(3) NOT NULL,
                                                WD  varchar(500) NOT NULL,
                                                CUI char(8) NOT NULL,
                                                LUI varchar(10) NOT NULL,
                                                SUI varchar(10) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MRXW_LAV = "

                                        DROP TABLE IF EXISTS @schema.MRXW_LAV;
                                        CREATE TABLE @schema.MRXW_LAV (
                                                LAT char(3) NOT NULL,
                                                WD  varchar(200) NOT NULL,
                                                CUI char(8) NOT NULL,
                                                LUI varchar(10) NOT NULL,
                                                SUI varchar(10) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MRXW_NOR = "

                                        DROP TABLE IF EXISTS @schema.MRXW_NOR;
                                        CREATE TABLE @schema.MRXW_NOR (
                                                LAT	char(3) NOT NULL,
                                                WD	varchar(200) NOT NULL,
                                                CUI	char(8) NOT NULL,
                                                LUI	varchar(10) NOT NULL,
                                                SUI	varchar(10) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MRXW_POL = "

                                        DROP TABLE IF EXISTS @schema.MRXW_POL;
                                        CREATE TABLE @schema.MRXW_POL (
                                                LAT	char(3) NOT NULL,
                                                WD	varchar(200) NOT NULL,
                                                CUI	char(8) NOT NULL,
                                                LUI	varchar(10) NOT NULL,
                                                SUI	varchar(10) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MRXW_POR = "

                                        DROP TABLE IF EXISTS @schema.MRXW_POR;
                                        CREATE TABLE @schema.MRXW_POR (
                                                LAT	char(3) NOT NULL,
                                                WD	varchar(200) NOT NULL,
                                                CUI	char(8) NOT NULL,
                                                LUI	varchar(10) NOT NULL,
                                                SUI	varchar(10) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MRXW_RUS = "

                                        DROP TABLE IF EXISTS @schema.MRXW_RUS;
                                        CREATE TABLE @schema.MRXW_RUS (
                                                LAT char(3) NOT NULL,
                                                WD  varchar(200) NOT NULL,
                                                CUI char(8) NOT NULL,
                                                LUI varchar(10) NOT NULL,
                                                SUI varchar(10) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MRXW_SCR = "

                                        DROP TABLE IF EXISTS @schema.MRXW_SCR;
                                        CREATE TABLE @schema.MRXW_SCR (
                                                LAT char(3) NOT NULL,
                                                WD  varchar(200) NOT NULL,
                                                CUI char(8) NOT NULL,
                                                LUI varchar(10) NOT NULL,
                                                SUI varchar(10) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MRXW_SPA = "

                                        DROP TABLE IF EXISTS @schema.MRXW_SPA;
                                        CREATE TABLE @schema.MRXW_SPA (
                                                LAT	char(3) NOT NULL,
                                                WD	varchar(200) NOT NULL,
                                                CUI	char(8) NOT NULL,
                                                LUI	varchar(10) NOT NULL,
                                                SUI	varchar(10) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MRXW_SWE = "

                                        DROP TABLE IF EXISTS @schema.MRXW_SWE;
                                        CREATE TABLE @schema.MRXW_SWE (
                                                LAT	char(3) NOT NULL,
                                                WD	varchar(200) NOT NULL,
                                                CUI	char(8) NOT NULL,
                                                LUI	varchar(10) NOT NULL,
                                                SUI	varchar(10) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MRXW_TUR = "

                                        DROP TABLE IF EXISTS @schema.MRXW_TUR;
                                        CREATE TABLE @schema.MRXW_TUR (
                                                LAT	char(3) NOT NULL,
                                                WD	varchar(200) NOT NULL,
                                                CUI	char(8) NOT NULL,
                                                LUI	varchar(10) NOT NULL,
                                                SUI	varchar(10) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        AMBIGSUI = "

                                        DROP TABLE IF EXISTS @schema.AMBIGSUI;
                                        CREATE TABLE @schema.AMBIGSUI (
                                                SUI	varchar(10) NOT NULL,
                                                CUI	char(8) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        AMBIGLUI = "

                                        DROP TABLE IF EXISTS @schema.AMBIGLUI;
                                        CREATE TABLE @schema.AMBIGLUI (
                                                LUI	varchar(10) NOT NULL,
                                                CUI	char(8) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        DELETEDCUI = "

                                        DROP TABLE IF EXISTS @schema.DELETEDCUI;
                                        CREATE TABLE @schema.DELETEDCUI (
                                                PCUI	char(8) NOT NULL,
                                                PSTR	text NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        DELETEDLUI = "

                                        DROP TABLE IF EXISTS @schema.DELETEDLUI;
                                        CREATE TABLE @schema.DELETEDLUI (
                                                PLUI	varchar(10) NOT NULL,
                                                PSTR	text NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        DELETEDSUI = "

                                        DROP TABLE IF EXISTS @schema.DELETEDSUI;
                                        CREATE TABLE @schema.DELETEDSUI (
                                                PSUI	varchar(10) NOT NULL,
                                                LAT	char(3) NOT NULL,
                                                PSTR	text NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MERGEDCUI = "

                                        DROP TABLE IF EXISTS @schema.MERGEDCUI;
                                        CREATE TABLE @schema.MERGEDCUI (
                                                PCUI	char(8) NOT NULL,
                                                CUI	char(8) NOT NULL,
                                                FILLER_COL INTEGER
                                        )",
        MERGEDLUI = "

                                        DROP TABLE IF EXISTS @schema.MERGEDLUI;
                                        CREATE TABLE @schema.MERGEDLUI (
                                                PLUI	varchar(10),
                                                LUI	varchar(10),
                                                FILLER_COL INTEGER
                                        )"
      )

    ddl <- ddl[names(ddl) %in% tables]

    for (i in 1:length(ddl)) {
      pg13::send(
        conn = conn,
        sql_statement = SqlRender::render(ddl[[i]], schema = schema),
        verbose = verbose,
        render_sql = render_sql
      )
    }
  }



#' @title
#' Copy RRF to Table
#' @seealso
#'  \code{\link[tibble]{as_tibble}}
#'  \code{\link[dplyr]{mutate}},\code{\link[dplyr]{filter}},\code{\link[dplyr]{select}},\code{\link[dplyr]{distinct}}
#'  \code{\link[stringr]{str_remove}}
#'  \code{\link[progress]{progress_bar}}
#'  \code{\link[SqlRender]{render}}
#'  \code{\link[pg13]{send}}
#'  \code{\link[secretary]{c("typewrite", "typewrite")}},\code{\link[secretary]{typewrite_warning}},\code{\link[secretary]{italicize}}
#'  \code{\link[purrr]{map}}
#' @rdname copy_rrfs
#' @export
#' @importFrom tibble as_tibble_col
#' @importFrom dplyr mutate filter select distinct
#' @importFrom stringr str_remove_all
#' @importFrom progress progress_bar
#' @importFrom SqlRender render
#' @importFrom pg13 send
#' @importFrom secretary typewrite typewrite_warning italicize
#' @importFrom purrr map


copy_rrfs <-
  function(path_to_rrfs,
           tables,
           conn,
           schema,
           verbose = TRUE,
           render_sql = TRUE) {
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

      sql <- SqlRender::render(
        "COPY @schema.@tableName FROM '@rrf_path' WITH DELIMITER E'|' CSV QUOTE E'\b';",
        schema = schema,
        tableName = table,
        rrf_path = rrf_path
      )


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

      if (length(output) == 1) {
        if (output == "Error") {
          errors <-
            c(
              errors,
              table
            )
        }
      }
    }

    if (length(errors) > 0) {
      secretary::typewrite("Tables:")
      tables %>%
        purrr::map(~ secretary::typewrite(., tabs = 4, timepunched = FALSE))

      secretary::typewrite_warning("Some tables did not copy:")
      errors %>%
        purrr::map(~ secretary::typewrite(secretary::italicize(.), tabs = 4, timepunched = FALSE))
    } else {
      secretary::typewrite("All tables copied:")
      tables %>%
        purrr::map(~ secretary::typewrite(., tabs = 4, timepunched = FALSE))
    }
  }


#' @title
#' Add Indexes
#' @seealso
#'  \code{\link[progress]{progress_bar}}
#'  \code{\link[SqlRender]{render}}
#'  \code{\link[pg13]{send}}
#'  \code{\link[secretary]{typewrite_warning}},\code{\link[secretary]{c("typewrite", "typewrite")}},\code{\link[secretary]{italicize}}
#'  \code{\link[purrr]{map}}
#' @rdname add_indexes
#' @export
#' @importFrom progress progress_bar
#' @importFrom SqlRender render
#' @importFrom pg13 send
#' @importFrom secretary typewrite_warning typewrite italicize
#' @importFrom purrr map


add_indexes <-
  function(conn,
           schema,
           verbose = TRUE,
           render_sql = TRUE) {
    indexes <-
      c(
        "CREATE INDEX X_MRCOC_CUI1 ON @schema.MRCOC(CUI1);",
        "CREATE INDEX X_MRCOC_AUI1 ON @schema.MRCOC(AUI1);",
        "CREATE INDEX X_MRCOC_CUI2 ON @schema.MRCOC(CUI2);",
        "CREATE INDEX X_MRCOC_AUI2 ON @schema.MRCOC(AUI2);",
        "CREATE INDEX X_MRCOC_SAB ON @schema.MRCOC(SAB);",
        "CREATE INDEX X_MRCONSO_CUI ON @schema.MRCONSO(CUI);",
        "ALTER TABLE @schema.MRCONSO ADD CONSTRAINT X_MRCONSO_PK PRIMARY KEY (AUI);",
        "CREATE INDEX X_MRCONSO_SUI ON @schema.MRCONSO(SUI);",
        "CREATE INDEX X_MRCONSO_LUI ON @schema.MRCONSO(LUI);",
        "CREATE INDEX X_MRCONSO_CODE ON @schema.MRCONSO(CODE);",
        "CREATE INDEX X_MRCONSO_SAB_TTY ON @schema.MRCONSO(SAB,TTY);",
        "CREATE INDEX X_MRCONSO_SCUI ON @schema.MRCONSO(SCUI);",
        "CREATE INDEX X_MRCONSO_STR ON @schema.MRCONSO(STR);",
        "CREATE INDEX X_MRCXT_CUI ON @schema.MRCXT(CUI);",
        "CREATE INDEX X_MRCXT_AUI ON @schema.MRCXT(AUI);",
        "CREATE INDEX X_MRCXT_SAB ON @schema.MRCXT(SAB);",
        "CREATE INDEX X_MRDEF_CUI ON @schema.MRDEF(CUI);",
        "CREATE INDEX X_MRDEF_AUI ON @schema.MRDEF(AUI);",
        "ALTER TABLE @schema.MRDEF ADD CONSTRAINT X_MRDEF_PK PRIMARY KEY (ATUI);",
        "CREATE INDEX X_MRDEF_SAB ON @schema.MRDEF(SAB);",
        "CREATE INDEX X_MRHIER_CUI ON @schema.MRHIER(CUI);",
        "CREATE INDEX X_MRHIER_AUI ON @schema.MRHIER(AUI);",
        "CREATE INDEX X_MRHIER_SAB ON @schema.MRHIER(SAB);",
        "CREATE INDEX X_MRHIER_PTR ON @schema.MRHIER(PTR);",
        "CREATE INDEX X_MRHIER_PAUI ON @schema.MRHIER(PAUI);",
        "CREATE INDEX X_MRHIST_CUI ON @schema.MRHIST(CUI);",
        "CREATE INDEX X_MRHIST_SOURCEUI ON @schema.MRHIST(SOURCEUI);",
        "CREATE INDEX X_MRHIST_SAB ON @schema.MRHIST(SAB);",
        "ALTER TABLE @schema.MRRANK ADD CONSTRAINT X_MRRANK_PK PRIMARY KEY (SAB,TTY);",
        "CREATE INDEX X_MRREL_CUI1 ON @schema.MRREL(CUI1);",
        "CREATE INDEX X_MRREL_AUI1 ON @schema.MRREL(AUI1);",
        "CREATE INDEX X_MRREL_CUI2 ON @schema.MRREL(CUI2);",
        "CREATE INDEX X_MRREL_AUI2 ON @schema.MRREL(AUI2);",
        "ALTER TABLE @schema.MRREL ADD CONSTRAINT X_MRREL_PK PRIMARY KEY (RUI);",
        "CREATE INDEX X_MRREL_SAB ON @schema.MRREL(SAB);",
        "ALTER TABLE @schema.MRSAB ADD CONSTRAINT X_MRSAB_PK PRIMARY KEY (VSAB);",
        "CREATE INDEX X_MRSAB_RSAB ON @schema.MRSAB(RSAB);",
        "CREATE INDEX X_MRSAT_CUI ON @schema.MRSAT(CUI);",
        "CREATE INDEX X_MRSAT_METAUI ON @schema.MRSAT(METAUI);",
        "ALTER TABLE @schema.MRSAT ADD CONSTRAINT X_MRSAT_PK PRIMARY KEY (ATUI);",
        "CREATE INDEX X_MRSAT_SAB ON @schema.MRSAT(SAB);",
        "CREATE INDEX X_MRSAT_ATN ON @schema.MRSAT(ATN);",
        "CREATE INDEX X_MRSTY_CUI ON @schema.MRSTY(CUI);",
        "ALTER TABLE @schema.MRSTY ADD CONSTRAINT X_MRSTY_PK PRIMARY KEY (ATUI);",
        "CREATE INDEX X_MRSTY_STY ON @schema.MRSTY(STY);",
        "CREATE INDEX X_MRXNS_ENG_NSTR ON @schema.MRXNS_ENG(NSTR);",
        "CREATE INDEX X_MRXNW_ENG_NWD ON @schema.MRXNW_ENG(NWD);",
        "CREATE INDEX X_MRXW_BAQ_WD ON @schema.MRXW_BAQ(WD);",
        "CREATE INDEX X_MRXW_CZE_WD ON @schema.MRXW_CZE(WD);",
        "CREATE INDEX X_MRXW_DAN_WD ON @schema.MRXW_DAN(WD);",
        "CREATE INDEX X_MRXW_DUT_WD ON @schema.MRXW_DUT(WD);",
        "CREATE INDEX X_MRXW_ENG_WD ON @schema.MRXW_ENG(WD);",
        "CREATE INDEX X_MRXW_FIN_WD ON @schema.MRXW_FIN(WD);",
        "CREATE INDEX X_MRXW_FRE_WD ON @schema.MRXW_FRE(WD);",
        "CREATE INDEX X_MRXW_GER_WD ON @schema.MRXW_GER(WD);",
        "CREATE INDEX X_MRXW_HEB_WD ON @schema.MRXW_HEB(WD);",
        "CREATE INDEX X_MRXW_HUN_WD ON @schema.MRXW_HUN(WD);",
        "CREATE INDEX X_MRXW_ITA_WD ON @schema.MRXW_ITA(WD);",
        "CREATE INDEX X_MRXW_JPN_WD ON @schema.MRXW_JPN(WD);",
        "CREATE INDEX X_MRXW_NOR_WD ON @schema.MRXW_NOR(WD);",
        "CREATE INDEX X_MRXW_POR_WD ON @schema.MRXW_POR(WD);",
        "CREATE INDEX X_MRXW_RUS_WD ON @schema.MRXW_RUS(WD);",
        "CREATE INDEX X_MRXW_SPA_WD ON @schema.MRXW_SPA(WD);",
        "CREATE INDEX X_MRXW_SWE_WD ON @schema.MRXW_SWE(WD);",
        "CREATE INDEX X_AMBIGSUI_SUI ON @schema.AMBIGSUI(SUI);",
        "CREATE INDEX X_AMBIGLUI_LUI ON @schema.AMBIGLUI(LUI);"
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
      index <- SqlRender::render(indexes[i],
        schema = schema
      )

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


      if (length(output) == 1) {
        if (output == "Error") {
          errors <-
            c(
              errors,
              index
            )
        }
      }
    }

    if (length(errors) > 0) {
      secretary::typewrite_warning("Some indexes failed:")
      errors %>%
        purrr::map(~ secretary::typewrite(secretary::italicize(.), tabs = 4, timepunched = FALSE))
    }
  }
