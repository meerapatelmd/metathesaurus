#' @title
#' Instantiate MySQL5.5
#' @inherit run_setup description
#' @inheritParams run_setup
#' @seealso
#'  \code{\link[preQL]{query}}
#'  \code{\link[purrr]{map}},\code{\link[purrr]{map2}}
#'  \code{\link[SqlRender]{render}}
#'  \code{\link[stringr]{str_replace}},\code{\link[stringr]{str_remove}}
#'  \code{\link[RMySQL]{character(0)}}
#'  \code{\link[tibble]{as_tibble}}
#'  \code{\link[dplyr]{mutate}},\code{\link[dplyr]{select}},\code{\link[dplyr]{distinct}}
#'  \code{\link[rubix]{filter_for}}
#'  \code{\link[progress]{progress_bar}}
#' @rdname run_mysql
#' @family setup
#' @export
#' @importFrom preQL query
#' @importFrom purrr map map2
#' @importFrom SqlRender render
#' @importFrom stringr str_replace_all str_remove
#' @importFrom RMySQL dbSendQuery dbClearResult
#' @importFrom tibble as_tibble_col
#' @importFrom dplyr mutate select distinct filter
#' @importFrom progress progress_bar

run_mysql <-
        function(conn,
                 mrconso_only = FALSE,
                 omop_only = FALSE,
                 english_only = TRUE,
                 rrf_dir)

                {


                Tables <- preQL::query(conn = conn,
                                       "SHOW TABLES;")

                if (nrow(Tables)) {

                Tables %>%
                        purrr::map(~preQL::query(conn = conn,
                                                 sql_statement =
                                                         SqlRender::render("DROP TABLE IF EXISTS @tableName;",
                                                                           tableName = .)))

                }

                tables <-
                        c('AMBIGLUI',
                          'AMBIGSUI',
                          'DELETEDCUI',
                          'DELETEDLUI',
                          'DELETEDSUI',
                          'MERGEDCUI',
                          'MERGEDLUI',
                          'MRAUI',
                          'MRCOLS',
                          'MRCONSO',
                          'MRCUI',
                          'MRCXT',
                          'MRDEF',
                          'MRDOC',
                          'MRFILES',
                          'MRHIER',
                          'MRHIST',
                          'MRMAP',
                          'MRRANK',
                          'MRREL',
                          'MRSAB',
                          'MRSAT',
                          'MRSMAP',
                          'MRSTY',
                          'MRXNS_ENG',
                          'MRXNW_ENG',
                          'MRXW_BAQ',
                          'MRXW_CHI',
                          'MRXW_CZE',
                          'MRXW_DAN',
                          'MRXW_DUT',
                          'MRXW_ENG',
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
                          'MRXW_TUR')

                if (mrconso_only) {
                        tables <- "MRCONSO"
                }

                if (omop_only) {
                        tables <- c("MRCONSO", "MRHIER","MRMAP","MRSMAP", "MRSAT","MRREL")
                }

                if (english_only) {

                        tables <- tables[!(tables %in% c('MRXW_BAQ', 'MRXW_CHI', 'MRXW_CZE', 'MRXW_DAN', 'MRXW_DUT', 'MRXW_EST', 'MRXW_FIN', 'MRXW_FRE', 'MRXW_GER', 'MRXW_GRE', 'MRXW_HEB', 'MRXW_HUN', 'MRXW_ITA', 'MRXW_JPN', 'MRXW_KOR', 'MRXW_LAV', 'MRXW_NOR', 'MRXW_POL', 'MRXW_POR', 'MRXW_RUS', 'MRXW_SCR', 'MRXW_SPA', 'MRXW_SWE', 'MRXW_TUR'))]

                }


                sqls <-
                        list(
                                AMBIGLUI = 'CREATE TABLE AMBIGLUI (
                                                    LUI	varchar(10) NOT NULL,
                                                    CUI	char(8) NOT NULL
                                                ) CHARACTER SET utf8;',
                                AMBIGSUI = 'CREATE TABLE AMBIGSUI (
                                                    SUI	varchar(10) NOT NULL,
                                                    CUI	char(8) NOT NULL
                                                ) CHARACTER SET utf8;',
                                DELETEDCUI = 'CREATE TABLE DELETEDCUI (
                                                    PCUI	char(8) NOT NULL,
                                                    PSTR	text NOT NULL
                                                ) CHARACTER SET utf8;',
                                DELETEDLUI = 'CREATE TABLE DELETEDLUI (
                                                        PLUI	varchar(10) NOT NULL,
                                                        PSTR	text NOT NULL
                                                        ) CHARACTER SET utf8;',
                                DELETEDSUI = 'CREATE TABLE DELETEDSUI (
                                                   PSUI	varchar(10) NOT NULL,
                                                    LAT	char(3) NOT NULL,
                                                    PSTR	text NOT NULL
                                                ) CHARACTER SET utf8;',
                                MERGEDCUI = 'CREATE TABLE MERGEDCUI (
                                                PCUI	char(8) NOT NULL,
                                                CUI	char(8) NOT NULL
                                                        ) CHARACTER SET utf8;',
                                MERGEDLUI = 'CREATE TABLE MERGEDLUI (
                                                                 PLUI	varchar(10),
                                                                 LUI	varchar(10)
                                                         ) CHARACTER SET utf8;',
                                MRAUI = 'CREATE TABLE MRAUI (
                                    AUI1	varchar(9) NOT NULL,
                                    CUI1	char(8) NOT NULL,
                                    VER	varchar(10) NOT NULL,
                                    REL	varchar(4),
                                    RELA	varchar(100),
                                    MAPREASON	text NOT NULL,
                                    AUI2	varchar(9) NOT NULL,
                                    CUI2	char(8) NOT NULL,
                                    MAPIN	char(1) NOT NULL
                                ) CHARACTER SET utf8;',
                                MRCOLS = 'CREATE TABLE MRCOLS (
                                    COL	varchar(40),
                                    DES	varchar(200),
                                    REF	varchar(40),
                                    MIN	int unsigned,
                                    AV	numeric(5,2),
                                    MAX	int unsigned,
                                    FIL	varchar(50),
                                    DTY	varchar(40)
                                ) CHARACTER SET utf8;',
                                MRCONSO = 'CREATE TABLE MRCONSO (
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
                                    SRL	int unsigned NOT NULL,
                                    SUPPRESS	char(1) NOT NULL,
                                    CVF	int unsigned
                                ) CHARACTER SET utf8;',
                                MRCUI = 'CREATE TABLE MRCUI (
                                    CUI1	char(8) NOT NULL,
                                    VER	varchar(10) NOT NULL,
                                    REL	varchar(4) NOT NULL,
                                    RELA	varchar(100),
                                    MAPREASON	text,
                                    CUI2	char(8),
                                    MAPIN	char(1)
                                ) CHARACTER SET utf8;',
                                MRCXT = 'CREATE TABLE MRCXT (
                                    CUI	char(8),
                                    SUI	varchar(10),
                                    AUI	varchar(9),
                                    SAB	varchar(40),
                                    CODE	varchar(100),
                                    CXN	int unsigned,
                                    CXL	char(3),
                                    MRCXTRANK	int unsigned,
                                    CXS	text,
                                    CUI2	char(8),
                                    AUI2	varchar(9),
                                    HCD	varchar(100),
                                    RELA	varchar(100),
                                    XC	varchar(1),
                                    CVF	int unsigned
                                ) CHARACTER SET utf8;',
                                MRDEF = 'CREATE TABLE MRDEF (
                                    CUI	char(8) NOT NULL,
                                    AUI	varchar(9) NOT NULL,
                                    ATUI	varchar(11) NOT NULL,
                                    SATUI	varchar(50),
                                    SAB	varchar(40) NOT NULL,
                                    DEF	text NOT NULL,
                                    SUPPRESS	char(1) NOT NULL,
                                    CVF	int unsigned
                                ) CHARACTER SET utf8;',
                                MRDOC = 'CREATE TABLE MRDOC (
                                    DOCKEY	varchar(50) NOT NULL,
                                    VALUE	varchar(200),
                                    TYPE	varchar(50) NOT NULL,
                                    EXPL	text
                                ) CHARACTER SET utf8;',
                                MRFILES = 'CREATE TABLE MRFILES (
                                    FIL	varchar(50),
                                    DES	varchar(200),
                                    FMT	text,
                                    CLS	int unsigned,
                                    RWS	int unsigned,
                                    BTS	bigint
                                ) CHARACTER SET utf8;',
                                MRHIER = 'CREATE TABLE MRHIER (
                                    CUI	char(8) NOT NULL,
                                    AUI	varchar(9) NOT NULL,
                                    CXN	int unsigned NOT NULL,
                                    PAUI	varchar(10),
                                    SAB	varchar(40) NOT NULL,
                                    RELA	varchar(100),
                                    PTR	text,
                                    HCD	varchar(100),
                                    CVF	int unsigned
                                ) CHARACTER SET utf8;',
                                MRHIST = 'CREATE TABLE MRHIST (
                                    CUI	char(8),
                                    SOURCEUI	varchar(100),
                                    SAB	varchar(40),
                                    SVER	varchar(40),
                                    CHANGETYPE	text,
                                    CHANGEKEY	text,
                                    CHANGEVAL	text,
                                    REASON	text,
                                    CVF	int unsigned
                                ) CHARACTER SET utf8;',
                                MRMAP = 'CREATE TABLE MRMAP (
                                    MAPSETCUI	char(8) NOT NULL,
                                    MAPSETSAB	varchar(40) NOT NULL,
                                    MAPSUBSETID	varchar(10),
                                    MAPRANK	int unsigned,
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
                                    CVF	int unsigned
                                ) CHARACTER SET utf8;',
                                MRRANK = 'CREATE TABLE MRRANK (
                                    MRRANK_RANK	int unsigned NOT NULL,
                                    SAB	varchar(40) NOT NULL,
                                    TTY	varchar(40) NOT NULL,
                                    SUPPRESS	char(1) NOT NULL
                                ) CHARACTER SET utf8;',
                                MRREL = 'CREATE TABLE MRREL (
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
                                    CVF	int unsigned
                                ) CHARACTER SET utf8;',
                                MRSAB = 'CREATE TABLE MRSAB (
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
                                    SRL	int unsigned NOT NULL,
                                    TFR	int unsigned,
                                    CFR	int unsigned,
                                    CXTY	varchar(50),
                                    TTYL	varchar(400),
                                    ATNL	text,
                                    LAT	char(3),
                                    CENC	varchar(40) NOT NULL,
                                    CURVER	char(1) NOT NULL,
                                    SABIN	char(1) NOT NULL,
                                    SSN	text NOT NULL,
                                    SCIT	text NOT NULL
                                ) CHARACTER SET utf8;',
                                MRSAT = 'CREATE TABLE MRSAT (
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
                                    CVF	int unsigned
                                ) CHARACTER SET utf8;',
                                MRSMAP = 'CREATE TABLE MRSMAP (
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
                                    CVF	int unsigned
                                ) CHARACTER SET utf8;',
                                MRSTY = 'CREATE TABLE MRSTY (
                                    CUI	char(8) NOT NULL,
                                    TUI	char(4) NOT NULL,
                                    STN	varchar(100) NOT NULL,
                                    STY	varchar(50) NOT NULL,
                                    ATUI	varchar(11) NOT NULL,
                                    CVF	int unsigned
                                ) CHARACTER SET utf8;',
                                MRXNS_ENG = 'CREATE TABLE MRXNS_ENG (
                                    LAT	char(3) NOT NULL,
                                    NSTR	text NOT NULL,
                                    CUI	char(8) NOT NULL,
                                    LUI	varchar(10) NOT NULL,
                                    SUI	varchar(10) NOT NULL
                                ) CHARACTER SET utf8;',
                                MRXNW_ENG = 'CREATE TABLE MRXNW_ENG (
                                    LAT	char(3) NOT NULL,
                                    NWD	varchar(100) NOT NULL,
                                    CUI	char(8) NOT NULL,
                                    LUI	varchar(10) NOT NULL,
                                    SUI	varchar(10) NOT NULL
                                ) CHARACTER SET utf8;',
                                MRXW_BAQ = 'CREATE TABLE MRXW_BAQ (
                                    LAT	char(3) NOT NULL,
                                    WD	varchar(200) NOT NULL,
                                    CUI	char(8) NOT NULL,
                                    LUI	varchar(10) NOT NULL,
                                    SUI	varchar(10) NOT NULL
                                ) CHARACTER SET utf8;
                                ',
                                MRXW_CHI = 'CREATE TABLE MRXW_CHI (
                                    LAT	char(3) NOT NULL,
                                    WD	varchar(200) NOT NULL,
                                    CUI	char(8) NOT NULL,
                                    LUI	varchar(10) NOT NULL,
                                    SUI	varchar(10) NOT NULL
                                ) CHARACTER SET utf8;
                                ',
                                MRXW_CZE = 'CREATE TABLE MRXW_CZE (
                                    LAT	char(3) NOT NULL,
                                    WD	varchar(200) NOT NULL,
                                    CUI	char(8) NOT NULL,
                                    LUI	varchar(10) NOT NULL,
                                    SUI	varchar(10) NOT NULL
                                ) CHARACTER SET utf8;
                                ',
                                MRXW_DAN = 'CREATE TABLE MRXW_DAN (
                                    LAT	char(3) NOT NULL,
                                    WD	varchar(200) NOT NULL,
                                    CUI	char(8) NOT NULL,
                                    LUI	varchar(10) NOT NULL,
                                    SUI	varchar(10) NOT NULL
                                ) CHARACTER SET utf8;
                                ',
                                MRXW_DUT = 'CREATE TABLE MRXW_DUT (
                                    LAT	char(3) NOT NULL,
                                    WD	varchar(200) NOT NULL,
                                    CUI	char(8) NOT NULL,
                                    LUI	varchar(10) NOT NULL,
                                    SUI	varchar(10) NOT NULL
                                ) CHARACTER SET utf8;
                                ',
                                MRXW_ENG = 'CREATE TABLE MRXW_ENG (
                                    LAT	char(3) NOT NULL,
                                    WD	varchar(200) NOT NULL,
                                    CUI	char(8) NOT NULL,
                                    LUI	varchar(10) NOT NULL,
                                    SUI	varchar(10) NOT NULL
                                ) CHARACTER SET utf8;
                                ',
                                MRXW_EST = 'CREATE TABLE MRXW_EST (
                                    LAT	char(3) NOT NULL,
                                    WD	varchar(200) NOT NULL,
                                    CUI	char(8) NOT NULL,
                                    LUI	varchar(10) NOT NULL,
                                    SUI	varchar(10) NOT NULL
                                ) CHARACTER SET utf8;
                                ',
                                MRXW_FIN = 'CREATE TABLE MRXW_FIN (
                                    LAT	char(3) NOT NULL,
                                    WD	varchar(200) NOT NULL,
                                    CUI	char(8) NOT NULL,
                                    LUI	varchar(10) NOT NULL,
                                    SUI	varchar(10) NOT NULL
                                ) CHARACTER SET utf8;
                                ',
                                MRXW_FRE = 'CREATE TABLE MRXW_FRE (
                                    LAT	char(3) NOT NULL,
                                    WD	varchar(200) NOT NULL,
                                    CUI	char(8) NOT NULL,
                                    LUI	varchar(10) NOT NULL,
                                    SUI	varchar(10) NOT NULL
                                ) CHARACTER SET utf8;
                                ',
                                MRXW_GER = 'CREATE TABLE MRXW_GER (
                                    LAT	char(3) NOT NULL,
                                    WD	varchar(200) NOT NULL,
                                    CUI	char(8) NOT NULL,
                                    LUI	varchar(10) NOT NULL,
                                    SUI	varchar(10) NOT NULL
                                ) CHARACTER SET utf8;

                                ',
                                MRXW_GRE = 'CREATE TABLE MRXW_GRE (
                                    LAT	char(3) NOT NULL,
                                    WD	varchar(200) NOT NULL,
                                    CUI	char(8) NOT NULL,
                                    LUI	varchar(10) NOT NULL,
                                    SUI	varchar(10) NOT NULL
                                ) CHARACTER SET utf8;

                                ',
                                MRXW_HEB = 'CREATE TABLE MRXW_HEB (
                                    LAT	char(3) NOT NULL,
                                    WD	varchar(200) NOT NULL,
                                    CUI	char(8) NOT NULL,
                                    LUI	varchar(10) NOT NULL,
                                    SUI	varchar(10) NOT NULL
                                ) CHARACTER SET utf8;

                                ',
                                MRXW_HUN = 'CREATE TABLE MRXW_HUN (
                                    LAT	char(3) NOT NULL,
                                    WD	varchar(200) NOT NULL,
                                    CUI	char(8) NOT NULL,
                                    LUI	varchar(10) NOT NULL,
                                    SUI	varchar(10) NOT NULL
                                ) CHARACTER SET utf8;

                                ',
                                MRXW_ITA = 'CREATE TABLE MRXW_ITA (
                                    LAT	char(3) NOT NULL,
                                    WD	varchar(200) NOT NULL,
                                    CUI	char(8) NOT NULL,
                                    LUI	varchar(10) NOT NULL,
                                    SUI	varchar(10) NOT NULL
                                ) CHARACTER SET utf8;

                                ',
                                MRXW_JPN = 'CREATE TABLE MRXW_JPN (
                                    LAT char(3) NOT NULL,
                                    WD  varchar(500) NOT NULL,
                                    CUI char(8) NOT NULL,
                                    LUI varchar(10) NOT NULL,
                                    SUI varchar(10) NOT NULL
                                ) CHARACTER SET utf8;',
                                MRXW_KOR = 'CREATE TABLE MRXW_KOR (
                                    LAT char(3) NOT NULL,
                                    WD  varchar(500) NOT NULL,
                                    CUI char(8) NOT NULL,
                                    LUI varchar(10) NOT NULL,
                                    SUI varchar(10) NOT NULL
                                ) CHARACTER SET utf8;

                                ',
                                MRXW_LAV = 'CREATE TABLE MRXW_LAV (
                                    LAT char(3) NOT NULL,
                                    WD  varchar(200) NOT NULL,
                                    CUI char(8) NOT NULL,
                                    LUI varchar(10) NOT NULL,
                                    SUI varchar(10) NOT NULL
                                ) CHARACTER SET utf8;

                                ',
                                MRXW_NOR = 'CREATE TABLE MRXW_NOR (
                                    LAT	char(3) NOT NULL,
                                    WD	varchar(200) NOT NULL,
                                    CUI	char(8) NOT NULL,
                                    LUI	varchar(10) NOT NULL,
                                    SUI	varchar(10) NOT NULL
                                ) CHARACTER SET utf8;

                                ',
                                MRXW_POL = 'CREATE TABLE MRXW_POL (
                                    LAT	char(3) NOT NULL,
                                    WD	varchar(200) NOT NULL,
                                    CUI	char(8) NOT NULL,
                                    LUI	varchar(10) NOT NULL,
                                    SUI	varchar(10) NOT NULL
                                ) CHARACTER SET utf8;

                                ',
                                MRXW_POR = 'CREATE TABLE MRXW_POR (
                                    LAT	char(3) NOT NULL,
                                    WD	varchar(200) NOT NULL,
                                    CUI	char(8) NOT NULL,
                                    LUI	varchar(10) NOT NULL,
                                    SUI	varchar(10) NOT NULL
                                ) CHARACTER SET utf8;

                                ',
                                MRXW_RUS = 'CREATE TABLE MRXW_RUS (
                                    LAT char(3) NOT NULL,
                                    WD  varchar(200) NOT NULL,
                                    CUI char(8) NOT NULL,
                                    LUI varchar(10) NOT NULL,
                                    SUI varchar(10) NOT NULL
                                ) CHARACTER SET utf8;

                                ',
                                MRXW_SCR = 'CREATE TABLE MRXW_SCR (
                                    LAT char(3) NOT NULL,
                                    WD  varchar(200) NOT NULL,
                                    CUI char(8) NOT NULL,
                                    LUI varchar(10) NOT NULL,
                                    SUI varchar(10) NOT NULL
                                ) CHARACTER SET utf8;

                                ',
                                MRXW_SPA = 'CREATE TABLE MRXW_SPA (
                                    LAT	char(3) NOT NULL,
                                    WD	varchar(200) NOT NULL,
                                    CUI	char(8) NOT NULL,
                                    LUI	varchar(10) NOT NULL,
                                    SUI	varchar(10) NOT NULL
                                ) CHARACTER SET utf8;

                                ',
                                MRXW_SWE = 'CREATE TABLE MRXW_SWE (
                                    LAT	char(3) NOT NULL,
                                    WD	varchar(200) NOT NULL,
                                    CUI	char(8) NOT NULL,
                                    LUI	varchar(10) NOT NULL,
                                    SUI	varchar(10) NOT NULL
                                ) CHARACTER SET utf8;

                                ',
                                MRXW_TUR = 'CREATE TABLE MRXW_TUR (
                                    LAT	char(3) NOT NULL,
                                    WD	varchar(200) NOT NULL,
                                    CUI	char(8) NOT NULL,
                                    LUI	varchar(10) NOT NULL,
                                    SUI	varchar(10) NOT NULL
                                ) CHARACTER SET utf8;
                                '
                        ) %>%
                        purrr::map(stringr::str_replace_all,
                                   "[\r\n\t]", " ")

                sqls <- sqls[tables]

                errors <- vector()
                for (i in 1:length(sqls)) {

                        sql <- sqls[[i]]

                        resultset <-
                                RMySQL::dbSendQuery(conn = conn,
                                                    statement = sql)

                        if (class(resultset) != "MySQLResult") {

                                errors <-
                                        c(errors,
                                          sql)
                        }

                        RMySQL::dbClearResult(resultset)

                }

                # Load data
                Tables <- preQL::query(conn = conn,
                                       "SHOW TABLES;") %>%
                                                unlist()

                rrfFileNames <- paste0(Tables, ".RRF")
                rrfFilePaths <-
                        list.files(path = rrf_dir,
                                   recursive = TRUE,
                                   pattern = "[.]RRF$",
                                   full.names = TRUE) %>%
                        tibble::as_tibble_col("filePaths") %>%
                        dplyr::mutate(baseNames = basename(filePaths)) %>%
                        dplyr::filter(baseNames %in% rrfFileNames) %>%
                        dplyr::select(filePaths) %>%
                        dplyr::distinct() %>%
                        unlist()
                rrfFileTables <- stringr::str_remove(basename(rrfFilePaths),
                                                     "[.]{1}RRF")

                sqls <-
                rrfFileTables %>%
                        purrr::map2(rrfFilePaths,
                                    function(x,y)
                                            SqlRender::render("load data local infile '@filePath' into table @tableName fields terminated by '|' ESCAPED BY '' lines terminated by '\n';",
                                                              filePath = y,
                                                              tableName = x)
                                            )

                pb <- progress::progress_bar$new(
                        format = "[:bar] :elapsedfull :current/:total (:percent)",
                        total = length(sqls),
                        width = 80,
                        clear = FALSE)

                pb$tick(0)
                Sys.sleep(0.2)

                for (i in 1:length(sqls)) {

                        sql <- sqls[[i]]

                        resultset <-
                                RMySQL::dbSendQuery(conn = conn,
                                                    statement = sql)

                        if (class(resultset) != "MySQLResult") {

                                errors <-
                                        c(errors,
                                          sql)
                        }

                        RMySQL::dbClearResult(resultset)


                        pb$tick()
                        Sys.sleep(0.2)

                }


                indexes <-
                "
                CREATE INDEX X_MRCONSO_CUI ON MRCONSO(CUI);

                ALTER TABLE MRCONSO ADD CONSTRAINT X_MRCONSO_PK  PRIMARY KEY BTREE (AUI);

                CREATE INDEX X_MRCONSO_SUI ON MRCONSO(SUI);

                CREATE INDEX X_MRCONSO_LUI ON MRCONSO(LUI);

                CREATE INDEX X_MRCONSO_CODE ON MRCONSO(CODE);

                CREATE INDEX X_MRCONSO_SAB_TTY ON MRCONSO(SAB,TTY);

                CREATE INDEX X_MRCONSO_SCUI ON MRCONSO(SCUI);

                CREATE INDEX X_MRCONSO_SDUI ON MRCONSO(SDUI);

                CREATE INDEX X_MRCONSO_STR ON MRCONSO(STR(255));

                CREATE INDEX X_MRCXT_CUI ON MRCXT(CUI);

                CREATE INDEX X_MRCXT_AUI ON MRCXT(AUI);

                CREATE INDEX X_MRCXT_SAB ON MRCXT(SAB);

                CREATE INDEX X_MRDEF_CUI ON MRDEF(CUI);

                CREATE INDEX X_MRDEF_AUI ON MRDEF(AUI);

                ALTER TABLE MRDEF ADD CONSTRAINT X_MRDEF_PK  PRIMARY KEY BTREE (ATUI);

                CREATE INDEX X_MRDEF_SAB ON MRDEF(SAB);

                CREATE INDEX X_MRHIER_CUI ON MRHIER(CUI);

                CREATE INDEX X_MRHIER_AUI ON MRHIER(AUI);

                CREATE INDEX X_MRHIER_SAB ON MRHIER(SAB);

                CREATE INDEX X_MRHIER_PTR ON MRHIER(PTR(255));

                CREATE INDEX X_MRHIER_PAUI ON MRHIER(PAUI);

                CREATE INDEX X_MRHIST_CUI ON MRHIST(CUI);

                CREATE INDEX X_MRHIST_SOURCEUI ON MRHIST(SOURCEUI);

                CREATE INDEX X_MRHIST_SAB ON MRHIST(SAB);

                ALTER TABLE MRRANK ADD CONSTRAINT X_MRRANK_PK  PRIMARY KEY BTREE (SAB,TTY);

                CREATE INDEX X_MRREL_CUI1 ON MRREL(CUI1);

                CREATE INDEX X_MRREL_AUI1 ON MRREL(AUI1);

                CREATE INDEX X_MRREL_CUI2 ON MRREL(CUI2);

                CREATE INDEX X_MRREL_AUI2 ON MRREL(AUI2);

                ALTER TABLE MRREL ADD CONSTRAINT X_MRREL_PK  PRIMARY KEY BTREE (RUI);

                CREATE INDEX X_MRREL_SAB ON MRREL(SAB);

                ALTER TABLE MRSAB ADD CONSTRAINT X_MRSAB_PK  PRIMARY KEY BTREE (VSAB);

                CREATE INDEX X_MRSAB_RSAB ON MRSAB(RSAB);

                CREATE INDEX X_MRSAT_CUI ON MRSAT(CUI);

                CREATE INDEX X_MRSAT_METAUI ON MRSAT(METAUI);

                ALTER TABLE MRSAT ADD CONSTRAINT X_MRSAT_PK  PRIMARY KEY BTREE (ATUI);

                CREATE INDEX X_MRSAT_SAB ON MRSAT(SAB);

                CREATE INDEX X_MRSAT_ATN ON MRSAT(ATN);

                CREATE INDEX X_MRSTY_CUI ON MRSTY(CUI);

                ALTER TABLE MRSTY ADD CONSTRAINT X_MRSTY_PK  PRIMARY KEY BTREE (ATUI);

                CREATE INDEX X_MRSTY_STY ON MRSTY(STY);

                CREATE INDEX X_MRXNS_ENG_NSTR ON MRXNS_ENG(NSTR(255));

                CREATE INDEX X_MRXNW_ENG_NWD ON MRXNW_ENG(NWD);

                CREATE INDEX X_MRXW_BAQ_WD ON MRXW_BAQ(WD);

                CREATE INDEX X_MRXW_CHI_WD ON MRXW_CHI(WD);

                CREATE INDEX X_MRXW_CZE_WD ON MRXW_CZE(WD);

                CREATE INDEX X_MRXW_DAN_WD ON MRXW_DAN(WD);

                CREATE INDEX X_MRXW_DUT_WD ON MRXW_DUT(WD);

                CREATE INDEX X_MRXW_ENG_WD ON MRXW_ENG(WD);

                CREATE INDEX X_MRXW_EST_WD ON MRXW_EST(WD);

                CREATE INDEX X_MRXW_FIN_WD ON MRXW_FIN(WD);

                CREATE INDEX X_MRXW_FRE_WD ON MRXW_FRE(WD);

                CREATE INDEX X_MRXW_GER_WD ON MRXW_GER(WD);

                CREATE INDEX X_MRXW_GRE_WD ON MRXW_GRE(WD);

                CREATE INDEX X_MRXW_HEB_WD ON MRXW_HEB(WD);

                CREATE INDEX X_MRXW_HUN_WD ON MRXW_HUN(WD);

                CREATE INDEX X_MRXW_ITA_WD ON MRXW_ITA(WD);

                CREATE INDEX X_MRXW_JPN_WD ON MRXW_JPN(WD(255));

                CREATE INDEX X_MRXW_KOR_WD ON MRXW_KOR(WD(255));

                CREATE INDEX X_MRXW_LAV_WD ON MRXW_LAV(WD);

                CREATE INDEX X_MRXW_NOR_WD ON MRXW_NOR(WD);

                CREATE INDEX X_MRXW_POL_WD ON MRXW_POL(WD);

                CREATE INDEX X_MRXW_POR_WD ON MRXW_POR(WD);

                CREATE INDEX X_MRXW_RUS_WD ON MRXW_RUS(WD);

                CREATE INDEX X_MRXW_SCR_WD ON MRXW_SCR(WD);

                CREATE INDEX X_MRXW_SPA_WD ON MRXW_SPA(WD);

                CREATE INDEX X_MRXW_SWE_WD ON MRXW_SWE(WD);

                CREATE INDEX X_MRXW_TUR_WD ON MRXW_TUR(WD);

                CREATE INDEX X_AMBIGSUI_SUI ON AMBIGSUI(SUI);

                CREATE INDEX X_AMBIGLUI_LUI ON AMBIGLUI(LUI);

                CREATE INDEX X_MRAUI_CUI2 ON MRAUI(CUI2);

                CREATE INDEX X_MRCUI_CUI2 ON MRCUI(CUI2);

                CREATE INDEX X_MRMAP_MAPSETCUI ON MRMAP(MAPSETCUI);" %>%
                       sqlsplit(split = "[;]", type = "after") %>%
                        unlist() %>%
                        trimws()

                pb <- progress::progress_bar$new(
                        format = "[:bar] :elapsedfull :current/:total (:percent)",
                        total = length(indexes),
                        width = 80,
                        clear = FALSE)


                pb$tick(0)
                Sys.sleep(0.2)


                for (i in 1:length(indexes)) {

                        resultset <-
                                tryCatch(
                                        RMySQL::dbSendQuery(conn = conn,
                                                            statement = indexes[i]),
                                        error = function(e) "Error"
                                )

                        if (class(resultset) != "MySQLResult") {

                                errors <-
                                        c(errors,
                                          indexes[i])
                        }  else {

                                RMySQL::dbClearResult(resultset)
                        }




                        pb$tick()
                        Sys.sleep(0.2)


                }

                if (length(errors)) {

                        cat(errors,
                            sep = "\n")

                }

        }
