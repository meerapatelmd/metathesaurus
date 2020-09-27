#' DDL META Tables
#' @param path Path to the unpacked RRF files
#' @import preQL
#' @importFrom DatabaseConnector dbExecute
#' @export

#
# mysql_ddl(username = Sys.getenv("umls_username"),
#           password = Sys.getenv("umls_password"))
#
#

mysql_ddl <-
        function(dbname = "umls",
                 username,
                 password,
                 mrconso_only = FALSE,
                 omop_only = FALSE,
                 english_only = TRUE)

                {

                #version = c("full_eng", "full", "omop", "mrconso"))

                conn <-
                preQL::connectMySQL5.5(dbname = dbname,
                                       username = username,
                                       password = password)


                # tables <- list(
                #         MRCOLS = 'DROP TABLE IF EXISTS MRCOLS;
                #                 CREATE TABLE MRCOLS (
                #                         COL	varchar(40),
                #                         DES	varchar(200),
                #                         REF	varchar(40),
                #                         MIN	int unsigned,
                #                         AV	numeric(5,2),
                #                         MAX	int unsigned,
                #                         FIL	varchar(50),
                #                         DTY	varchar(40)
                #                 ) CHARACTER SET utf8;',
                #         MRCONSO = 'DROP TABLE IF EXISTS MRCONSO;
                #                 CREATE TABLE MRCONSO (
                #                         CUI	char(8) NOT NULL,
                #                         LAT	char(3) NOT NULL,
                #                         TS	char(1) NOT NULL,
                #                         LUI	varchar(10) NOT NULL,
                #                         STT	varchar(3) NOT NULL,
                #                         SUI	varchar(10) NOT NULL,
                #                         ISPREF	char(1) NOT NULL,
                #                         AUI	varchar(9) NOT NULL,
                #                         SAUI	varchar(50),
                #                         SCUI	varchar(100),
                #                         SDUI	varchar(100),
                #                         SAB	varchar(40) NOT NULL,
                #                         TTY	varchar(40) NOT NULL,
                #                         CODE	varchar(100) NOT NULL,
                #                         STR	text NOT NULL,
                #                         SRL	int unsigned NOT NULL,
                #                         SUPPRESS	char(1) NOT NULL,
                #                         CVF	int unsigned
                #                 ) CHARACTER SET utf8;',
                #         MRCUI = 'DROP TABLE IF EXISTS MRCUI;
                #                 CREATE TABLE MRCUI (
                #                         CUI1	char(8) NOT NULL,
                #                         VER	varchar(10) NOT NULL,
                #                         REL	varchar(4) NOT NULL,
                #                         RELA	varchar(100),
                #                         MAPREASON	text,
                #                         CUI2	char(8),
                #                         MAPIN	char(1)
                #                 ) CHARACTER SET utf8;',
                #         MRCXT = 'DROP TABLE IF EXISTS MRCXT;
                #                 CREATE TABLE MRCXT (
                #                         CUI	char(8),
                #                         SUI	varchar(10),
                #                         AUI	varchar(9),
                #                         SAB	varchar(40),
                #                         CODE	varchar(100),
                #                         CXN	int unsigned,
                #                         CXL	char(3),
                #                         MRCXTRANK	int unsigned,
                #                         CXS	text,
                #                         CUI2	char(8),
                #                         AUI2	varchar(9),
                #                         HCD	varchar(100),
                #                         RELA	varchar(100),
                #                         XC	varchar(1),
                #                         CVF	int unsigned
                #                 ) CHARACTER SET utf8;',
                #         MRDEF = 'DROP TABLE IF EXISTS MRDEF;
                #                 CREATE TABLE MRDEF (
                #                         CUI	char(8) NOT NULL,
                #                         AUI	varchar(9) NOT NULL,
                #                         ATUI	varchar(11) NOT NULL,
                #                         SATUI	varchar(50),
                #                         SAB	varchar(40) NOT NULL,
                #                         DEF	text NOT NULL,
                #                         SUPPRESS	char(1) NOT NULL,
                #                         CVF	int unsigned
                #                 ) CHARACTER SET utf8;',
                #         MRDOC = 'DROP TABLE IF EXISTS MRDOC;
                #                 CREATE TABLE MRDOC (
                #                         DOCKEY	varchar(50) NOT NULL,
                #                         VALUE	varchar(200),
                #                         TYPE	varchar(50) NOT NULL,
                #                         EXPL	text
                #                 ) CHARACTER SET utf8;',
                #         MRFILES = 'DROP TABLE IF EXISTS MRFILES;
                #                 CREATE TABLE MRFILES (
                #                         FIL	varchar(50),
                #                         DES	varchar(200),
                #                         FMT	text,
                #                         CLS	int unsigned,
                #                         RWS	int unsigned,
                #                         BTS	bigint
                #                 ) CHARACTER SET utf8;',
                #         MRHIER = 'DROP TABLE IF EXISTS MRHIER;
                #                 CREATE TABLE MRHIER (
                #                         CUI	char(8) NOT NULL,
                #                         AUI	varchar(9) NOT NULL,
                #                         CXN	int unsigned NOT NULL,
                #                         PAUI	varchar(10),
                #                         SAB	varchar(40) NOT NULL,
                #                         RELA	varchar(100),
                #                         PTR	text,
                #                         HCD	varchar(100),
                #                         CVF	int unsigned
                #                 ) CHARACTER SET utf8;',
                #         MRHIST = 'DROP TABLE IF EXISTS MRHIST;
                #                 CREATE TABLE MRHIST (
                #                         CUI	char(8),
                #                         SOURCEUI	varchar(100),
                #                         SAB	varchar(40),
                #                         SVER	varchar(40),
                #                         CHANGETYPE	text,
                #                         CHANGEKEY	text,
                #                         CHANGEVAL	text,
                #                         REASON	text,
                #                         CVF	int unsigned
                #                 ) CHARACTER SET utf8;',
                #         MRMAP = 'DROP TABLE IF EXISTS MRMAP;
                #                         CREATE TABLE MRMAP (
                #                                 MAPSETCUI	char(8) NOT NULL,
                #                                 MAPSETSAB	varchar(40) NOT NULL,
                #                                 MAPSUBSETID	varchar(10),
                #                                 MAPRANK	int unsigned,
                #                                 MAPID	varchar(50) NOT NULL,
                #                                 MAPSID	varchar(50),
                #                                 FROMID	varchar(50) NOT NULL,
                #                                 FROMSID	varchar(50),
                #                                 FROMEXPR	text NOT NULL,
                #                                 FROMTYPE	varchar(50) NOT NULL,
                #                                 FROMRULE	text,
                #                                 FROMRES	text,
                #                                 REL	varchar(4) NOT NULL,
                #                                 RELA	varchar(100),
                #                                 TOID	varchar(50),
                #                                 TOSID	varchar(50),
                #                                 TOEXPR	text,
                #                                 TOTYPE	varchar(50),
                #                                 TORULE	text,
                #                                 TORES	text,
                #                                 MAPRULE	text,
                #                                 MAPRES	text,
                #                                 MAPTYPE	varchar(50),
                #                                 MAPATN	varchar(100),
                #                                 MAPATV	text,
                #                                 CVF	int unsigned
                #                         ) CHARACTER SET utf8;',
                #         MRRANK = 'DROP TABLE IF EXISTS MRRANK;
                #                         CREATE TABLE MRRANK (
                #                                 MRRANK_RANK	int unsigned NOT NULL,
                #                                 SAB	varchar(40) NOT NULL,
                #                                 TTY	varchar(40) NOT NULL,
                #                                 SUPPRESS	char(1) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRREL = 'DROP TABLE IF EXISTS MRREL;
                #                 CREATE TABLE MRREL (
                #                         CUI1	char(8) NOT NULL,
                #                         AUI1	varchar(9),
                #                         STYPE1	varchar(50) NOT NULL,
                #                         REL	varchar(4) NOT NULL,
                #                         CUI2	char(8) NOT NULL,
                #                         AUI2	varchar(9),
                #                         STYPE2	varchar(50) NOT NULL,
                #                         RELA	varchar(100),
                #                         RUI	varchar(10) NOT NULL,
                #                         SRUI	varchar(50),
                #                         SAB	varchar(40) NOT NULL,
                #                         SL	varchar(40) NOT NULL,
                #                         RG	varchar(10),
                #                         DIR	varchar(1),
                #                         SUPPRESS	char(1) NOT NULL,
                #                         CVF	int unsigned
                #                 ) CHARACTER SET utf8;',
                #         MRSAB = 'DROP TABLE IF EXISTS MRSAB;
                #                 CREATE TABLE MRSAB (
                #                         VCUI	char(8),
                #                         RCUI	char(8),
                #                         VSAB	varchar(40) NOT NULL,
                #                         RSAB	varchar(40) NOT NULL,
                #                         SON	text NOT NULL,
                #                         SF	varchar(40) NOT NULL,
                #                         SVER	varchar(40),
                #                         VSTART	char(8),
                #                         VEND	char(8),
                #                         IMETA	varchar(10) NOT NULL,
                #                         RMETA	varchar(10),
                #                         SLC	text,
                #                         SCC	text,
                #                         SRL	int unsigned NOT NULL,
                #                         TFR	int unsigned,
                #                         CFR	int unsigned,
                #                         CXTY	varchar(50),
                #                         TTYL	varchar(400),
                #                         ATNL	text,
                #                         LAT	char(3),
                #                         CENC	varchar(40) NOT NULL,
                #                         CURVER	char(1) NOT NULL,
                #                         SABIN	char(1) NOT NULL,
                #                         SSN	text NOT NULL,
                #                         SCIT	text NOT NULL
                #                 ) CHARACTER SET utf8;',
                #         MRSAT = 'DROP TABLE IF EXISTS MRSAT;
                #                 CREATE TABLE MRSAT (
                #                         CUI	char(8) NOT NULL,
                #                         LUI	varchar(10),
                #                         SUI	varchar(10),
                #                         METAUI	varchar(100),
                #                         STYPE	varchar(50) NOT NULL,
                #                         CODE	varchar(100),
                #                         ATUI	varchar(11) NOT NULL,
                #                         SATUI	varchar(50),
                #                         ATN	varchar(100) NOT NULL,
                #                         SAB	varchar(40) NOT NULL,
                #                         ATV	text,
                #                         SUPPRESS	char(1) NOT NULL,
                #                         CVF	int unsigned
                #                 ) CHARACTER SET utf8;',
                #         MRSMAP = 'DROP TABLE IF EXISTS MRSMAP;
                #                         CREATE TABLE MRSMAP (
                #                                 MAPSETCUI	char(8) NOT NULL,
                #                                 MAPSETSAB	varchar(40) NOT NULL,
                #                                 MAPID	varchar(50) NOT NULL,
                #                                 MAPSID	varchar(50),
                #                                 FROMEXPR	text NOT NULL,
                #                                 FROMTYPE	varchar(50) NOT NULL,
                #                                 REL	varchar(4) NOT NULL,
                #                                 RELA	varchar(100),
                #                                 TOEXPR	text,
                #                                 TOTYPE	varchar(50),
                #                                 CVF	int unsigned
                #                         ) CHARACTER SET utf8;',
                #         MRSTY = 'DROP TABLE IF EXISTS MRSTY;
                #                         CREATE TABLE MRSTY (
                #                                 CUI	char(8) NOT NULL,
                #                                 TUI	char(4) NOT NULL,
                #                                 STN	varchar(100) NOT NULL,
                #                                 STY	varchar(50) NOT NULL,
                #                                 ATUI	varchar(11) NOT NULL,
                #                                 CVF	int unsigned
                #                         ) CHARACTER SET utf8;',
                #         MRXNS_ENG = 'DROP TABLE IF EXISTS MRXNS_ENG;
                #                         CREATE TABLE MRXNS_ENG (
                #                                 LAT	char(3) NOT NULL,
                #                                 NSTR	text NOT NULL,
                #                                 CUI	char(8) NOT NULL,
                #                                 LUI	varchar(10) NOT NULL,
                #                                 SUI	varchar(10) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRXNW_ENG = 'DROP TABLE IF EXISTS MRXNW_ENG;
                #                         CREATE TABLE MRXNW_ENG (
                #                                 LAT	char(3) NOT NULL,
                #                                 NWD	varchar(100) NOT NULL,
                #                                 CUI	char(8) NOT NULL,
                #                                 LUI	varchar(10) NOT NULL,
                #                                 SUI	varchar(10) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRAUI = 'DROP TABLE IF EXISTS MRAUI;
                #                         CREATE TABLE MRAUI (
                #                                 AUI1	varchar(9) NOT NULL,
                #                                 CUI1	char(8) NOT NULL,
                #                                 VER	varchar(10) NOT NULL,
                #                                 REL	varchar(4),
                #                                 RELA	varchar(100),
                #                                 MAPREASON	text NOT NULL,
                #                                 AUI2	varchar(9) NOT NULL,
                #                                 CUI2	char(8) NOT NULL,
                #                                 MAPIN	char(1) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRXW_BAQ = 'DROP TABLE IF EXISTS MRXW_BAQ;
                #                         CREATE TABLE MRXW_BAQ (
                #                                 LAT	char(3) NOT NULL,
                #                                 WD	varchar(200) NOT NULL,
                #                                 CUI	char(8) NOT NULL,
                #                                 LUI	varchar(10) NOT NULL,
                #                                 SUI	varchar(10) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRXW_CHI = 'DROP TABLE IF EXISTS MRXW_CHI;
                #                         CREATE TABLE MRXW_CHI (
                #                                 LAT	char(3) NOT NULL,
                #                                 WD	varchar(200) NOT NULL,
                #                                 CUI	char(8) NOT NULL,
                #                                 LUI	varchar(10) NOT NULL,
                #                                 SUI	varchar(10) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRXW_CZE = 'DROP TABLE IF EXISTS MRXW_CZE;
                #                         CREATE TABLE MRXW_CZE (
                #                                 LAT	char(3) NOT NULL,
                #                                 WD	varchar(200) NOT NULL,
                #                                 CUI	char(8) NOT NULL,
                #                                 LUI	varchar(10) NOT NULL,
                #                                 SUI	varchar(10) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRXW_DAN = 'DROP TABLE IF EXISTS MRXW_DAN;
                #                         CREATE TABLE MRXW_DAN (
                #                                 LAT	char(3) NOT NULL,
                #                                 WD	varchar(200) NOT NULL,
                #                                 CUI	char(8) NOT NULL,
                #                                 LUI	varchar(10) NOT NULL,
                #                                 SUI	varchar(10) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRXW_DUT = 'DROP TABLE IF EXISTS MRXW_DUT;
                #                         CREATE TABLE MRXW_DUT (
                #                                 LAT	char(3) NOT NULL,
                #                                 WD	varchar(200) NOT NULL,
                #                                 CUI	char(8) NOT NULL,
                #                                 LUI	varchar(10) NOT NULL,
                #                                 SUI	varchar(10) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRXW_ENG = 'DROP TABLE IF EXISTS MRXW_ENG;
                #                         CREATE TABLE MRXW_ENG (
                #                                 LAT	char(3) NOT NULL,
                #                                 WD	varchar(200) NOT NULL,
                #                                 CUI	char(8) NOT NULL,
                #                                 LUI	varchar(10) NOT NULL,
                #                                 SUI	varchar(10) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRXW_EST = 'DROP TABLE IF EXISTS MRXW_EST;
                #                         CREATE TABLE MRXW_EST (
                #                                 LAT	char(3) NOT NULL,
                #                                 WD	varchar(200) NOT NULL,
                #                                 CUI	char(8) NOT NULL,
                #                                 LUI	varchar(10) NOT NULL,
                #                                 SUI	varchar(10) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRXW_FIN = 'DROP TABLE IF EXISTS MRXW_FIN;
                #                         CREATE TABLE MRXW_FIN (
                #                                 LAT	char(3) NOT NULL,
                #                                 WD	varchar(200) NOT NULL,
                #                                 CUI	char(8) NOT NULL,
                #                                 LUI	varchar(10) NOT NULL,
                #                                 SUI	varchar(10) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRXW_FRE = 'DROP TABLE IF EXISTS MRXW_FRE;
                #                         CREATE TABLE MRXW_FRE (
                #                                 LAT	char(3) NOT NULL,
                #                                 WD	varchar(200) NOT NULL,
                #                                 CUI	char(8) NOT NULL,
                #                                 LUI	varchar(10) NOT NULL,
                #                                 SUI	varchar(10) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRXW_GER = 'DROP TABLE IF EXISTS MRXW_GER;
                #                         CREATE TABLE MRXW_GER (
                #                                 LAT	char(3) NOT NULL,
                #                                 WD	varchar(200) NOT NULL,
                #                                 CUI	char(8) NOT NULL,
                #                                 LUI	varchar(10) NOT NULL,
                #                                 SUI	varchar(10) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRXW_GRE = 'DROP TABLE IF EXISTS MRXW_GRE;
                #                         CREATE TABLE MRXW_GRE (
                #                                 LAT	char(3) NOT NULL,
                #                                 WD	varchar(200) NOT NULL,
                #                                 CUI	char(8) NOT NULL,
                #                                 LUI	varchar(10) NOT NULL,
                #                                 SUI	varchar(10) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRXW_HEB = 'DROP TABLE IF EXISTS MRXW_HEB;
                #                         CREATE TABLE MRXW_HEB (
                #                                 LAT	char(3) NOT NULL,
                #                                 WD	varchar(200) NOT NULL,
                #                                 CUI	char(8) NOT NULL,
                #                                 LUI	varchar(10) NOT NULL,
                #                                 SUI	varchar(10) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRXW_HUN = 'DROP TABLE IF EXISTS MRXW_HUN;
                #                         CREATE TABLE MRXW_HUN (
                #                                 LAT	char(3) NOT NULL,
                #                                 WD	varchar(200) NOT NULL,
                #                                 CUI	char(8) NOT NULL,
                #                                 LUI	varchar(10) NOT NULL,
                #                                 SUI	varchar(10) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRXW_ITA = 'DROP TABLE IF EXISTS MRXW_ITA;
                #                         CREATE TABLE MRXW_ITA (
                #                                 LAT	char(3) NOT NULL,
                #                                 WD	varchar(200) NOT NULL,
                #                                 CUI	char(8) NOT NULL,
                #                                 LUI	varchar(10) NOT NULL,
                #                                 SUI	varchar(10) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRXW_JPN = 'DROP TABLE IF EXISTS MRXW_JPN;
                #                         CREATE TABLE MRXW_JPN (
                #                                 LAT char(3) NOT NULL,
                #                                 WD  varchar(500) NOT NULL,
                #                                 CUI char(8) NOT NULL,
                #                                 LUI varchar(10) NOT NULL,
                #                                 SUI varchar(10) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRXW_KOR = 'DROP TABLE IF EXISTS MRXW_KOR;
                #                         CREATE TABLE MRXW_KOR (
                #                                 LAT char(3) NOT NULL,
                #                                 WD  varchar(500) NOT NULL,
                #                                 CUI char(8) NOT NULL,
                #                                 LUI varchar(10) NOT NULL,
                #                                 SUI varchar(10) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRXW_LAV = 'DROP TABLE IF EXISTS MRXW_LAV;
                #                         CREATE TABLE MRXW_LAV (
                #                                 LAT char(3) NOT NULL,
                #                                 WD  varchar(200) NOT NULL,
                #                                 CUI char(8) NOT NULL,
                #                                 LUI varchar(10) NOT NULL,
                #                                 SUI varchar(10) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRXW_NOR = 'DROP TABLE IF EXISTS MRXW_NOR;
                #                         CREATE TABLE MRXW_NOR (
                #                                 LAT	char(3) NOT NULL,
                #                                 WD	varchar(200) NOT NULL,
                #                                 CUI	char(8) NOT NULL,
                #                                 LUI	varchar(10) NOT NULL,
                #                                 SUI	varchar(10) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRXW_POL = 'DROP TABLE IF EXISTS MRXW_POL;
                #                         CREATE TABLE MRXW_POL (
                #                                 LAT	char(3) NOT NULL,
                #                                 WD	varchar(200) NOT NULL,
                #                                 CUI	char(8) NOT NULL,
                #                                 LUI	varchar(10) NOT NULL,
                #                                 SUI	varchar(10) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRXW_POR = 'DROP TABLE IF EXISTS MRXW_POR;
                #                         CREATE TABLE MRXW_POR (
                #                                 LAT	char(3) NOT NULL,
                #                                 WD	varchar(200) NOT NULL,
                #                                 CUI	char(8) NOT NULL,
                #                                 LUI	varchar(10) NOT NULL,
                #                                 SUI	varchar(10) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRXW_RUS = 'DROP TABLE IF EXISTS MRXW_RUS;
                #                         CREATE TABLE MRXW_RUS (
                #                                 LAT char(3) NOT NULL,
                #                                 WD  varchar(200) NOT NULL,
                #                                 CUI char(8) NOT NULL,
                #                                 LUI varchar(10) NOT NULL,
                #                                 SUI varchar(10) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRXW_SCR = 'DROP TABLE IF EXISTS MRXW_SCR;
                #                         CREATE TABLE MRXW_SCR (
                #                                 LAT char(3) NOT NULL,
                #                                 WD  varchar(200) NOT NULL,
                #                                 CUI char(8) NOT NULL,
                #                                 LUI varchar(10) NOT NULL,
                #                                 SUI varchar(10) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRXW_SPA = 'DROP TABLE IF EXISTS MRXW_SPA;
                #                         CREATE TABLE MRXW_SPA (
                #                                 LAT	char(3) NOT NULL,
                #                                 WD	varchar(200) NOT NULL,
                #                                 CUI	char(8) NOT NULL,
                #                                 LUI	varchar(10) NOT NULL,
                #                                 SUI	varchar(10) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRXW_SWE = 'DROP TABLE IF EXISTS MRXW_SWE;
                #                         CREATE TABLE MRXW_SWE (
                #                                 LAT	char(3) NOT NULL,
                #                                 WD	varchar(200) NOT NULL,
                #                                 CUI	char(8) NOT NULL,
                #                                 LUI	varchar(10) NOT NULL,
                #                                 SUI	varchar(10) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MRXW_TUR = 'DROP TABLE IF EXISTS MRXW_TUR;
                #                         CREATE TABLE MRXW_TUR (
                #                                 LAT	char(3) NOT NULL,
                #                                 WD	varchar(200) NOT NULL,
                #                                 CUI	char(8) NOT NULL,
                #                                 LUI	varchar(10) NOT NULL,
                #                                 SUI	varchar(10) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         AMBIGSUI = 'DROP TABLE IF EXISTS AMBIGSUI;
                #                         CREATE TABLE AMBIGSUI (
                #                                 SUI	varchar(10) NOT NULL,
                #                                 CUI	char(8) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         AMBIGLUI = 'DROP TABLE IF EXISTS AMBIGLUI;
                #                         CREATE TABLE AMBIGLUI (
                #                                 LUI	varchar(10) NOT NULL,
                #                                 CUI	char(8) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         DELETEDCUI = 'DROP TABLE IF EXISTS DELETEDCUI;
                #                         CREATE TABLE DELETEDCUI (
                #                                 PCUI	char(8) NOT NULL,
                #                                 PSTR	text NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         DELETEDLUI = 'DROP TABLE IF EXISTS DELETEDLUI;
                #                         CREATE TABLE DELETEDLUI (
                #                                 PLUI	varchar(10) NOT NULL,
                #                                 PSTR	text NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         DELETEDSUI = 'DROP TABLE IF EXISTS DELETEDSUI;
                #                         CREATE TABLE DELETEDSUI (
                #                                 PSUI	varchar(10) NOT NULL,
                #                                 LAT	char(3) NOT NULL,
                #                                 PSTR	text NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MERGEDCUI = 'DROP TABLE IF EXISTS MERGEDCUI;
                #                         CREATE TABLE MERGEDCUI (
                #                                 PCUI	char(8) NOT NULL,
                #                                 CUI	char(8) NOT NULL
                #                         ) CHARACTER SET utf8;',
                #         MERGEDLUI = 'DROP TABLE IF EXISTS MERGEDLUI;
                #                         CREATE TABLE MERGEDLUI (
                #                                 PLUI	varchar(10),
                #                                 LUI	varchar(10)
                #                         ) CHARACTER SET utf8;'
                # )

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

                libPath <- system.file(package = "setupMetathesaurus", "sql/v2/ddl")
                sqlFiles <- normalizePath(file.path(libPath, paste0(tables, ".sql")))

                executeSQL2(sqlPaths = sqlFiles, conn = conn)
                preQL::dcMySQL5.5(conn = conn)


        }
