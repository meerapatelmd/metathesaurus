DROP TABLE IF EXISTS umls.MRCOLS;
CREATE TABLE umls.MRCOLS (
    COL	varchar(40),
    DES	varchar(200),
    REF	varchar(40),
    MIN	integer,
    AV	numeric(5,2),
    MAX	integer,
    FIL	varchar(50),
    DTY	varchar(40)
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRCONSO;
CREATE TABLE umls.MRCONSO (
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
    STR	text,
    SRL	integer NOT NULL,
    SUPPRESS	char(1) NOT NULL,
    CVF	text
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRCUI;
CREATE TABLE umls.MRCUI (
    CUI1	char(8) NOT NULL,
    VER	varchar(10) NOT NULL,
    REL	varchar(4) NOT NULL,
    RELA	varchar(100),
    MAPREASON	text,
    CUI2	char(8),
    MAPIN	char(1)
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRCXT;
CREATE TABLE umls.MRCXT (
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
    CVF	integer
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRDEF;
CREATE TABLE umls.MRDEF (
    CUI	char(8) NOT NULL,
    AUI	varchar(9) NOT NULL,
    ATUI	varchar(11) NOT NULL,
    SATUI	varchar(50),
    SAB	varchar(40) NOT NULL,
    DEF	text NOT NULL,
    SUPPRESS	char(1) NOT NULL,
    CVF	integer
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRDOC;
CREATE TABLE umls.MRDOC (
    DOCKEY	varchar(50) NOT NULL,
    VALUE	varchar(200),
    TYPE	varchar(50) NOT NULL,
    EXPL	text
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRFILES;
CREATE TABLE umls.MRFILES (
    FIL	varchar(50),
    DES	varchar(200),
    FMT	text,
    CLS	integer,
    RWS	integer,
    BTS	bigint
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRHIER;
CREATE TABLE umls.MRHIER (
    CUI	char(8) NOT NULL,
    AUI	varchar(9) NOT NULL,
    CXN	integer NOT NULL,
    PAUI	varchar(10),
    SAB	varchar(40) NOT NULL,
    RELA	varchar(100),
    PTR	text,
    HCD	varchar(100),
    CVF	integer
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRHIST;
CREATE TABLE umls.MRHIST (
    CUI	char(8),
    SOURCEUI	varchar(100),
    SAB	varchar(40),
    SVER	varchar(40),
    CHANGETYPE	text,
    CHANGEKEY	text,
    CHANGEVAL	text,
    REASON	text,
    CVF	integer
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRMAP;
CREATE TABLE umls.MRMAP (
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
    CVF	integer
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRRANK;
CREATE TABLE umls.MRRANK (
    MRRANK_RANK	integer NOT NULL,
    SAB	varchar(40) NOT NULL,
    TTY	varchar(40) NOT NULL,
    SUPPRESS	char(1) NOT NULL
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRREL;
CREATE TABLE umls.MRREL (
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
    CVF	integer
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRSAB;
CREATE TABLE umls.MRSAB (
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
    SCIT	text NOT NULL
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRSAT;
CREATE TABLE umls.MRSAT (
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
    CVF	integer
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRSMAP;
CREATE TABLE umls.MRSMAP (
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
    CVF	integer
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRSTY;
CREATE TABLE umls.MRSTY (
    CUI	char(8) NOT NULL,
    TUI	char(4) NOT NULL,
    STN	varchar(100) NOT NULL,
    STY	varchar(50) NOT NULL,
    ATUI	varchar(11) NOT NULL,
    CVF	integer
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRXNS_ENG;
CREATE TABLE umls.MRXNS_ENG (
    LAT	char(3) NOT NULL,
    NSTR	text NOT NULL,
    CUI	char(8) NOT NULL,
    LUI	varchar(10) NOT NULL,
    SUI	varchar(10) NOT NULL
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRXNW_ENG;
CREATE TABLE umls.MRXNW_ENG (
    LAT	char(3) NOT NULL,
    NWD	varchar(100) NOT NULL,
    CUI	char(8) NOT NULL,
    LUI	varchar(10) NOT NULL,
    SUI	varchar(10) NOT NULL
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRAUI;
CREATE TABLE umls.MRAUI (
    AUI1	varchar(9) NOT NULL,
    CUI1	char(8) NOT NULL,
    VER	varchar(10) NOT NULL,
    REL	varchar(4),
    RELA	varchar(100),
    MAPREASON	text NOT NULL,
    AUI2	varchar(9) NOT NULL,
    CUI2	char(8) NOT NULL,
    MAPIN	char(1) NOT NULL
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRXW_BAQ;
CREATE TABLE umls.MRXW_BAQ (
    LAT	char(3) NOT NULL,
    WD	varchar(200) NOT NULL,
    CUI	char(8) NOT NULL,
    LUI	varchar(10) NOT NULL,
    SUI	varchar(10) NOT NULL
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRXW_CHI;
CREATE TABLE umls.MRXW_CHI (
    LAT	char(3) NOT NULL,
    WD	varchar(200) NOT NULL,
    CUI	char(8) NOT NULL,
    LUI	varchar(10) NOT NULL,
    SUI	varchar(10) NOT NULL
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRXW_CZE;
CREATE TABLE umls.MRXW_CZE (
    LAT	char(3) NOT NULL,
    WD	varchar(200) NOT NULL,
    CUI	char(8) NOT NULL,
    LUI	varchar(10) NOT NULL,
    SUI	varchar(10) NOT NULL
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRXW_DAN;
CREATE TABLE umls.MRXW_DAN (
    LAT	char(3) NOT NULL,
    WD	varchar(200) NOT NULL,
    CUI	char(8) NOT NULL,
    LUI	varchar(10) NOT NULL,
    SUI	varchar(10) NOT NULL
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRXW_DUT;
CREATE TABLE umls.MRXW_DUT (
    LAT	char(3) NOT NULL,
    WD	varchar(200) NOT NULL,
    CUI	char(8) NOT NULL,
    LUI	varchar(10) NOT NULL,
    SUI	varchar(10) NOT NULL
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRXW_ENG;
CREATE TABLE umls.MRXW_ENG (
    LAT	char(3) NOT NULL,
    WD	varchar(200) NOT NULL,
    CUI	char(8) NOT NULL,
    LUI	varchar(10) NOT NULL,
    SUI	varchar(10) NOT NULL
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRXW_EST;
CREATE TABLE umls.MRXW_EST (
    LAT	char(3) NOT NULL,
    WD	varchar(200) NOT NULL,
    CUI	char(8) NOT NULL,
    LUI	varchar(10) NOT NULL,
    SUI	varchar(10) NOT NULL
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRXW_FIN;
CREATE TABLE umls.MRXW_FIN (
    LAT	char(3) NOT NULL,
    WD	varchar(200) NOT NULL,
    CUI	char(8) NOT NULL,
    LUI	varchar(10) NOT NULL,
    SUI	varchar(10) NOT NULL
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRXW_FRE;
CREATE TABLE umls.MRXW_FRE (
    LAT	char(3) NOT NULL,
    WD	varchar(200) NOT NULL,
    CUI	char(8) NOT NULL,
    LUI	varchar(10) NOT NULL,
    SUI	varchar(10) NOT NULL
,FILLER_COLUMN text);
DROP TABLE IF EXISTS umls.MRXW_GER;
CREATE TABLE umls.MRXW_GER (
    LAT	char(3) NOT NULL,
    WD	varchar(200) NOT NULL,
    CUI	char(8) NOT NULL,
    LUI	varchar(10) NOT NULL,
    SUI	varchar(10) NOT NULL
,FILLER_COLUMN text);

DROP TABLE IF EXISTS umls.MRXW_GRE;
CREATE TABLE umls.MRXW_GRE (
    LAT	char(3) NOT NULL,
    WD	varchar(200) NOT NULL,
    CUI	char(8) NOT NULL,
    LUI	varchar(10) NOT NULL,
    SUI	varchar(10) NOT NULL
,FILLER_COLUMN text);

DROP TABLE IF EXISTS umls.MRXW_HEB;
CREATE TABLE umls.MRXW_HEB (
    LAT	char(3) NOT NULL,
    WD	varchar(200) NOT NULL,
    CUI	char(8) NOT NULL,
    LUI	varchar(10) NOT NULL,
    SUI	varchar(10) NOT NULL
,FILLER_COLUMN text);

DROP TABLE IF EXISTS umls.MRXW_HUN;
CREATE TABLE umls.MRXW_HUN (
    LAT	char(3) NOT NULL,
    WD	varchar(200) NOT NULL,
    CUI	char(8) NOT NULL,
    LUI	varchar(10) NOT NULL,
    SUI	varchar(10) NOT NULL
,FILLER_COLUMN text);

DROP TABLE IF EXISTS umls.MRXW_ITA;
CREATE TABLE umls.MRXW_ITA (
    LAT	char(3) NOT NULL,
    WD	varchar(200) NOT NULL,
    CUI	char(8) NOT NULL,
    LUI	varchar(10) NOT NULL,
    SUI	varchar(10) NOT NULL
,FILLER_COLUMN text);

DROP TABLE IF EXISTS umls.MRXW_JPN;
CREATE TABLE umls.MRXW_JPN (
    LAT char(3) NOT NULL,
    WD  varchar(500) NOT NULL,
    CUI char(8) NOT NULL,
    LUI varchar(10) NOT NULL,
    SUI varchar(10) NOT NULL
,FILLER_COLUMN text);

DROP TABLE IF EXISTS umls.MRXW_KOR;
CREATE TABLE umls.MRXW_KOR (
    LAT char(3) NOT NULL,
    WD  varchar(500) NOT NULL,
    CUI char(8) NOT NULL,
    LUI varchar(10) NOT NULL,
    SUI varchar(10) NOT NULL
,FILLER_COLUMN text);

DROP TABLE IF EXISTS umls.MRXW_LAV;
CREATE TABLE umls.MRXW_LAV (
    LAT char(3) NOT NULL,
    WD  varchar(200) NOT NULL,
    CUI char(8) NOT NULL,
    LUI varchar(10) NOT NULL,
    SUI varchar(10) NOT NULL
,FILLER_COLUMN text);

DROP TABLE IF EXISTS umls.MRXW_NOR;
CREATE TABLE umls.MRXW_NOR (
    LAT	char(3) NOT NULL,
    WD	varchar(200) NOT NULL,
    CUI	char(8) NOT NULL,
    LUI	varchar(10) NOT NULL,
    SUI	varchar(10) NOT NULL
,FILLER_COLUMN text);

DROP TABLE IF EXISTS umls.MRXW_POL;
CREATE TABLE umls.MRXW_POL (
    LAT	char(3) NOT NULL,
    WD	varchar(200) NOT NULL,
    CUI	char(8) NOT NULL,
    LUI	varchar(10) NOT NULL,
    SUI	varchar(10) NOT NULL
,FILLER_COLUMN text);

DROP TABLE IF EXISTS umls.MRXW_POR;
CREATE TABLE umls.MRXW_POR (
    LAT	char(3) NOT NULL,
    WD	varchar(200) NOT NULL,
    CUI	char(8) NOT NULL,
    LUI	varchar(10) NOT NULL,
    SUI	varchar(10) NOT NULL
,FILLER_COLUMN text);

DROP TABLE IF EXISTS umls.MRXW_RUS;
CREATE TABLE umls.MRXW_RUS (
    LAT char(3) NOT NULL,
    WD  varchar(200) NOT NULL,
    CUI char(8) NOT NULL,
    LUI varchar(10) NOT NULL,
    SUI varchar(10) NOT NULL
,FILLER_COLUMN text);

DROP TABLE IF EXISTS umls.MRXW_SCR;
CREATE TABLE umls.MRXW_SCR (
    LAT char(3) NOT NULL,
    WD  varchar(200) NOT NULL,
    CUI char(8) NOT NULL,
    LUI varchar(10) NOT NULL,
    SUI varchar(10) NOT NULL
,FILLER_COLUMN text);

DROP TABLE IF EXISTS umls.MRXW_SPA;
CREATE TABLE umls.MRXW_SPA (
    LAT	char(3) NOT NULL,
    WD	varchar(200) NOT NULL,
    CUI	char(8) NOT NULL,
    LUI	varchar(10) NOT NULL,
    SUI	varchar(10) NOT NULL
,FILLER_COLUMN text);

DROP TABLE IF EXISTS umls.MRXW_SWE;
CREATE TABLE umls.MRXW_SWE (
    LAT	char(3) NOT NULL,
    WD	varchar(200) NOT NULL,
    CUI	char(8) NOT NULL,
    LUI	varchar(10) NOT NULL,
    SUI	varchar(10) NOT NULL
,FILLER_COLUMN text);

DROP TABLE IF EXISTS umls.MRXW_TUR;
CREATE TABLE umls.MRXW_TUR (
    LAT	char(3) NOT NULL,
    WD	varchar(200) NOT NULL,
    CUI	char(8) NOT NULL,
    LUI	varchar(10) NOT NULL,
    SUI	varchar(10) NOT NULL
,FILLER_COLUMN text);

DROP TABLE IF EXISTS umls.AMBIGSUI;
CREATE TABLE umls.AMBIGSUI (
    SUI	varchar(10) NOT NULL,
    CUI	char(8) NOT NULL
,FILLER_COLUMN text);

DROP TABLE IF EXISTS umls.AMBIGLUI;
CREATE TABLE umls.AMBIGLUI (
    LUI	varchar(10) NOT NULL,
    CUI	char(8) NOT NULL
,FILLER_COLUMN text);

DROP TABLE IF EXISTS umls.DELETEDCUI;
CREATE TABLE umls.DELETEDCUI (
    PCUI	char(8) NOT NULL,
    PSTR	text NOT NULL
,FILLER_COLUMN text);

DROP TABLE IF EXISTS umls.DELETEDLUI;
CREATE TABLE umls.DELETEDLUI (
    PLUI	varchar(10) NOT NULL,
    PSTR	text NOT NULL
,FILLER_COLUMN text);

DROP TABLE IF EXISTS umls.DELETEDSUI;
CREATE TABLE umls.DELETEDSUI (
    PSUI	varchar(10) NOT NULL,
    LAT	char(3) NOT NULL,
    PSTR	text NOT NULL
,FILLER_COLUMN text);

DROP TABLE IF EXISTS umls.MERGEDCUI;
CREATE TABLE umls.MERGEDCUI (
    PCUI	char(8) NOT NULL,
    CUI	char(8) NOT NULL
,FILLER_COLUMN text);

DROP TABLE IF EXISTS umls.MERGEDLUI;
CREATE TABLE umls.MERGEDLUI (
    PLUI	varchar(10),
    LUI	varchar(10)
,FILLER_COLUMN text);

