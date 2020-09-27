DROP TABLE IF EXISTS MRREL;
CREATE TABLE MRREL (
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
) CHARACTER SET utf8;
