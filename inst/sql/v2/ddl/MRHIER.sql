DROP TABLE IF EXISTS MRHIER;
CREATE TABLE MRHIER (
    CUI	char(8) NOT NULL,
    AUI	varchar(9) NOT NULL,
    CXN	int unsigned NOT NULL,
    PAUI	varchar(10),
    SAB	varchar(40) NOT NULL,
    RELA	varchar(100),
    PTR	text,
    HCD	varchar(100),
    CVF	int unsigned
) CHARACTER SET utf8;
