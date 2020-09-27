DROP TABLE IF EXISTS MRCUI;
CREATE TABLE MRCUI (
    CUI1	char(8) NOT NULL,
    VER	varchar(10) NOT NULL,
    REL	varchar(4) NOT NULL,
    RELA	varchar(100),
    MAPREASON	text,
    CUI2	char(8),
    MAPIN	char(1)
) CHARACTER SET utf8;
