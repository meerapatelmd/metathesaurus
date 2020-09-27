DROP TABLE IF EXISTS MRSMAP;
CREATE TABLE MRSMAP (
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
) CHARACTER SET utf8;
