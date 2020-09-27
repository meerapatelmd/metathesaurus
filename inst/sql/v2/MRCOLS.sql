DROP TABLE IF EXISTS MRCOLS;
CREATE TABLE MRCOLS (
    COL	varchar(40),
    DES	varchar(200),
    REF	varchar(40),
    MIN	int unsigned,
    AV	numeric(5,2),
    MAX	int unsigned,
    FIL	varchar(50),
    DTY	varchar(40)
) CHARACTER SET utf8;
