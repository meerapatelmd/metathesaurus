load data local infile '@filePath/MRCONSO.RRF' into table MRCONSO fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/MRHIER.RRF' into table MRHIER fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/MRMAP.RRF' into table MRMAP fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/MRSMAP.RRF' into table MRSMAP fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/MRSAT.RRF' into table MRSAT fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/MRREL.RRF' into table MRREL fields terminated by '|' ESCAPED BY '' lines terminated by '\n';
