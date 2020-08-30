load data local infile '@filePath/MRCONSO.RRF' into table MRCONSO fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/MRCUI.RRF' into table MRCUI fields terminated by '|' lines terminated by '\n';

load data local infile '@filePath/MRCXT.RRF' into table MRCXT fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/MRDEF.RRF' into table MRDEF fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/MRDOC.RRF' into table MRDOC fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/MRFILES.RRF' into table MRFILES fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/MRHIER.RRF' into table MRHIER fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/MRHIST.RRF' into table MRHIST fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/MRMAP.RRF' into table MRMAP fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/MRRANK.RRF' into table MRRANK fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/MRREL.RRF' into table MRREL fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/MRSAB.RRF' into table MRSAB fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/MRSAT.RRF' into table MRSAT fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/MRSMAP.RRF' into table MRSMAP fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/MRSTY.RRF' into table MRSTY fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/MRXNS_ENG.RRF' into table MRXNS_ENG fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/MRXNW_ENG.RRF' into table MRXNW_ENG fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/MRAUI.RRF' into table MRAUI fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/MRXW_ENG.RRF' into table MRXW_ENG fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/AMBIGSUI.RRF' into table AMBIGSUI fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/AMBIGLUI.RRF' into table AMBIGLUI fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/CHANGE/DELETEDCUI.RRF' into table DELETEDCUI fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/CHANGE/DELETEDLUI.RRF' into table DELETEDLUI fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/CHANGE/DELETEDSUI.RRF' into table DELETEDSUI fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/CHANGE/MERGEDCUI.RRF' into table MERGEDCUI fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

load data local infile '@filePath/CHANGE/MERGEDLUI.RRF' into table MERGEDLUI fields terminated by '|' ESCAPED BY '' lines terminated by '\n';
