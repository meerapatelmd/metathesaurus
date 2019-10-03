/**************************************************************************
* Copyright 2016 Observational Health Data Sciences and Informatics (OHDSI)
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
* Authors: Timur Vakhitov, Christian Reich
* Date: 2017
**************************************************************************/

DROP TABLE IF EXISTS MRCONSO;
CREATE TABLE MRCONSO
(
  CUI       CHAR(8),
  LAT       CHAR(3),
  TS        CHAR(1),
  LUI       VARCHAR(10),
  STT       VARCHAR(3),
  SUI       VARCHAR(10),
  ISPREF    CHAR(1),
  AUI       VARCHAR(9) NOT NULL,
  SAUI      VARCHAR(50),
  SCUI      VARCHAR(100),
  SDUI      VARCHAR(100),
  SAB       VARCHAR(40),
  TTY       VARCHAR(40),
  CODE      VARCHAR(100),
  STR       VARCHAR(3000),
  SRL       INT ,
  SUPPRESS  CHAR(1),
  CVF       INT,
  FILLER_COLUMN INT
);

load data local infile 'MRCONSO.RRF' into table MRCONSO fields terminated by '|' ESCAPED BY '' lines terminated by '\n';


DROP TABLE IF EXISTS MRHIER;
CREATE TABLE MRHIER
(
  CUI   CHAR(8),
  AUI   VARCHAR(9),
  CXN   INT,
  PAUI  VARCHAR(9),
  SAB   VARCHAR(40),
  RELA  VARCHAR(100),
  PTR   VARCHAR(1000),
  HCD   VARCHAR(150),
  CVF   INT,
  FILLER_COLUMN INT
);

load data local infile 'MRHIER.RRF' into table MRHIER fields terminated by '|' ESCAPED BY '' lines terminated by '\n';


DROP TABLE IF EXISTS MRMAP;
CREATE TABLE MRMAP
(
  MAPSETCUI    CHAR(8),
  MAPSETSAB    VARCHAR(40),
  MAPSUBSETID  VARCHAR(10),
  MAPRANK      INT,
  MAPID        VARCHAR(50),
  MAPSID       VARCHAR(50),
  FROMID       VARCHAR(50),
  FROMSID      VARCHAR(50),
  FROMEXPR     TEXT(4000),
  FROMTYPE     VARCHAR(50),
  FROMRULE     TEXT(4000),
  FROMRES      TEXT(4000),
  REL          VARCHAR(4),
  RELA         VARCHAR(100),
  TOID         VARCHAR(50),
  TOSID        VARCHAR(50),
  TOEXPR       TEXT(4000),
  TOTYPE       VARCHAR(50),
  TORULE       TEXT(4000),
  TORES        TEXT(4000),
  MAPRULE      TEXT(4000),
  MAPRES       TEXT(4000),
  MAPTYPE      VARCHAR(50),
  MAPATN       VARCHAR(20),
  MAPATV       TEXT(4000),
  CVF          INT,
  FILLER_COLUMN INT
);

load data local infile 'MRMAP.RRF' into table MRMAP fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

DROP TABLE IF EXISTS MRSMAP;
CREATE TABLE MRSMAP
(
  MAPSETCUI            CHAR (8),
  MAPSETSAB            VARCHAR (40),
  MAPID                VARCHAR (50),
  MAPSID               VARCHAR (50),
  FROMEXPR             VARCHAR (4000),
  FROMTYPE             VARCHAR (50),
  REL                  VARCHAR (4),
  RELA                 VARCHAR (100),
  TOEXPR               VARCHAR (4000),
  TOTYPE               VARCHAR (50),
  CVF                  INT,
  VOCABULARY_DATE      DATE,
  VOCABULARY_VERSION   VARCHAR (200)
);

load data local infile 'MRSMAP.RRF' into table MRSMAP fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

DROP TABLE IF EXISTS MRSAT;
CREATE TABLE MRSAT
(
  CUI       CHAR(8),
  LUI       VARCHAR(10),
  SUI       VARCHAR(10),
  METAUI    VARCHAR(50),
  STYPE     VARCHAR(50),
  CODE      VARCHAR(50),
  ATUI      VARCHAR(11),
  SATUI     VARCHAR(50),
  ATN       VARCHAR(100),
  SAB       VARCHAR(40),
  ATV       TEXT,
  SUPPRESS  CHAR(1),
  CVF       VARCHAR(50),
  FILLER_COLUMN INT
);

load data local infile 'MRSAT.RRF' into table MRSAT fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

DROP TABLE IF EXISTS MRREL;
CREATE TABLE MRREL
(
  CUI1      CHAR(8),
  AUI1      VARCHAR(9),
  STYPE1    VARCHAR(50),
  REL       VARCHAR(4),
  CUI2      CHAR(8),
  AUI2      VARCHAR(9),
  STYPE2    VARCHAR(50),
  RELA      VARCHAR(100),
  RUI       VARCHAR(10),
  SRUI      VARCHAR(50),
  SAB       VARCHAR(40),
  SL        VARCHAR(40),
  RG        VARCHAR(10),
  DIR       VARCHAR(1),
  SUPPRESS  CHAR(1),
  CVF       INT,
  FILLER_COLUMN INT
);

load data local infile 'MRREL.RRF' into table MRREL fields terminated by '|' ESCAPED BY '' lines terminated by '\n';

CREATE INDEX X_MRSAT_CUI ON MRSAT (CUI);
CREATE INDEX X_MRCONSO_CODE ON MRCONSO (CODE);
CREATE INDEX X_MRCONSO_CUI ON MRCONSO (CUI);
CREATE INDEX X_MRCONSO_LUI ON MRCONSO (LUI);
CREATE UNIQUE INDEX X_MRCONSO_PK ON MRCONSO (AUI);
CREATE INDEX X_MRCONSO_SAB_TTY ON MRCONSO (SAB, TTY);
CREATE INDEX X_MRCONSO_SCUI ON MRCONSO (SCUI);
CREATE INDEX X_MRCONSO_SDUI ON MRCONSO (SDUI);
CREATE INDEX X_MRCONSO_STR ON MRCONSO (STR);
CREATE INDEX X_MRCONSO_SUI ON MRCONSO (SUI);
CREATE INDEX X_MRREL_AUI ON MRREL (AUI1, AUI2);
