DROP TABLE IF EXISTS {postprocess_schema}.snomedct_us;
CREATE TABLE  {postprocess_schema}.%s (
ptr_id INTEGER NOT NULL,
ptr text NOT NULL,
  aui varchar(12),
  code varchar(100),
  str text,
  rela varchar(100),
  ptr_level INTEGER NOT NULL,
  ptr_aui varchar(12) NOT NULL,
  ptr_code varchar(100),
  ptr_str text
);

WITH relatives0 AS (
SELECT DISTINCT m.ptr_id, s1.aui, s1.code, s1.str, m.rela, m.ptr
FROM {postprocess_schema}.mrhier m
INNER JOIN {schema}.mrconso s1
ON s1.aui = m.aui
WHERE m.sab = ''%s''
),
relatives1 AS (
  SELECT ptr_id, ptr, aui, code, str, rela, unnest(string_to_array(ptr, ''.'')) AS ptr_aui
  FROM relatives0 r0
  ORDER BY ptr_id
),
relatives2 AS (
  SELECT r1.*, ROW_NUMBER() OVER (PARTITION BY ptr_id) AS ptr_level
  FROM relatives1 r1
),
relatives3 AS (
  SELECT r2.*, m.code AS ptr_code, m.str AS ptr_str
  FROM relatives2 r2
  LEFT JOIN {schema}.mrconso m
  ON m.aui = r2.ptr_aui
)

INSERT INTO {postprocess_schema}.%s
SELECT DISTINCT
ptr_id,
ptr,
  aui,
  code,
  str,
  rela,
  ptr_level,
  ptr_aui,
  ptr_code,
  ptr_str
FROM relatives3
ORDER BY ptr_id, ptr_level
;
