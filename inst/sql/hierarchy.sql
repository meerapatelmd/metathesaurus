DROP SCHEMA {schema} CASCADE;
CREATE SCHEMA {schema};

-----------------------------------------------------------
-- 1. Add `ptr_id` field
-----------------------------------------------------------
DROP TABLE IF EXISTS {schema}.tmp_mrhier;
CREATE TABLE {schema}.tmp_mrhier AS (
	SELECT DISTINCT
	  m.AUI,
	  c.CODE,
	  c.SAB,
	  c.STR,
	  m.RELA,
	  m.PTR
	 FROM mth.mrhier m
	 INNER JOIN mth.mrconso c
	 ON c.aui = m.aui
);


SELECT COUNT(*)
FROM {schema}.tmp_mrhier;

DROP TABLE IF EXISTS {schema}.mrhier;
CREATE TABLE {schema}.mrhier AS (
   SELECT ROW_NUMBER() OVER() AS ptr_id, m.*
   FROM {schema}.tmp_mrhier m
)
;

DROP TABLE IF EXISTS {schema}.tmp_mrhier;


-- QA that the max row number is equal to the row count
SELECT max(ptr_id)
FROM {schema}.mrhier;


-----------------------------------------------------------
-- 2. Subset MRHIER with `ptr_id` by vocabulary
-----------------------------------------------------------
-- Create lookup table between all the hierarchy vocabularies
-- and a cleaned up version if their string representation as their
-- destination table name to loop over
-----------------------------------------------------------
DROP TABLE IF EXISTS {schema}.lookup;
CREATE TABLE {schema}.lookup as (
      SELECT
	    h.sab AS hierarchy_sab,
	    REGEXP_REPLACE(h.sab, '[[:punct:]]', '_') AS hierarchy_table,
	    COUNT(*)
	  FROM mth.mrhier h
	  INNER JOIN mth.mrconso c
	  ON c.aui = h.aui
	  WHERE c.lat = 'ENG'
	  GROUP BY h.sab, hierarchy_table
	  HAVING COUNT(*) > 1
	  ORDER BY COUNT(*)
);

SELECT * FROM {schema}.lookup;


-----------------------------------------------------------
-- Write table subsets
-----------------------------------------------------------
do
$$
declare
    f record;
    tbl varchar(255);
    sab varchar(255);
begin
    for f in select * from {schema}.lookup
    loop
      tbl := f.hierarchy_table;
      sab := f.hierarchy_sab;
	  raise notice '% (% rows)', sab, f.count;
	  EXECUTE
	   format(
  '
  DROP TABLE IF EXISTS {schema}.%s;
  CREATE TABLE  {schema}.%s (
    ptr_id INTEGER NOT NULL,
  	aui varchar(12),
  	code varchar(100),
  	str text,
  	rela varchar(100),
  	relative_aui varchar(12) NOT NULL,
  	relative_code varchar(100),
  	relative_str text,
  	relative_level INTEGER NOT NULL
  );

  WITH relatives0 AS (
	SELECT DISTINCT m.ptr_id, s1.aui, s1.code, s1.str, m.rela, m.ptr
	FROM {schema}.mrhier m
	INNER JOIN (SELECT * FROM mth.mrconso WHERE sab = ''%s'') s1
	ON s1.aui = m.aui
  ),
  relatives1 AS (
  	SELECT ptr_id, aui, code, str, rela, ptr, unnest(string_to_array(ptr, ''.'')) AS relative_aui
  	FROM relatives0 r0
  	ORDER BY ptr_id
  ),
  relatives2 AS (
  	SELECT r1.*, ROW_NUMBER() OVER (PARTITION BY ptr_id) AS relative_level
  	FROM relatives1 r1
  ),
  relatives3 AS (
  	SELECT r2.*, m.code AS relative_code, m.str AS relative_str
  	FROM relatives2 r2
  	LEFT JOIN mth.mrconso m
  	ON m.aui = r2.relative_aui
  )

  INSERT INTO {schema}.%s
  SELECT DISTINCT
    ptr_id,
  	aui,
  	code,
  	str,
  	rela,
  	relative_aui,
  	relative_code,
  	relative_str,
  	relative_level
  FROM relatives3
  ;
  ',tbl, tbl, sab,tbl);
    end loop;
end;
$$
;
