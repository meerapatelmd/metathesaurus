/*
The RxNorm Ingredient requires installation of
the 'PL/R' Postgres Extension. This extension allows
for the execution of R scripts in Postgres.

Clone the git repository https://github.com/postgres-plr/plr
From the directory run:
```
export R_HOME=/Library/Frameworks/R.framework/Resources
export PATH=$PATH:/Applications/Postgres.app/Contents/Versions/13/bin
USE_PGXS=1 make clean
USE_PGXS=1 make install
```
*/

CREATE OR REPLACE FUNCTION setup_rxmap(mth_version varchar, mth_release_dt varchar) RETURNS void AS '
library(metathesaurus)
setup_rxmap(mth_version = mth_version,
mth_release_dt = mth_release_dt)
' LANGUAGE plr;


SELECT setup_rxmap('{mth_version}', '{mth_release_dt}');




WITH compl AS (
 SELECT tty,aui,code,str, in_aui AS ingr_aui FROM rxmap.rxnorm_ingredient_map
 UNION
 SELECT tty,aui,code,str, pin_aui AS ingr_aui FROM rxmap.rxnorm_precise_ingredient_map
 UNION
 SELECT tty,aui,code,str, min_aui AS ingr_aui FROM rxmap.rxnorm_multi_ingredient_map
)

select
  map.tty,
  map.aui,
  map.code,
  map.str,
  ingr.*
from rxclass.rxclass_rxnorm_in_pin_min_map ingr
LEFT JOIN compl map
ON map.ingr_aui = ingr.rxnorm_in_pin_min_aui
where map.aui IS NOT NULL
UNION
select
  ingr.rxnorm_in_pin_min_tty as tty,
  ingr.rxnorm_in_pin_min_aui AS aui,
  ingr.rxnorm_in_pin_min_code::integer AS code,
  ingr.rxnorm_in_pin_min_str AS str,
  ingr.*
from rxclass.rxclass_rxnorm_in_pin_min_map ingr
;
