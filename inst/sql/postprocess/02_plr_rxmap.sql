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
