/*
The RxNorm Ingredient requires installation of
the 'PL/R' Postgres Extension. This extension allows
for the execution of R scripts in Postgres.
*/

CREATE OR REPLACE FUNCTION write_rxnorm_path_lookup() RETURNS void AS '
library(metathesaurus)
write_rxnorm_path_lookup()
' LANGUAGE plr;


CREATE OR REPLACE FUNCTION get_rxnorm_map(from_tty varchar, to_tty varchar) RETURNS TABLE AS '
library(metathesaurus)
get_rxnorm_map()
' LANGUAGE plr;

SELECT write_rxnorm_path_lookup();
