##Settings
## Metamorphosys saves output to the UMLS/OUTPUT/{version}/ Directory. MySQL scripts for the NET subdir are executed, but MySQL scripts are not
## available for the META or LEX outputs and need to be generated in-house. For this iteration the following RRF files are chosen to populate
## our mySQL umls database in their respective tables because these are the files used for OMOP Vocabulary 5.0:
##      MRCONSO.RRF
##      MRHIER.RRF
##      MRMAP.RRF
##      MRSMAP.RRF
##      MRSAT.RRF
##      MRREL.RRF
## shell/mysql_meta_tables.sql was forked from the load_source_tables.sql found at https://github.com/patelm9/Vocabulary-v5.0/tree/master/UMLS
## and the LOAD DATA INTO... statements were added to populate the tables. The following script will create new LOAD DATA INTO statements if
## additional tables/rrfs are desired in the umls database in the future. However, the CREATE TABLE functions would still need to be written
## to mysql_meta_tables.sql

##Settings
target_rrf_files <- c("MRCONSO.RRF",
                         "MRHIER.RRF",
                         "MRMAP.RRF",
                         "MRSMAP.RRF",
                         "MRSAT.RRF",
                         "MRREL.RRF"
)

##Getting tablenames from rrf filenames
target_tablenames <- mirroR::strip_fn(target_rrf_files)
current_tablenames <- mySeagull::get_tables("umls")
if (length(caterpillaR::diff_between_vectors(target_tablenames, current_tablenames))) {
        typewriteR::tell_me("The following tables need to be written:")
        typewriteR::tell_me("\t", caterpillaR::diff_between_vectors(target_tablenames, current_tablenames))
}

rm(target_tablenames, current_tablenames, target_rrf_files)



