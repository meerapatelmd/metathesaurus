##Settings
## After executing UMLS Metathesaurus, the following were copied from UMLS to INPUT/NEW:
##      MRCONSO.RRF
##      MRHIER.RRF
##      MRMAP.RRF
##      MRSMAP.RRF
##      MRSAT.RRF
##      MRREL.RRF

##Settings
path_to_mmys_output <- "./UMLS/OUTPUT/2019AA/META"
target_filenames    <- c("MRCONSO.RRF",
                         "MRHIER.RRF",
                         "MRMAP.RRF",
                         "MRSMAP.RRF",
                         "MRSAT.RRF",
                         "MRREL.RRF"
)

destination_path <- "./INPUT/NEW"

full_filenames <- mirroR::create_path_to_file(path_to_mmys_output, target_filenames)
new_full_filenames <- mirroR::create_path_to_file(destination_path, target_filenames)

mapply(file.copy, full_filenames, new_full_filenames)
