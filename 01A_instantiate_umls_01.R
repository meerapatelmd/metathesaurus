##Settings
PROJECT_ROADMAP_ID          <- "1"
PROJECT_ROADMAP_ID_COMMENT <- "Instantiating UMLS vocabulary from MetaMorphoSys using open_metamorphysis() and whole_enchilada.props configuration file because batch run method was throwing errors."

##Preparation
somersaulteR::mutate_new_cols_if_not_exist(PROJECT_ROADMAP, "PROJECT_ROADMAP_ID_COMMENT")
somersaulteR::append_dataframe_if_new_obs(PROJECT_ROADMAP, "PROJECT_ROADMAP_ID", "PROJECT_ROADMAP_ID_COMMENT", PROJECT_ROADMAP_TIMESTAMP)
mirroR::save_robj(PROJECT_ROADMAP)

##Opening Metamorphosys and executing based on whole_enchilada.props configuration (ALL possible vocabularies and data elements in this version)
open_metamorphysis()


## Downloaded 2019AA-full from UMLS site, unzipped Metamorphysis into the same directory, and executed using UMLS/whole_enchilada.prop configuration
#### UMLS/OUTPUT/2019AA contains Metamorphosysis output



