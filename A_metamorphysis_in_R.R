source("./R/open_metamorphysis.R")

##Procedure
## 1. Downloaded umls-2019AA-full.zip to UMLS/ and unzip. Unzip mmsys.zip into the same directory
path_to_mmsys <- "UMLS/2019AB/mmsys"

##Opening Metamorphosys and executing based on whole_enchilada.props configuration (ALL possible vocabularies and data elements in this version)
open_metamorphysis(path_to_mmsys = path_to_mmsys)

