library(tidyverse)
library(devtools)
options(stringsAsFactors = FALSE)
devtools::install_github("patelm9/mirroR")
devtools::install_github("patelm9/caterpillaR")
devtools::install_github("patelm9/somersaulteR")
devtools::install_github("patelm9/projektoR")
devtools::install_github("patelm9/projectloggeR")
devtools::install_github("patelm9/typewriteR")
devtools::install_github("patelm9/mySeagull")

source("./R/open_metamorphysis.R")

##Settings
subdir_names <- c("INPUT", "OUTPUT", "UMLS", "UMLS/OUTPUT", "R")
input_dir_names <- c("INPUT")
output_dir_names <- c("OUTPUT")
other_dir_names  <- c("R", "UMLS", "UMLS/OUTPUT")


##Creating Directory Tree
projektoR::setup_project_dirtree(input_dir_names = input_dir_names,
                                 output_dir_names = output_dir_names,
                                 other_dir_names = other_dir_names)


##Creating Logs
projectloggeR::instantiate_project_log(dirs_to_log = c(input_dir_names, output_dir_names))

##Checklist
## A. Instantiate UMLS from OHDSI git repo
## B. Instantiate OMOP vocabularies
## C. Get Breast Cancer Data Elements
## D. Create Mapping Strategy
## E. Map

PROJECT_ROADMAP <-
        bind_rows(
                data.frame(
                        STEP_NUMBER = 1,
                        STEP_LABEL = c("Instantiate data sources"),
                        STEP_PART  = LETTERS[1:3],
                        STEP_PART_LABEL = c("Instantiate UMLS from OHDSI git repo",
                                            "Instantiate OMOP vocabularies from Athena",
                                            "Instantiate breast cancer data elements")
                        ),
                data.frame(
                        STEP_NUMBER = 2,
                        STEP_LABEL  = c("Create mapping strategy")
                        ),
                data.frame(
                        STEP_NUMBER = 3,
                        STEP_LABEL  = c("Execute mapping strategy")
                )
        ) %>%
        somersaulteR::add_primary_key("PROJECT_ROADMAP_ID") %>%
        somersaulteR::add_timestamp_column("PROJECT_ROADMAP_TIMESTAMP")

if (!(file.exists("PROJECT_ROADMAP.RData"))) {
        saveRDS(PROJECT_ROADMAP, "PROJECT_ROADMAP.RData")
} else {
        PROJECT_ROADMAP <- readRDS("PROJECT_ROADMAP.RData")
}
