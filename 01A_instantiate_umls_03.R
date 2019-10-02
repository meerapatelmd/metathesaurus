#/usr/local/opt/mysql@5.5/bin/mysql
#CREATE DATABASE IF NOT EXISTS umls CHARACTER SET utf8 COLLATE utf8_unicode_ci;

#/usr/local/opt/mysql@5.5/bin/mysql.server start

#source('~/GitHub/patelm9/projektoR/R/read_new_project_files.R')
projectloggeR::instantiate_project_log(subdir_names = subdir_names)
projektoR::read_new_project_files(subdir_names = subdir_names)

INPUT_DATA_03 <- readRDS("INPUT_DATA_03.RData")
new_colnames <- paste0("X", 1:27)

INPUT_DATA_03_a <- data.frame(t(gsub("^X[0-9]{1,2}$", "", colnames(INPUT_DATA_03))) %>% unname())
INPUT_DATA_03_b <- INPUT_DATA_03 %>% unname()
colnames(INPUT_DATA_03_b) <- new_colnames

INPUT_DATA_03 <-
        bind_rows(INPUT_DATA_03_a,
                  INPUT_DATA_03_b
        )

write_project_csv(INPUT_DATA_03, csv_basename = "sources.mrmap")
##Postgres settings
dbms     <- "postgresql"
server   <- "localhost/polyester"
user     <- "meerapatel"
password <- ""
port     <- "5432"
schema   <- "sources"

conn <- DatabaseConnector::createConnectionDetails(dbms     = dbms,
                                                   user     = user,
                                                   password = password,
                                                   server   = server,
                                                   port     = port,
                                                   schema   = schema)
goteam <- DatabaseConnector::connect(conn)

DatabaseConnector::dbWriteTable(goteam, "mrmap", INPUT_DATA_03[1:10,])
write.csv(INPUT_DATA_03[1:10,], "test.csv", row.names = FALSE)
