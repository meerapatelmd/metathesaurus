# setupMetathesaurus::ddlMeta(full = TRUE,
#                             username = Sys.getenv("umls_username"),
#                             password = Sys.getenv("umls_password"))
# setupMetathesaurus::loadMeta(full = TRUE,
#                              path = "~/Desktop/2020AA/META",
#                              username = Sys.getenv("umls_username"),
#                              password = Sys.getenv("umls_password"))
# setupMetathesaurus::runIndices(full = TRUE,
#                              username = Sys.getenv("umls_username"),
#                              password = Sys.getenv("umls_password"))


# sqlPath <- paste0("inst/sql/full_pg_ddl.sql")
# SQL <- readSql(sqlPath)
# SQLs <-
#         centipede::strsplit(SQL, split = "[;]", type = "after") %>%
#         unlist() %>%
#         centipede::trimws()
#
# pgSQL <-
# SqlRender::translate(sql = SQL,
#                      targetDialect = "postgresql")

library(tidyverse)
library(rubix)
rrfToCsv(path = "~/Desktop/2020AA/META")
