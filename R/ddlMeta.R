#' DDL META Tables
#' @param path Path to the unpacked RRF files
#' @import preQL
#' @importFrom DatabaseConnector dbExecute
#' @export


ddlMeta <-
        function(dbname = "umls",
                 username,
                 password,
                 full = FALSE) {


                conn <-
                preQL::connectMySQL5.5(dbname = dbname,
                                       username = username,
                                       password = password)

                if (full) {
                        sqlPath <- paste0(system.file(package = "setupMetathesaurus"), "/sql/full_ddl.sql")

                } else {
                        sqlPath <- paste0(system.file(package = "setupMetathesaurus"), "/sql/ddl.sql")
                }

                executeSQL(sqlPath = sqlPath,
                           conn = conn)
                preQL::dcMySQL5.5(conn = conn)


        }
