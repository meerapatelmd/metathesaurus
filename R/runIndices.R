#' Run Indices
#' @description Run indices on the loaded data.
#' @param path Path to the unpacked RRF files
#' @import preQL
#' @importFrom DatabaseConnector dbExecute
#' @export

runIndices <-
        function(dbname = "umls",
                 username,
                 password,
                 full = FALSE) {


                conn <-
                preQL::connectMySQL5.5(dbname = dbname,
                                       username = username,
                                       password = password)

                if (full) {
                        sqlPath <- paste0(system.file(package = "setupMetathesaurus"), "/sql/full_indexes.sql")

                } else {

                        sqlPath <- paste0(system.file(package = "setupMetathesaurus"), "/sql/indices.sql")

                }

                executeSQL(sqlPath = sqlPath,
                           conn = conn)

                preQL::dcMySQL5.5(conn = conn)

        }
