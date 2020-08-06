#' Run Indices
#' @description Run indices on the loaded data.
#' @param path Path to the unpacked RRF files
#' @import preQL
#' @importFrom DatabaseConnector dbExecute
#' @export

runIndices <-
        function(dbname = "umls",
                 username,
                 password) {


                conn <-
                preQL::connectMySQL5.5(dbname = dbname,
                                       username = username,
                                       password = password)

                sqlPath <- paste0(system.file(package = "setupMetathesaurus"), "/sql/indices.sql")

                executeSQL(sqlPath = sqlPath,
                           conn = conn)

                preQL::dcMySQL5.5(conn = conn)

        }
