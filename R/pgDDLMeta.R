#' DDL META Tables
#' @param path Path to the unpacked RRF files
#' @import preQL
#' @importFrom DatabaseConnector dbExecute
#' @export


pgDDLlMeta <-
        function(conn) {

                        sqlPath <- paste0(system.file(package = "setupMetathesaurus"), "/sql/full_pg_ddl.sql")

                        sql <- SqlRender::readSql(sourceFile = sqlPath)
                        sqlList <- pg13::parseSQL(sql_statement = sql)

                       pg13::sendList(conn = conn,
                                      sqlList = sqlList)


        }
