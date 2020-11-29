#' DDL META Tables
#' @param path Path to the unpacked RRF files
#' @import preQL
#' @importFrom DatabaseConnector dbExecute
#' @export


testCopy <-
        function(conn) {

                        sqlPath <- "inst/sql/pgtest.sql"

                        sql <- SqlRender::render(SqlRender::readSql(sourceFile = sqlPath),
                                                 wd = getwd())

                        #print(sql)
                        #secretary::press_enter()
                        pg13::send(conn = conn,
                                   sql_statement = sql)


        }
