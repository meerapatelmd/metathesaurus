#' DDL META Tables
#' @param path Path to the unpacked RRF files
#' @import preQL
#' @importFrom DatabaseConnector dbExecute
#' @export


copyPg <-
        function(conn,
                 csvFile) {

                        tableName <- cave::strip_fn(csvFile)

                        sqlPath <- "inst/sql/copy.sql"

                        sql <- SqlRender::render(SqlRender::readSql(sourceFile = sqlPath),
                                                 wd = getwd(),
                                                 tableName = tableName)

                        #print(sql)
                        #secretary::press_enter()
                        pg13::send(conn = conn,
                                   sql_statement = sql)


        }
