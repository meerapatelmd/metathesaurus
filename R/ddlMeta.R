#' DDL META Tables
#' @param path Path to the unpacked RRF files
#' @import preQL
#' @importFrom DatabaseConnector dbExecute
#' @export


ddlMeta <-
        function(dbname = "umls",
                 username,
                 password) {

                conn <-
                preQL::connectMySQL5.5(dbname = dbname,
                                       username = username,
                                       password = password)

                sqlPath <- paste0(system.file(package = "setupMetathesaurus"), "/sql/ddl.sql")

                sql_statement <-
                        SqlRender::render(SqlRender::readSql(sourceFile = sqlPath))

                sql_statement <-
                        SqlRender::translate(sql = sql_statement,
                                             targetDialect = "oracle")


                sql_statement <-
                        centipede::strsplit(sql_statement, split = "[;]{1}", type = "after") %>%
                        unlist() %>%
                        trimws() %>%
                        centipede::no_blank()

                #print(sql_statement)


                output <- list()
                while (length(sql_statement) > 0) {
                        sql <- sql_statement[1]

                        res <-
                        tryCatch(RMySQL::dbSendQuery(conn = conn,
                                      statement = sql),
                                      error = function(e) "Error")

                        if (class(res) != "MySQLResult") {
                                secretary::typewrite_error(sql)
                        }

                        tryCatch(
                        output[[length(output)+1]] <-
                                RMySQL::dbGetStatement(res),
                        error = function(e) "Error")

                        tryCatch(
                        RMySQL::dbClearResult(res),
                        error = function(e) "Error")


                        sql_statement <- sql_statement[-1]
                }

                preQL::dcMySQL5.5(conn = conn)


        }
