#' Execute SQL
#' @description This function:
#'     1. Renders a SQL File
#'     2. Translates the dialect to Oracle dialect
#'     3. To identify exact statements that have thrown errors and they are first isolated by splitting single string read from the file on the semicolon
#'     4. Each single line is sent to the database. If an error is thrown, the single statement is printed in the R console in red.
#' @importFrom magrittr %>%
#' @import SqlRender
#' @importFrom centipede strsplit
#' @importFrom centipede no_blank
#' @importFrom secretary typewrite_error
#' @import RMySQL
#' @param sqlPath path to source SQL file
#' @param ... Parameter values, if any, that are passed to the SqlRender::render function.
#' @export

executeSQL2 <-
        function(sqlPaths,
                 ...,
                 conn) {

                sql_statement <-
                        SqlRender::render(SqlRender::readSql(sourceFile = sqlPaths),
                                          ...)

                # sql_statement <-
                #         SqlRender::translate(sql = sql_statement,
                #                              targetDialect = "oracle")


                sql_statements <<-
                        sqlPaths %>%
                        purrr::map(function(x)
                                        SqlRender::render(SqlRender::readSql(sourceFile = x),
                                                          ...))



                total <- length(sql_statements)

                pb <- progress::progress_bar$new(
                        format = "[:bar] :elapsedfull :current/:total (:percent)",
                        total = total,
                        width = 80,
                        clear = FALSE)

                pb$tick(0)
                # Sleep time required to allow for progress bar to update after each pb$tick
                Sys.sleep(0.2)

                errors <- vector()
                for (i in 1:length(sql_statements)) {
                        sql <- sql_statements[[i]]

                        sql2 <<- sql

                        res <-
                                tryCatch(RMySQL::dbSendQuery(conn = conn,
                                                             statement = sql),
                                         error = function(e) "Error")

                        if (class(res) != "MySQLResult") {
                                #secretary::typewrite_error("\n\n", sql, "\n")
                                errors <-
                                        c(errors,
                                          sql)
                        }

                        tryCatch(
                                output[[length(output)+1]] <-
                                        RMySQL::dbGetStatement(res),
                                error = function(e) "Error")

                        tryCatch(
                                RMySQL::dbClearResult(res),
                                error = function(e) "Error")

                        Sys.sleep(0.2)

                        pb$tick()
                        # Sleep time required to allow for progress bar to update after each pb$tick
                        Sys.sleep(0.2)
                }

                if (length(errors)) {
                        secretary::typewrite("Errors:")
                        errors %>%
                                purrr::map(~secretary::typewrite(., tabs = 1))

                } else {
                        secretary::typewrite("No Errors.")
                }
        }
