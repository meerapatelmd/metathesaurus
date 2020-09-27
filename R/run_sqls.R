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

run_sqls <-
        function(sqls,
                 ...,
                 conn) {

                total <- length(sqls)
                pb <- progress::progress_bar$new(
                        format = "[:bar] :elapsedfull :current/:total (:percent)",
                        total = total,
                        width = 80,
                        clear = FALSE)

                pb$tick(0)
                # Sleep time required to allow for progress bar to update after each pb$tick
                Sys.sleep(0.2)

                for (i in 1:length(sqls)) {
                        sql <- sqls[i]

                        res <-
                                tryCatch(RMySQL::dbSendQuery(conn = conn,
                                                             statement = sql),
                                         error = function(e) "Error")

                        if (class(res) != "MySQLResult") {
                                secretary::typewrite_error("\n\n", sql, "\n")
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
        }
