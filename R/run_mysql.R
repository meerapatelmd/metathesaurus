#' Execute SQL
#' @importFrom magrittr %>%
#' @import SqlRender
#' @importFrom centipede strsplit
#' @importFrom centipede no_blank
#' @importFrom secretary typewrite_error
#' @import RMySQL
#' @param sqlPath path to source SQL file
#' @param ... Parameter values, if any, that are passed to the SqlRender::render function.
#' @export

run_mysql <-
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

                output <- list()
                errors <- vector()
                for (i in 1:length(sqls)) {

                        pb$tick()
                        # Sleep time required to allow for progress bar to update after each pb$tick
                        Sys.sleep(0.2)

                        sql <- sqls[[i]]


                        res <-
                                tryCatch(RMySQL::dbSendQuery(conn = conn,
                                                             statement = sql),
                                         error = function(e) "Error")

                        if (class(res) != "MySQLResult") {

                                secretary::typewrite_error("\n\n", sql, "\n")
                                errors <-
                                        c(errors, sql)
                        }

                        tryCatch(
                                output[[length(output)+1]] <-
                                        RMySQL::dbGetStatement(res),
                                error = function(e) "Error")

                        tryCatch(
                                RMySQL::dbClearResult(res),
                                error = function(e) "Error")

                }

                list(Errors = errors,
                     Output = output)
        }
