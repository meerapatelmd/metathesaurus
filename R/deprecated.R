#' (Deprecated) DDL META Tables
#' @param path Path to the unpacked RRF files
#' @import preQL
#' @importFrom DatabaseConnector dbExecute
#' @export


copyPg <-
        function(conn,
                 csvFile) {

                .Deprecated()


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





#' (Deprecated) DDL META Tables
#' @import preQL
#' @importFrom DatabaseConnector dbExecute
#' @keywords internal
#' @export


ddlMeta <-
        function(dbname = "umls",
                 username,
                 password,
                 full = FALSE) {


                .Deprecated("mysql_ddl")

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





#' (Deprecated) Execute SQL
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
#' @keywords internal

executeSQL <-
        function(sqlPath,
                 ...,
                 conn) {

                .Deprecated()
                sql_statement <-
                        SqlRender::render(SqlRender::readSql(sourceFile = sqlPath),
                                          ...)

                # sql_statement <-
                #         SqlRender::translate(sql = sql_statement,
                #                              targetDialect = "oracle")


                sql_statement <-
                        centipede::strsplit(sql_statement, split = "[;]{1}", type = "after") %>%
                        unlist() %>%
                        trimws() %>%
                        centipede::no_blank()

                total <- length(sql_statement)
                pb <- progress::progress_bar$new(
                        format = "[:bar] :elapsedfull :current/:total (:percent)",
                        total = total,
                        width = 80,
                        clear = FALSE)

                pb$tick(0)
                # Sleep time required to allow for progress bar to update after each pb$tick
                Sys.sleep(0.2)

                for (i in 1:length(sql_statement)) {
                        sql <- sql_statement[i]


                        sql1 <<- sql
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





#' (Deprecated) Load META Tables
#' @param path Path to the unpacked RRF files
#' @import preQL
#' @importFrom DatabaseConnector dbExecute
#' @export

loadMeta <-
        function(path,
                 dbname = "umls",
                 username,
                 password,
                 full = FALSE) {
                .Deprecated()

                if (grepl("[/]{1}$", path)) {
                        stop("'path' value ", path, " cannot end with slash")
                }

                filePath <- path.expand(path = path)


                # ddlMeta(dbname = dbname,
                #         username = username,
                #         password = password)


                conn <-
                preQL::connectMySQL5.5(dbname = dbname,
                                       username = username,
                                       password = password)


                if (full) {
                        sqlPath <- paste0(system.file(package = "setupMetathesaurus"), "/sql/full_load.sql")

                } else {
                sqlPath <- paste0(system.file(package = "setupMetathesaurus"), "/sql/load.sql")
                }

                executeSQL(sqlPath = sqlPath,
                           filePath = filePath,
                           conn = conn)

                # sql_statement <-
                #         SqlRender::render(SqlRender::readSql(sourceFile = sqlPath),
                #                           filePath = path)
                #
                # sql_statement <-
                #         SqlRender::translate(sql = sql_statement,
                #                              targetDialect = "oracle")
                #
                #
                #
                # sql_statement <-
                #         centipede::strsplit(sql_statement, split = "[;]{1}", type = "after") %>%
                #         unlist() %>%
                #         trimws() %>%
                #         centipede::no_blank()
                #
                #
                # while (length(sql_statement) > 0) {
                #         sql <- sql_statement[1]
                #
                #         res <-
                #                 tryCatch(RMySQL::dbSendQuery(conn = conn,
                #                                              statement = sql),
                #                          error = function(e) "Error")
                #
                #         if (class(res) != "MySQLResult") {
                #                 secretary::typewrite_error(sql)
                #         }
                #
                #         tryCatch(
                #                 output[[length(output)+1]] <-
                #                         RMySQL::dbGetStatement(res),
                #                 error = function(e) "Error")
                #
                #         tryCatch(
                #                 RMySQL::dbClearResult(res),
                #                 error = function(e) "Error")
                #
                #
                #         sql_statement <- sql_statement[-1]
                # }

                #########
                # sqlPath <- paste0(system.file(package = "setupMetathesaurus"), "/sql/indices.sql")
                #
                # sql_statement <-
                #         SqlRender::render(SqlRender::readSql(sourceFile = sqlPath))
                #
                # sql_statement <-
                #         SqlRender::translate(sql = sql_statement,
                #                              targetDialect = "oracle")
                #
                # print(sql_statement)
                #
                # RMySQL::dbSendQuery(conn = conn,
                #                     statement = sql_statement)
                #
                #
                preQL::dcMySQL5.5(conn = conn)

        }





#' @title
#' (Deprecated) RRF to CSV
#' @seealso
#'  \code{\link[progress]{progress_bar}}
#'  \code{\link[cave]{strip_fn}}
#'  \code{\link[readr]{read_delim}}
#'  \code{\link[rubix]{rm_multibyte_chars}}
#'  \code{\link[broca]{simply_write_csv}}
#' @rdname rrfToCsv
#' @export
#' @importFrom progress progress_bar
#' @importFrom cave strip_fn
#' @importFrom readr read_delim
#' @importFrom rubix rm_multibyte_chars
#' @importFrom broca simply_write_csv
#' @importFrom magrittr %>%

rrfToCsv <-
        function(path) {
                .Deprecated()
                rrfFiles <- list.files(path = path, pattern = "RRF$", full.names = T)
                rrfFiles <-
                        c(grep(pattern = "[_]{1}", rrfFiles, invert = TRUE, value= TRUE),
                          grep(pattern = "ENG", rrfFiles, value= TRUE))


                pb <- progress::progress_bar$new(format = ":percent [:bar] :elapsedfull :current/:total",
                                                 total = length(rrfFiles))

                pb$tick(0)
                Sys.sleep(0.2)

                for (i in 1:length(rrfFiles)) {
                        rrfFile <- rrfFiles[i]

                        csvFile <- paste0(cave::strip_fn(rrfFile), ".csv")

                        if (!file.exists(csvFile)) {
                                        rrf <- readr::read_delim(rrfFile,
                                                                 quote = "",
                                                                 delim = "|",
                                                                 col_names = FALSE,
                                                                 col_types = cols(.default = "c"))

                                        rrf <-
                                                rrf %>%
                                                rubix::rm_multibyte_chars() %>%
                                                dplyr::mutate_all(trimws, which = "both")

                                        broca::simply_write_csv(rrf,
                                                                file = csvFile)
                        }

                        cat("\n")
                        pb$tick()
                        Sys.sleep(0.2)
                }
        }





#' (Deprecated) Run Indices
#' @description Run indices on the loaded data.
#' @param path Path to the unpacked RRF files
#' @keywords internal

runIndices <-
        function(dbname = "umls",
                 username,
                 password,
                 full = FALSE) {

                .Deprecated()

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





#' (Deprecated) DDL META Tables
#' @param path Path to the unpacked RRF files
#' @import preQL
#' @importFrom DatabaseConnector dbExecute
#' @export


testCopy <-
        function(conn) {

                .Deprecated()

                        sqlPath <- "inst/sql/pgtest.sql"

                        sql <- SqlRender::render(SqlRender::readSql(sourceFile = sqlPath),
                                                 wd = getwd())

                        #print(sql)
                        #secretary::press_enter()
                        pg13::send(conn = conn,
                                   sql_statement = sql)


        }





