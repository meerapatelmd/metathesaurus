#' Load META Tables
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
