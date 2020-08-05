#' Load META Tables
#' @param path Path to the unpacked RRF files
#' @import preQL
#' @importFrom DatabaseConnector dbExecute
#' @export

loadMeta <-
        function(path,
                 dbname = "umls",
                 username,
                 password) {

                conn <-
                preQL::connectMySQL5.5(dbname = dbname,
                                       username = username,
                                       password = password)

                sqlPath <- paste0(system.file(package = "setupMetathesaurus"), "/sql/mysql_meta_tables.sql")

                sql_statement <-
                        SqlRender::render(SqlRender::readSql(sourceFile = sqlPath))

                sql_statement <-
                        SqlRender::translate(sql = sql_statement,
                                             targetDialect = "oracle")

                print(sql_statement)

                DatabaseConnector::dbExecute(conn = conn,
                              statement = sql_statement)

                preQL::dcMySQL5.5(conn = conn)

        }
