#' Load META Tables
#' @param path Path to the unpacked RRF files
#' @import preQL
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

                pg13::execute(conn = conn,
                              sql_statement = sql_statement)

                preQL::dcMySQL5.5(conn = conn)

        }
