#' DDL META Tables
#' @param path Path to the unpacked RRF files
#' @import preQL
#' @importFrom DatabaseConnector dbExecute
#' @export

# conn <-
#         preQL::connectMySQL5.5(dbname = "umls",
#                                username = Sys.getenv("umls_username"),
#                                password = Sys.getenv("umls_password"))
# RMySQL::dbSendQuery(conn = conn,
#                     statement = "DROP TABLE IF EXISTS MRCONSO;")
# RMySQL::dbSendQuery(conn = conn,
#                     statement = "CREATE TABLE MRCONSO
# (
#   CUI       CHAR(8),
#   LAT       CHAR(3),
#   TS        CHAR(1),
#   LUI       VARCHAR(10),
#   STT       VARCHAR(3),
#   SUI       VARCHAR(10),
#   ISPREF    VARCHAR(1),
#   AUI       VARCHAR(9) NOT NULL,
#   SAUI      VARCHAR(50),
#   SCUI      VARCHAR(100),
#   SDUI      VARCHAR(100),
#   SAB       VARCHAR(40),
#   TTY       VARCHAR(40),
#   CODE      VARCHAR(100),
#   STR       VARCHAR(3000),
#   SRL       INT ,
#   SUPPRESS  VARCHAR(1),
#   CVF       INT,
#   FILLER_COLUMN INT
# );")


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

                print(sql_statement)

                RMySQL::dbSendQuery(conn = conn,
                              statement = sql_statement)

                preQL::dcMySQL5.5(conn = conn)

        }
