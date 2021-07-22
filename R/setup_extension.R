#' @title
#' Write a SAB Subset
#'
#' @description
#' Filter the MRCONSO table for a given vocabulary and the STR
#' value designated as the main LABEL, which is an added field
#' in the table. This function assumes that the core metathesaurus
#' tables are within a `mth` schema.
#'
#' @param sab Vocabulary.
#' @param extension_schema Schema to which these extension tables are to be written.
#' @param tty_rank Vector of tty values for the given sab in the order of rank. If
#' not provided, the function defaults to the rank given to the sab in the MRRANK
#' table in descending order.
#' @return
#' A table named by `sab` is written to the `extension_schema`. If `sab`
#' value contained any punctuation, it is replaced with an underscore.
#'
#' @seealso
#'  \code{\link[stringr]{str_replace}}
#'  \code{\link[SqlRender]{render}}
#'  \code{\link[pg13]{send}},\code{\link[pg13]{query}}
#' @rdname write_mrconso_sab_table
#' @export
#' @importFrom stringr str_replace_all
#' @importFrom SqlRender render
#' @importFrom pg13 send query
#' @importFrom dplyr transmute
#' @importFrom tibble tibble
write_mrconso_sab_table <-
        function(sab,
                 extension_schema,
                 conn_fun = "pg13::local_connect(verbose = FALSE)",
                 tty_rank,
                 verbose = TRUE,
                 render_sql = TRUE) {

                sab_table <- stringr::str_replace_all(sab,
                                                      pattern = "[[:punct:]]",
                                                      replacement = "_")

                ddl_statement <-
                        SqlRender::render(
                                "
                                  DROP TABLE IF EXISTS @extension_schema.@table;
                                  CREATE TABLE @extension_schema.@table (
                                          CUI	char(8) NOT NULL,
                                          LAT	char(3) NOT NULL,
                                          TS	char(1) NOT NULL,
                                          LUI	varchar(10) NOT NULL,
                                          STT	varchar(3) NOT NULL,
                                          SUI	varchar(10) NOT NULL,
                                          ISPREF	char(1) NOT NULL,
                                          AUI	varchar(9) NOT NULL,
                                          SAUI	varchar(50),
                                          SCUI	varchar(100),
                                          SDUI	varchar(100),
                                          SAB	varchar(40) NOT NULL,
                                          TTY	varchar(40) NOT NULL,
                                          CODE	varchar(100) NOT NULL,
                                          STR	text NOT NULL,
                                          SRL	integer NOT NULL,
                                          SUPPRESS	char(1) NOT NULL,
                                          CVF	integer,
                                          FILLER_COL INTEGER,
                                          LABEL text NOT NULL
                                  )",
                                extension_schema = extension_schema,
                                table = sab_table
                        )

                pg13::send(
                        conn_fun = conn_fun,
                        sql_statement = ddl_statement,
                        verbose = verbose,
                        render_sql = render_sql
                )

                if (missing(tty_rank)) {
                        fct_vector <-
                                pg13::query(conn_fun = conn_fun,
                                            sql_statement =
                                                    SqlRender::render(
                                                    "SELECT DISTINCT tty,mrrank_rank
                                                     FROM mth.MRRANK
                                                     WHERE sab = '@sab' AND suppress = 'N'
                                                    ORDER BY mrrank_rank DESC",
                                                    sab = sab
                                                    ),
                                            verbose = verbose,
                                            render_sql = render_sql
                                            )

                        print(fct_vector)

                        fct_vector <- fct_vector$tty

                } else {
                       fct_vector <-  tty_rank
                }


                field <- "tty"
                fct_vector_index <- 1:length(fct_vector)
                fct_vector_index_last <- length(fct_vector)+1

                df <-
                        tibble::tibble(fct_vector,
                               fct_vector_index) %>%
                        dplyr::transmute(field = field,
                                  fct_vector,
                                  fct_vector_index)


                sql_statement <-
                        c("WITH a AS (",
                          "SELECT *,",
                          "  CASE  ",
                          mapply(sprintf,
                                 "    WHEN %s = '%s' THEN %s",
                                 df$field,
                                 df$fct_vector,
                                 df$fct_vector_index,
                                 USE.NAMES = F),
                          sprintf("    ELSE %s", fct_vector_index_last),
                          sprintf("  END tty_rank"),
                          "FROM mth.mrconso",
                          "WHERE sab = '@sab'",
                          "AND lat = 'ENG'",
                          "),",
                          "b AS (",
                          " SELECT code,min(tty_rank) AS min_tty_rank",
                          " FROM a",
                          " GROUP BY code",
                          "),",
                          "c AS (",
                          "  SELECT ",
                          "    a.*,",
                          "    ROW_NUMBER() OVER(PARTITION BY a.code ORDER BY a.str DESC) AS rank",
                          "  FROM a",
                          "  INNER JOIN b",
                          "  ON a.code = b.code AND a.tty_rank = b.min_tty_rank",
                          ")",
                          "",
                          "",
                          "INSERT INTO @extension_schema.@table ",
                          "  SELECT",
                          "     a.cui,",
                          "     a.lat,",
                          "     a.ts,",
                          "     a.lui,",
                          "     a.stt,",
                          "     a.sui,",
                          "     a.ispref,",
                          "     a.aui,",
                          "     a.saui,",
                          "     a.scui,",
                          "     a.sdui,",
                          "     a.sab,",
                          "     a.tty,",
                          "     a.code,",
                          "     c.str,",
                          "     a.srl,",
                          "     a.suppress,",
                          "     a.cvf,",
                          "     a.filler_col,",
                          "     a.str AS label",
                          "  FROM a ",
                          "  FULL JOIN c ",
                          "  ON ",
                          "    a.cui = c.cui AND",
                          "    a.sab = c.sab AND",
                          "    a.tty = c.tty AND",
                          "    a.code = c.code AND",
                          "    a.str = c.str",
                          "  WHERE c.rank = 1;"
                        )

                sql_statement <-
                        paste(sql_statement,
                              collapse = "\n")

                sql_statement <-
                        SqlRender::render(sql_statement,
                                          sab = sab,
                                          extension_schema = extension_schema,
                                          table = sab_table)

                pg13::send(
                        conn_fun = conn_fun,
                        sql_statement = sql_statement,
                        verbose = verbose,
                        render_sql = render_sql
                )
        }


#' @title
#' Write More Than 1 SAB Subset at Once
#'
#' @description
#' Write all or some of the SAB subset tables via
#' `write_sab_subset_table()`.
#'
#' @param sabs (optional) Vector of SABs to write. If missing,
#' all the sabs in the existing MRCONSO table are written.
#' @param extension_schema Schema to write these extension tables to. Default: 'mrconso_sab'
#' @return
#' One or more tables of the sab subsets to the given schema. A new 'label'
#' field is added to designate the core concept name for the given
#' concept.

#' @seealso
#'  \code{\link[pg13]{drop_cascade}},\code{\link[pg13]{create_schema}}
#' @rdname setup_mrconso_sab_subsets
#' @export
#' @importFrom pg13 drop_cascade create_schema query
setup_mrconso_sab_subsets <-
        function(sabs,
                 conn_fun = "pg13::local_connect()",
                 extension_schema = "mrconso_sab") {

        pg13::drop_cascade(conn_fun = conn_fun,
                           schema = extension_schema)

        pg13::create_schema(conn_fun = conn_fun,
                            schema = extension_schema)

        if (missing(sabs)) {

                sabs <-
                        pg13::query(conn_fun = conn_fun,
                                    sql_statement = "SELECT DISTINCT sab FROM mth.MRCONSO ORDER BY sab") %>% unlist() %>% unname()

        }

        for (sab in sabs) {
               write_mrconso_sab_table(sab = sab,
                                        extension_schema = extension_schema)
        }

}
