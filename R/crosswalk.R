#' @title
#' Write a Crosswalk Table
#'
#' @description
#' Subset the MRCONSO table by vocabulary and reduce the rows
#' to 1 `str` per `code`. All original fields from the MRCONSO
#' table are preserved. Crosswalks can then be performed by
#' `cui` between vocabularies. This function assumes that the core metathesaurus
#' tables are within a `umls` schema. Note that any source `sab`
#' value that contains punctuation other than an underscore
#' is replaced with an underscore to write tables with names that
#' Postgres can accept.
#'
#' @param sab Vocabulary.
#' @param crosswalk_schema Schema to which these extension tables are to be written.
#' @param tty_rank (Optional). Vector of case insensitive tty values for the
#' given sab in the order of rank. Any invalid tty values are skipped and those
#' not included in the tty_rank vector are subranked after. If
#' not provided, the function defaults to the rank given to the sab in the MRRANK
#' table in descending order.
#'
#' @return
#' A table named by `sab` is written to the `crosswalk_schema`. If `sab`
#' value contained any punctuation, it is replaced with an underscore.
#'
#' @rdname write_crosswalk_table
#' @export
#' @importFrom stringr str_replace_all
#' @importFrom pg13 schema_exists create_schema table_exists drop_table send query sQuo read_table write_table
#' @importFrom dplyr n bind_rows
#' @importFrom cli cli_inform
#' @importFrom tibble tibble rowid_to_column tribble
#' @importFrom utils capture.output
#' @importFrom huxtable hux set_caption theme_compact huxtable number_format fmt_pretty
#' @importFrom glue glue

write_crosswalk_table <-
  function(sab,
           crosswalk_schema = "umls_crosswalk",
           conn_fun = "pg13::local_connect(verbose = FALSE)",
           tty_ranking,
           log_schema = "public",
           log_table_name = "setup_mth_log",
           crosswalk_log_table_name = "setup_mth_crosswalk_log",
           verbose = TRUE,
           render_sql = TRUE) {
    sab_table <- stringr::str_replace_all(sab,
      pattern = "[[:punct:]]",
      replacement = "_"
    )

    if (!pg13::schema_exists(conn_fun = conn_fun,
                            schema = crosswalk_schema)) {

            pg13::create_schema(conn_fun = conn_fun,
                                schema = crosswalk_schema)
    }

    if (pg13::table_exists(conn_fun = conn_fun,
                           schema = crosswalk_schema,
                           table_name = sab_table)) {

            pg13::drop_table(
                    conn_fun = conn_fun,
                    schema = crosswalk_schema,
                    table = sab_table,
                    verbose = verbose,
                    render_sql = render_sql
            )
    }

    ddl_statement <-
      render(
        "
          DROP TABLE IF EXISTS {crosswalk_schema}.{table};
          CREATE TABLE {crosswalk_schema}.{table} (
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
                  FILLER_COL INTEGER
                                  )",
        crosswalk_schema = crosswalk_schema,
        table = sab_table
      )

    pg13::send(
      conn_fun = conn_fun,
      sql_statement = ddl_statement,
      verbose = verbose,
      render_sql = render_sql,
      checks = ""
    )

    if (missing(tty_ranking)) {
      tty_ranking_df <-
        pg13::query(
          conn_fun = conn_fun,
          sql_statement =
            render(
           "SELECT DISTINCT tty,mrrank_rank
             FROM umls.MRRANK
             WHERE sab = '{sab}'
            ORDER BY mrrank_rank DESC",
              sab = sab
            ),
          verbose = verbose,
          render_sql = render_sql,
          checks = ""
        ) %>%
              transmute(tty,
                        rank = 1:dplyr::n())


      tty_ranking <- tty_ranking_df$tty

    } else {

  # Implement case insensitivity
  tty_ranking <- toupper(tty_ranking)

  # Checking if user-provider tty are valid
    valid_tty <-
            pg13::query(
                    conn_fun = conn_fun,
                    sql_statement =
                            render(
                                    "SELECT DISTINCT tty
     FROM umls.MRRANK
     WHERE sab = '{sab}' AND tty IN ({tty_ranking})",
                                    sab = sab,
                                    tty_ranking = pg13::sQuo(tty_ranking)
                            ),
                    verbose = verbose,
                    render_sql = render_sql,
                    checks = ""
            ) %>%
            unlist() %>%
            unname()

    if (!all(tty_ranking %in% valid_tty)) {

            invalid_tty <- tty_ranking[!(tty_ranking %in% valid_tty)]
            cli::cli_inform("The following {length(invalid_tty)} invalid `tty` and will be ignored: {invalid_tty}.")

            tty_ranking <-
                    tty_ranking[!(tty_ranking %in% invalid_tty)]
    }



        missing_tty_df <-
        pg13::query(
                conn_fun = conn_fun,
                sql_statement =
                        render(
                                "SELECT DISTINCT tty,mrrank_rank
             FROM umls.MRRANK
             WHERE sab = '{sab}' AND tty NOT IN ({tty_ranking})
            ORDER BY mrrank_rank DESC",
                                sab = sab,
                                tty_ranking = pg13::sQuo(tty_ranking)
                        ),
                verbose = verbose,
                render_sql = render_sql,
                checks = ""
        )

        tty_ranking_df <-
                tibble::tibble(
                        tty = c(tty_ranking, missing_tty_df$tty)
                ) %>%
                tibble::rowid_to_column("rank")



    }


    console.tbl.print <-
    utils::capture.output(
            huxtable::hux(
                    tty_ranking_df,
                    add_colnames = TRUE) %>%
                    huxtable::set_caption(sab) %>%
                    huxtable::theme_compact()
    )
    # Remove erroneous final line that list column names
    console.tbl.print <-
            console.tbl.print[-length(console.tbl.print)]

    typewrite()
    cat(console.tbl.print,
        sep = "\n")


    # Getting bottommost rank integer that will catch
    # only orphaned concepts (unlikely)
    bottom_rank <-
            max(tty_ranking_df$rank)+1



    sql_statement <-
      c(
        "WITH ranked_subset AS (",
        "SELECT *,",
        "  CASE  ",
        glue::glue("WHEN tty = '{tty_ranking_df$tty}' THEN {tty_ranking_df$rank}"),
        glue::glue("  ELSE {bottom_rank}"),
        "  END tty_rank",
        "FROM umls.mrconso",
        "WHERE sab = '{sab}'",
        "AND lat = 'ENG'",
        "),",
        "top_rank AS (",
        " SELECT code,min(tty_rank) AS min_tty_rank",
        " FROM ranked_subset",
        " GROUP BY code",
        "),",
        "top_rank_sans_ties AS (",
        "  SELECT ",
        "    a.*,",
        "    ROW_NUMBER() OVER(PARTITION BY a.code ORDER BY a.ispref DESC) AS rank",
        "  FROM ranked_subset a ",
        "  INNER JOIN top_rank b ",
        "  ON a.code = b.code AND a.tty_rank = b.min_tty_rank",
        ")",
        "",
        "",
        "INSERT INTO {crosswalk_schema}.{table} ",
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
        "     a.filler_col",
        "  FROM ranked_subset a ",
        "  FULL JOIN top_rank_sans_ties c ",
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
        collapse = "\n"
      )

    sql_statement <-
      render(sql_statement,
        sab = sab,
        crosswalk_schema = crosswalk_schema,
        table = sab_table
      )

    pg13::send(
      conn_fun = conn_fun,
      sql_statement = sql_statement,
      verbose = verbose,
      render_sql = render_sql,
      checks = ""
    )


    unique_code_ct <-
    pg13::query(
            conn_fun = conn_fun,
            sql_statement = glue::glue("SELECT COUNT(DISTINCT code) FROM {crosswalk_schema}.{sab_table};"),
            verbose = verbose,
            render_sql = render_sql,
            checks = ""
    )

    original_unique_code_ct <-
            pg13::query(
                    conn_fun = conn_fun,
                    sql_statement = glue::glue("SELECT COUNT(DISTINCT code) FROM umls.mrconso WHERE sab = '{sab}';"),
                    verbose = verbose,
                    render_sql = render_sql,
                    checks = ""
            )

    row_count <-
    pg13::query(
            conn_fun = conn_fun,
            sql_statement = glue::glue("SELECT COUNT(*) FROM {crosswalk_schema}.{sab_table};"),
            verbose = verbose,
            render_sql = render_sql,
            checks = ""
    )


    metrics_df <-
            tibble::tribble(
                    ~Row, ~Count,
                    "Unique codes in MRCONSO:", original_unique_code_ct$count,
                    "Unique codes:", unique_code_ct$count,
                    "Total rows:", row_count$count
            )

    metrics_ht <-
            huxtable::huxtable(
                    metrics_df,
                    add_colnames = FALSE)

    huxtable::number_format(metrics_ht)[,2] <- huxtable::fmt_pretty()

    console.tbl.print2 <-
            utils::capture.output(
            metrics_ht %>%
                    huxtable::set_caption(glue::glue("{crosswalk_schema}.{sab_table} Table")) %>%
                    huxtable::theme_compact()
            )
    console.tbl.print2 <-
            console.tbl.print2[-length(console.tbl.print2)]

    typewrite()
    cat(console.tbl.print2,
        sep = "\n")


    source_metathesarus_version <-
    pg13::query(
            conn_fun = conn_fun,
            sql_statement =
                    glue::glue("SELECT log.sm_version, log.sm_release_date FROM {log_schema}.{log_table_name} log WHERE log.sm_datetime IN (SELECT MAX(sm_datetime) FROM {log_schema}.{log_table_name});"),
            verbose = verbose,
            render_sql = render_sql,
            checks = ""
    )


    this_log_df <-
            tibble::tibble(
                    smc_datetime = Sys.time(),
                    smc_umls_version  = source_metathesarus_version$sm_version,
                    smc_umls_release_dt = source_metathesarus_version$sm_release_date,
                    sab    = sab,
                    crosswalk_schema = crosswalk_schema,
                    crosswalk_table  = sab_table,
                    crosswalk_tty_rank   = paste(tty_ranking_df$tty, collapse = "|"),
                    source_code_ct = original_unique_code_ct$count,
                    table_code_ct = unique_code_ct$count,
                    table_row_ct    = row_count$count

            )


    if (pg13::table_exists(
            conn_fun = conn_fun,
            schema = log_schema,
            table_name = crosswalk_log_table_name
    )) {
            updated_log <-
                    dplyr::bind_rows(
                            pg13::read_table(
                                    conn_fun = conn_fun,
                                    schema = log_schema,
                                    table = crosswalk_log_table_name,
                                    verbose = verbose,
                                    render_sql = render_sql
                            ),
                            this_log_df
                    )
    } else {
            updated_log <- this_log_df
    }

    pg13::drop_table(
            conn_fun = conn_fun,
            schema = log_schema,
            table = crosswalk_log_table_name,
            verbose = verbose,
            render_sql = render_sql
    )

    pg13::write_table(
            conn_fun = conn_fun,
            schema = log_schema,
            table_name = crosswalk_log_table_name,
            data = updated_log,
            verbose = verbose,
            render_sql = render_sql
    )


  }


#' @title
#' Write More Than 1 SAB Subset at Once
#'
#' @description
#' Write all or some of the SAB subset tables via
#' `write_crosswalk_table()`.
#'
#' @param sabs (optional) Vector of SABs to write. If missing,
#' all the sabs in the existing MRCONSO table are written.
#' @param crosswalk_schema Schema to write these extension tables to. Default: 'umls_crosswalk'
#' @return
#' One or more tables of the sab subsets to the given schema. A new 'label'
#' field is added to designate the core concept name for the given
#' concept.

#' @seealso
#'  \code{\link[pg13]{drop_cascade}},\code{\link[pg13]{create_schema}}
#' @rdname setup_crosswalk_Schema
#' @export
#' @importFrom pg13 drop_cascade create_schema query


setup_crosswalk_schema <-
  function(sabs,
           conn_fun = "pg13::local_connect()",
           crosswalk_schema = "umls_crosswalk") {

    pg13::drop_cascade(
      conn_fun = conn_fun,
      schema = crosswalk_schema
    )

    pg13::create_schema(
      conn_fun = conn_fun,
      schema = crosswalk_schema
    )

    if (missing(sabs)) {
      sabs <-
        pg13::query(
          conn_fun = conn_fun,
          sql_statement = "SELECT DISTINCT sab FROM umls.MRCONSO ORDER BY sab"
        ) %>%
        unlist() %>%
        unname()
    }

    for (sab in sabs) {
      write_crosswalk_table(
        sab = sab,
        crosswalk_schema = crosswalk_schema
      )
    }
  }


#' @title
#' Crosswalk between 2 Sabs
#' @description
#' Write a crosswalk to a table or return into
#' the R environment as a dataframe.
#'
#' @param to_schema (Optional). If provided along with a `to_table_name`, will
#' write the crosswalk to Postgres. This is useful for larger crosswalks that
#' would require too much memory in the R environment.
#' @param to_table_name (Optional).
#' @seealso
#'  \code{\link[stringr]{str_replace}}
#'  \code{\link[glue]{glue}}
#'  \code{\link[pg13]{send}},\code{\link[pg13]{c("query", "query")}}
#' @rdname crosswalk_sabs
#' @export
#' @importFrom stringr str_replace_all
#' @importFrom glue glue
#' @importFrom pg13 send query

crosswalk_sabs <-
        function(conn,
                 conn_fun = "pg13::local_connect()",
                 sab1,
                 sab2,
                 to_schema,
                 to_table_name,
                 crosswalk_schema = "umls_crosswalk",
                 verbose = TRUE,
                 render_sql = TRUE) {


                sab1_table <- stringr::str_replace_all(sab1,
                                                      pattern = "[[:punct:]]",
                                                      replacement = "_"
                )

                sab2_table <- stringr::str_replace_all(sab2,
                                                       pattern = "[[:punct:]]",
                                                       replacement = "_"
                )

                mrconso_fields <-
                        c('cui', 'lat', 'ts', 'lui', 'stt', 'sui', 'ispref', 'aui', 'saui', 'scui', 'sdui', 'sab', 'tty', 'code', 'str', 'srl', 'suppress', 'cvf', 'filler_col')



                sql_statement <-
                paste(
                c(
                "  SELECT DISTINCT",
                                paste(
                                as.character(
                                  c(
                glue::glue("    sab1.{mrconso_fields} AS {sab1_table}_{mrconso_fields}"),
                glue::glue("    sab2.{mrconso_fields} AS {sab2_table}_{mrconso_fields}"))),
                                collapse = ",\n"),
                glue::glue("  FROM {crosswalk_schema}.{sab1_table} sab1"),
                glue::glue("  FULL JOIN {crosswalk_schema}.{sab2_table} sab2"),
                "  ON sab1.cui = sab2.cui"),
                collapse = "\n"
                )

                if (!missing(to_schema) & !missing(to_table_name)) {

                  create_sql_statement <-
                    paste(
                      as.character(
                      glue::glue(
                        "DROP TABLE IF EXISTS {to_schema}.{to_table_name};\n",
                        "CREATE TABLE {to_schema}.{to_table_name} AS (\n",
                        "{sql_statement}",
                        "\n);"
                      )),
                      collapse = "\n"
                    )

                  pg13::send(
                    conn_fun = conn_fun,
                    sql_statement = create_sql_statement
                  )


                } else {


                pg13::query(
                        conn_fun = conn_fun,
                        sql_statement = sql_statement,
                        verbose = verbose,
                        render_sql = render_sql
                )

                }



        }
