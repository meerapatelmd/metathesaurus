#' @title
#' Instantiate Postgres
#' @inherit setup description
#' @inheritParams pkg_args
#' @rdname run_setup
#' @family setup
#' @export
#' @importFrom pg13 lsSchema send lsTables dropTable
#' @importFrom tibble as_tibble_col
#' @importFrom dplyr mutate select distinct filter
#' @importFrom stringr str_remove_all
#' @importFrom progress progress_bar
#' @importFrom glue glue


run_setup <-
  function(conn,
           conn_fun = "pg13::local_connect()",
           schema = "umls",
           path_to_rrfs,
           steps = c(
             "reset_schema",
             "ddl_tables",
             "copy_rrfs",
             "add_indexes",
             "log",
             "setup_crosswalk"),
           postprocess = TRUE,
           mrconso_only = FALSE,
           omop_only = FALSE,
           english_only = TRUE,
           log_schema = "public",
           log_table_name = "setup_umls_log",
           log_version,
           log_release_date,
           verbose = TRUE,
           render_sql = TRUE,
           render_only = FALSE) {


    if (missing(log_version) | missing(log_release_date)) {
      stop("`log_version` and `log_release_date` are required.")
    }

    if (missing(conn)) {
      conn <- eval(rlang::parse_expr(conn_fun))
      on.exit(expr = pg13::dc(conn = conn), add = TRUE,
              after = TRUE)
    }


    path_to_rrfs <- normalizePath(
      path = path_to_rrfs,
      mustWork = TRUE
    )

    ##### Setup Objects
    errors <- vector()

    tables <-
      c(
        "AMBIGLUI",
        "AMBIGSUI",
        "DELETEDCUI",
        "DELETEDLUI",
        "DELETEDSUI",
        "MERGEDCUI",
        "MERGEDLUI",
        "MRAUI",
        "MRCOLS",
        "MRCONSO",
        "MRCUI",
        "MRCXT",
        "MRDEF",
        "MRDOC",
        "MRFILES",
        "MRHIER",
        "MRHIST",
        "MRMAP",
        "MRRANK",
        "MRREL",
        "MRSAB",
        "MRSAT",
        "MRSMAP",
        "MRSTY",
        "MRXNS_ENG",
        "MRXNW_ENG",
        "MRXW_BAQ",
        "MRXW_CHI",
        "MRXW_CZE",
        "MRXW_DAN",
        "MRXW_DUT",
        "MRXW_ENG",
        "MRXW_EST",
        "MRXW_FIN",
        "MRXW_FRE",
        "MRXW_GER",
        "MRXW_GRE",
        "MRXW_HEB",
        "MRXW_HUN",
        "MRXW_ITA",
        "MRXW_JPN",
        "MRXW_KOR",
        "MRXW_LAV",
        "MRXW_NOR",
        "MRXW_POL",
        "MRXW_POR",
        "MRXW_RUS",
        "MRXW_SCR",
        "MRXW_SPA",
        "MRXW_SWE",
        "MRXW_TUR"
      )

    if (mrconso_only) {
      tables <- "MRCONSO"
    }

    if (omop_only) {
      tables <-
        c(
          'MRCONSO',
          'MRHIER',
          'MRMAP',
          'MRSMAP',
          'MRSAT',
          'MRREL'
        )
    }

    if (english_only) {
      tables <-
        tables[!(tables %in%  c(
                              'MRXW_BAQ',
                              'MRXW_CHI',
                              'MRXW_CZE',
                              'MRXW_DAN',
                              'MRXW_DUT',
                              'MRXW_EST',
                              'MRXW_FIN',
                              'MRXW_FRE',
                              'MRXW_GER',
                              'MRXW_GRE',
                              'MRXW_HEB',
                              'MRXW_HUN',
                              'MRXW_ITA',
                              'MRXW_JPN',
                              'MRXW_KOR',
                              'MRXW_LAV',
                              'MRXW_NOR',
                              'MRXW_POL',
                              'MRXW_POR',
                              'MRXW_RUS',
                              'MRXW_SCR',
                              'MRXW_SPA',
                              'MRXW_SWE',
                              'MRXW_TUR'
                            ))]

    }

    if ("reset_schema" %in% steps) {
      reset_schema(
        conn = conn,
        schema = schema,
        verbose = verbose,
        render_sql = render_sql
      )
    }


    if ("ddl_tables" %in% steps) {
      ddl_tables(
        conn = conn,
        schema = schema,
        tables = tables,
        verbose = verbose,
        render_sql = render_sql
      )
    }

    if ("copy_rrfs" %in% steps) {
      copy_rrfs(
        path_to_rrfs = path_to_rrfs,
        tables = tables,
        conn = conn,
        schema = schema,
        verbose = verbose,
        render_sql = render_sql
      )
    }


    if ("add_indexes" %in% steps) {
      add_indexes(
        conn = conn,
        schema = schema,
        verbose = verbose,
        render_sql = render_sql
      )
    }

    # Log
    if ("log" %in% steps) {

      sql_statement <-
        glue::glue(
      "
      CREATE TABLE IF NOT EXISTS {log_schema}.{log_table_name} (
          sm_datetime timestamp without time zone NOT NULL,
          sm_version varchar(100) NOT NULL,
          sm_release_date varchar(25) NOT NULL,
          sm_schema varchar(25) NOT NULL,
          ambiglui int,
          ambigsui int,
          deletedcui int,
          deletedlui int,
          deletedsui int,
          mergedcui int,
          mergedlui int,
          mraui int,
          mrcols int,
          mrconso int,
          mrcui int,
          mrcxt int,
          mrdef int,
          mrdoc int,
          mrfiles int,
          mrhier int,
          mrhist int,
          mrmap int,
          mrrank int,
          mrrel int,
          mrsab int,
          mrsat int,
          mrsmap int,
          mrsty int,
          mrxns_eng int,
          mrxnw_eng int,
          mrxw_eng int
      );
      ")

      pg13::send(conn = conn,
                 sql_statement = sql_statement)


      table_names <-
        pg13::ls_tables(
          conn = conn,
          schema = schema,
          verbose = verbose,
          render_sql = render_sql
        )

      current_row_count <-
        table_names %>%
        purrr::map(function(x) {
          pg13::query(
            conn = conn,
            sql_statement = pg13::render_row_count(
              schema = schema,
              tableName = x
            )
          )
        }) %>%
        purrr::set_names(tolower(table_names)) %>%
        dplyr::bind_rows(.id = "Table") %>%
        dplyr::rename(Rows = count) %>%
        tidyr::pivot_wider(
          names_from = "Table",
          values_from = "Rows"
        ) %>%
        dplyr::mutate(
          sm_datetime = as.character(Sys.time()),
          sm_version = log_version,
          sm_release_date = log_release_date,
          sm_schema = schema
        ) %>%
        dplyr::select(
          sm_datetime,
          sm_version,
          sm_release_date,
          sm_schema,
          dplyr::all_of(c(
            'ambiglui',
            'ambigsui',
            'deletedcui',
            'deletedlui',
            'deletedsui',
            'mergedcui',
            'mergedlui',
            'mraui',
            'mrcols',
            'mrconso',
            'mrcui',
            'mrcxt',
            'mrdef',
            'mrdoc',
            'mrfiles',
            'mrhier',
            'mrhist',
            'mrmap',
            'mrrank',
            'mrrel',
            'mrsab',
            'mrsat',
            'mrsmap',
            'mrsty',
            'mrxns_eng',
            'mrxnw_eng',
            'mrxw_eng'
          ))
        )


      current_row_count <-
        unname(unlist(current_row_count))

      sql_statement <-
        glue::glue(
          "
          INSERT INTO {log_schema}.{log_table_name}
          VALUES ({glue::glue_collapse(glue::single_quote(current_row_count), sep = ', ')})
          ;
          "
        )

      pg13::send(conn = conn,
                 sql_statement = sql_statement)


      cli::cat_line()
      cli::cat_boxx("Log Results",
        float = "center"
      )

      sql_statement <-
        glue::glue(
          "
          SELECT *
          FROM {log_schema}.{log_table_name}
          WHERE sm_datetime IN (SELECT MAX(sm_datetime) FROM {log_schema}.{log_table_name});
          "
        )

      updated_log <-
      pg13::query(
        conn = conn,
        sql_statement = sql_statement,
        checks = ''
      )


      print(tibble::as_tibble(updated_log))
      cli::cat_line()
    }

    if ("setup_crosswalk" %in% steps) {

      setup_crosswalk_schema(
        conn = conn,
        crosswalk_schema = "umls_crosswalk"
      )

    }

    if (postprocess) {

      run_postprocessing(
        conn = conn,
        umls_version = log_version,
        umls_release_dt = log_release_date,
        verbose = verbose,
        render_sql = render_sql,
        render_only = render_only
      )

    }


  }

#' @title
#' Run Postprocessing Only
#' @rdname run_postprocessing
#' @export
#' @importFrom rlang parse_expr
#' @importFrom pg13 dc send

run_postprocessing <-
  function(conn,
           conn_fun = "pg13::local_connect()",
           umls_version,
           umls_release_dt,
           verbose = TRUE,
           render_sql = TRUE,
           render_only = FALSE) {

    if (missing(umls_version)|missing(umls_release_dt)) {
      stop("`umls_version` and `umls_release_dt` are required.")
    }


    if (missing(conn)) {
      conn <- eval(rlang::parse_expr(conn_fun))
      on.exit(expr = pg13::dc(conn = conn), add = TRUE,
              after = TRUE)
    }

    postprocess_sqls <-
      list.files(system.file(package = "metathesaurus",
                             "sql",
                             "Hierarchy"),
                 full.names = TRUE,
                 pattern = "[.]{1}sql$")

    for (postprocess_sql in postprocess_sqls) {

      sql_statement <-
        paste(readLines(con = postprocess_sql),
              collapse = "\n")


      sql_statement <-
        glue::glue(sql_statement)


      pg13::send(
        conn = conn,
        sql_statement = sql_statement,
        verbose = verbose,
        render_sql = render_sql,
        checks = ''
      )


    }
  }
