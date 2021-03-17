#' @title
#' Instantiate Postgres
#' @inherit setup description
#' @inheritParams setup
#' @seealso
#'  \code{\link[pg13]{lsSchema}},\code{\link[pg13]{send}},\code{\link[pg13]{lsTables}},\code{\link[pg13]{dropTable}}
#'  \code{\link[SqlRender]{render}}
#'  \code{\link[tibble]{as_tibble}}
#'  \code{\link[dplyr]{mutate}},\code{\link[dplyr]{select}},\code{\link[dplyr]{distinct}}
#'  \code{\link[stringr]{str_remove}}
#'  \code{\link[progress]{progress_bar}}
#' @rdname setup_pg_mth
#' @family setup
#' @export
#' @importFrom pg13 lsSchema send lsTables dropTable
#' @importFrom SqlRender render
#' @importFrom tibble as_tibble_col
#' @importFrom dplyr mutate select distinct filter
#' @importFrom stringr str_remove_all
#' @importFrom progress progress_bar


setup_pg_mth <-
        function(conn,
                 schema = "mth",
                 path_to_rrfs,
                 steps = c("reset_schema",
                           "ddl_tables",
                           "copy_rrfs",
                           "add_indexes",
                           "log"),
                 mrconso_only = FALSE,
                 omop_only = FALSE,
                 english_only = TRUE,
                 log_schema = "public",
                 log_table_name = "setup_mth_log",
                 log_version,
                 log_release_date,
                 verbose = TRUE,
                 render_sql = TRUE,
                 render_only = FALSE) {

                if (missing(log_version)|missing(log_release_date)) {
                        stop("`log_version` and `log_release_date` are required.")
                }


                path_to_rrfs <- normalizePath(path = path_to_rrfs,
                                              mustWork = TRUE)

                ##### Setup Objects
                errors <- vector()

                tables <-
                        c('AMBIGLUI',
                          'AMBIGSUI',
                          'DELETEDCUI',
                          'DELETEDLUI',
                          'DELETEDSUI',
                          'MERGEDCUI',
                          'MERGEDLUI',
                          'MRAUI',
                          'MRCOLS',
                          'MRCONSO',
                          'MRCUI',
                          'MRCXT',
                          'MRDEF',
                          'MRDOC',
                          'MRFILES',
                          'MRHIER',
                          'MRHIST',
                          'MRMAP',
                          'MRRANK',
                          'MRREL',
                          'MRSAB',
                          'MRSAT',
                          'MRSMAP',
                          'MRSTY',
                          'MRXNS_ENG',
                          'MRXNW_ENG',
                          'MRXW_BAQ',
                          'MRXW_CHI',
                          'MRXW_CZE',
                          'MRXW_DAN',
                          'MRXW_DUT',
                          'MRXW_ENG',
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
                          'MRXW_TUR')

                if (mrconso_only) {
                        tables <- "MRCONSO"
                }

                if (omop_only) {
                        tables <- c("MRCONSO", "MRHIER","MRMAP","MRSMAP", "MRSAT","MRREL")
                }

                if (english_only) {

                        tables <- tables[!(tables %in% c('MRXW_BAQ', 'MRXW_CHI', 'MRXW_CZE', 'MRXW_DAN', 'MRXW_DUT', 'MRXW_EST', 'MRXW_FIN', 'MRXW_FRE', 'MRXW_GER', 'MRXW_GRE', 'MRXW_HEB', 'MRXW_HUN', 'MRXW_ITA', 'MRXW_JPN', 'MRXW_KOR', 'MRXW_LAV', 'MRXW_NOR', 'MRXW_POL', 'MRXW_POR', 'MRXW_RUS', 'MRXW_SCR', 'MRXW_SPA', 'MRXW_SWE', 'MRXW_TUR'))]

                }

                if ("reset_schema" %in% steps) {
                reset_schema(conn = conn,
                             schema = schema,
                             verbose = verbose,
                             render_sql = render_sql)
                }


                if ("ddl_tables" %in% steps) {
                        ddl_tables(conn = conn,
                                   schema = schema,
                                   tables = tables,
                                   verbose = verbose,
                                   render_sql = render_sql)
                }

                if ("copy_rrfs" %in% steps) {
                copy_rrfs(path_to_rrfs = path_to_rrfs,
                          tables = tables,
                          conn = conn,
                          schema = schema,
                          verbose = verbose,
                          render_sql = render_sql)
                }


                if ("add_indexes" %in% steps) {

                        add_indexes(conn = conn,
                                    schema = schema,
                                    verbose = verbose,
                                    render_sql = render_sql)
                }

                # Log
                if ("log" %in% steps) {
                table_names <-
                        pg13::ls_tables(conn = conn,
                                        schema = schema,
                                        verbose = verbose,
                                        render_sql = render_sql)

                current_row_count <-
                        table_names %>%
                        purrr::map(function(x) pg13::query(conn = conn,
                                                           sql_statement = pg13::render_row_count(schema = schema,
                                                                                                  tableName = x))) %>%
                        purrr::set_names(tolower(table_names)) %>%
                        dplyr::bind_rows(.id = "Table") %>%
                        dplyr::rename(Rows = count) %>%
                        tidyr::pivot_wider(names_from = "Table",
                                           values_from = "Rows") %>%
                        dplyr::mutate(sm_datetime = Sys.time(),
                                      sm_version = log_version,
                                      sm_release_date = log_release_date,
                                      sm_schema = schema) %>%
                        dplyr::select(sm_datetime,
                                      sm_version,
                                      sm_release_date,
                                      sm_schema,
                                      dplyr::everything())



                if (pg13::table_exists(conn = conn,
                                       schema = log_schema,
                                       table_name = log_table_name)) {

                        updated_log <-
                                dplyr::bind_rows(
                                        pg13::read_table(conn = conn,
                                                         schema = log_schema,
                                                         table = log_table_name,
                                                         verbose = verbose,
                                                         render_sql = render_sql,
                                                         render_only = render_only),
                                        current_row_count)  %>%
                                dplyr::select(sm_datetime,
                                              sm_version,
                                              sm_release_date,
                                              sm_schema,
                                              dplyr::everything())

                } else {
                        updated_log <- current_row_count
                }

                pg13::drop_table(conn = conn,
                                 schema = log_schema,
                                 table = log_table_name,
                                 verbose = verbose,
                                 render_sql = render_sql,
                                 render_only = render_only)

                pg13::write_table(conn = conn,
                                  schema = log_schema,
                                  table_name = log_table_name,
                                  data = updated_log,
                                  verbose = verbose,
                                  render_sql = render_sql,
                                  render_only = render_only)

                cli::cat_line()
                cli::cat_boxx("Log Results",
                              float = "center")
                print(tibble::as_tibble(updated_log))
                cli::cat_line()

                }
        }
