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
                           "add_indexes"),
                 mrconso_only = FALSE,
                 omop_only = FALSE,
                 english_only = TRUE,
                 verbose = TRUE,
                 render_sql = TRUE) {


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
        }
