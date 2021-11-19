#' @title
#' Retrieve the Table Listing RxNorm Paths
#'
#' @description
#' Retrieve the table found at
#' \url{https://lhncbc.nlm.nih.gov/RxNav/applications/RxNavViews.html#label:appendix}
#' that will be used to generate the SQL queries to derive RxNorm maps.
#' @param url Source URL. It is not hardcoded in case the URL changes in the future.
#'  Default: 'https://lhncbc.nlm.nih.gov/RxNav/applications/RxNavViews.html#label:appendix'
#' @return
#' Tibble of a machine-readable version of what is read at the `url` parameter.
#' @rdname get_rxnorm_paths
#' @family RxNorm Map
#' @export
#' @import rvest
#' @import tidyr
#' @import dplyr
#' @import pg13
#' @importFrom rlang parse_expr
#' @importFrom glue glue
get_rxnorm_paths <-
        function(url = "https://lhncbc.nlm.nih.gov/RxNav/applications/RxNavViews.html#label:appendix") {
                input <-
                        read_html(url) %>%
                        rvest::html_table() %>%
                        pluck(2)
                # Limit overwhelming the server if run in multiple successions
                Sys.sleep(3)
                input <- input %>% as_tibble(.name_repair = "unique")
                input_a <- input %>% select_at(vars(1:2)) %>% unname()
                input_b <- input %>% select_at(vars(3:4)) %>% unname()
                colnames(input_a) <- c("start_to_end", "path")
                colnames(input_b) <- c("start_to_end", "path")
                output <-
                        bind_rows(input_a, input_b) %>%
                        mutate(path = str_remove_all(path, pattern = "[\r\t\n]")) %>%
                        separate_rows(path,
                                      sep = "or")

                output$path_count <-
                        output$path %>%
                        map(function(x) length(unlist(strsplit(x, split = "=>")))-1) %>%
                        unlist()
                max_path_count <-
                        max(output$path_count)

                output_cols <-
                        paste0("path_", 1:max_path_count)

                output$station_count <-
                        output$path %>%
                        map(function(x) length(unlist(strsplit(x, split = "=>")))) %>%
                        unlist()

                max_station_count <-
                        max(output$station_count)
                output_station_cols <-
                        paste0("station_", 1:max_station_count)

                output2 <-
                        output %>%
                        tidyr::separate(col = path,
                                        into = output_station_cols,
                                        sep = " => ")

                output_list <-
                        vector(mode = "list",
                               length = max_station_count)

                for (i in 1:max_station_count) {

                        if (i != 1) {
                                output_list[[i]] <-
                                        eval(
                                                rlang::parse_expr(
                                                        as.character(
                                                                glue::glue(
                                                                        "
      output2 %>%
      mutate(path_level = '{i-1}') %>%
      select(start_to_end, path_level, from = station_{i-1}, to = station_{i}) %>%
      distinct() %>%
      dplyr::filter_all(all_vars(!is.na(.)))
      "
                                                                ))))

                        }

                }

                output_list2 <-
                        bind_rows(output_list) %>%
                        distinct() %>%
                        arrange(start_to_end, from, to) %>%
                        extract(col = start_to_end,
                                into = c("start", "end"),
                                regex = "([A-Z]{2,}) => ([A-Z]{2,})")

                output_list2
        }

#' @title FUNCTION_TITLE
#' @description FUNCTION_DESCRIPTION
#' @param conn PARAM_DESCRIPTION
#' @param conn_fun PARAM_DESCRIPTION, Default: 'pg13::local_connect()'
#' @param start_arg PARAM_DESCRIPTION
#' @param end_arg PARAM_DESCRIPTION
#' @param verbose PARAM_DESCRIPTION, Default: TRUE
#' @param render_sql PARAM_DESCRIPTION, Default: TRUE
#' @param render_only PARAM_DESCRIPTION, Default: FALSE
#' @param checks PARAM_DESCRIPTION, Default: ''
#' @return OUTPUT_DESCRIPTION
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[dplyr]{filter}}
#'  \code{\link[glue]{glue}}
#'  \code{\link[cli]{cli_abort}}
#'  \code{\link[pg13]{query}}
#' @rdname get_rxnorm_map
#' @export
#' @importFrom dplyr filter
#' @importFrom glue glue
#' @importFrom cli cli_inform
#' @importFrom pg13 query
get_rxnorm_map <-
        function(conn,
                 conn_fun = "pg13::local_connect()",
                 start_arg,
                 end_arg,
                 verbose = TRUE,
                 render_sql = TRUE,
                 render_only = FALSE,
                 checks = "") {

                rxnorm_concept_map <-
                        get_rxnorm_concept_map()
                Sys.sleep(1)
                run_map <-
                        rxnorm_concept_map %>%
                        dplyr::filter(start %in% start_arg,
                                      end %in% end_arg) %>%
                        distinct() %>%
                        arrange(as.integer(path_level))

                if (nrow(run_map)==0) {
                        stop(glue::glue("A path starting from '{start_arg}' and ending at '{end_arg}' does not exist."))
                }

                max_path_level <-
                        max(run_map$path_level)
                cli::cli_inform("Path from '{start_arg}' to '{end_arg}' is {max_path_level} level{?s} in length.")

                output <- list()
                for (i in 1:nrow(run_map)) {
                        from <- run_map$from[i]
                        to   <- run_map$to[i]

                        sql_statement <-
                                glue::glue(
                                        "
    SELECT
     m1.aui AS {from}_aui,
     m1.code AS {from}_code,
     m1.str  AS {from}_str,
     m2.aui AS {to}_aui,
     m2.code AS {to}_code,
     m2.str  AS {to}_str
    FROM umls.MRCONSO m1
    INNER JOIN umls.MRREL r
    ON r.aui1 = m1.aui
    INNER JOIN umls.MRCONSO m2
    ON r.aui2 = m2.aui
    WHERE
      m1.sab = 'RXNORM' AND
      m1.tty = '{from}' AND
      m2.sab = 'RXNORM' AND
      m2.tty = '{to}'
    ;
    ")

                        sql_statement <-
                                as.character(sql_statement)

                        output[[i]] <-
                                pg13::query(conn = conn,
                                            sql_statement = sql_statement,
                                            verbose = verbose,
                                            render_sql = render_sql,
                                            render_only = render_only,
                                            checks = checks)

                }
                final_output <-
                        output %>%
                        map(function(x) x$value)


                final_output %>%
                        reduce(full_join) %>%
                        select(starts_with(tolower(start_arg)),
                               starts_with(tolower(end_arg))) %>%
                        distinct()
        }


#' @title FUNCTION_TITLE
#' @description FUNCTION_DESCRIPTION

#' @return OUTPUT_DESCRIPTION
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[dplyr]{filter}},\code{\link[dplyr]{mutate}},\code{\link[dplyr]{filter_all}}
#' @rdname get_rxnorm_ingredient_map
#' @export
#' @importFrom dplyr filter transmute filter_at
get_rxnorm_ingredient_map <-
        function() {
                rxnorm_concept_map <-
                        get_rxnorm_paths()

                ingr_rxnorm_concept_map <-
                        rxnorm_concept_map %>%
                        dplyr::filter(start == 'IN')

                end_args <-
                        unique(ingr_rxnorm_concept_map$end)

                rxnorm_ingredient_map <-
                        vector(mode = "list",
                               length = length(end_args))
                names(rxnorm_ingredient_map) <-
                        end_args

                for (end_arg in end_args) {

                        rxnorm_ingredient_map[[end_arg]] <-
                                get_rxnorm_map(start_arg =  "IN",
                                               end_arg = end_arg) %>%
                                rename_all(str_remove_all,
                                           pattern = paste0(tolower(end_arg), "_"))
                }

                rxnorm_ingredient_map2 <-
                        bind_rows(rxnorm_ingredient_map,
                                  .id = "tty") %>%
                        select(
                                tty,
                                aui,
                                code,
                                str,
                                in_aui,
                                in_code,
                                in_str) %>%
                        distinct() %>%
                        arrange(in_aui, tty, aui)

                # Add Ingredients mappings to self
                final_rxnorm_ingredient_map <-
                        bind_rows(rxnorm_ingredient_map2,
                                  rxnorm_ingredient_map2 %>%
                                          dplyr::transmute(tty = 'IN',
                                                           aui = in_aui,
                                                           code = in_code,
                                                           str = in_str,
                                                           in_aui,
                                                           in_aui,
                                                           in_str)) %>%
                        dplyr::filter_at(vars(c(aui, code, str)),
                                         all_vars(!is.na(.))) %>%
                        distinct()

        }
