#' @title
#' Retrieve the Table Listing RxNorm Paths
#'
#' @description
#' Retrieve the table found at
#' \url{https://lhncbc.nlm.nih.gov/RxNav/applications/RxNavViews.html#label:appendix}
#' that will be used to generate the SQL queries to derive RxNorm maps.
#' @param url Source URL. It is not hardcoded in case the URL changes in the future.
#'  Default: 'https://lhncbc.nlm.nih.gov/RxNav/applications/RxNavViews.html#label:appendix'
#' @details
#' The tibble parsed from the URL is cached indefinitely. This is to avoid losing
#' this dataframe in the event that the source documentation is ever lost.
#' @return
#' Tibble of a machine-readable version of what is read at the `url` parameter.
#' @rdname read_rxnorm_paths
#' @family RxNorm Map
#' @export
#' @import rvest
#' @import tidyr
#' @import dplyr
#' @import pg13
#' @import tibble
#' @import purrr
#' @import R.cache
#' @import secretary
#' @importFrom rlang parse_expr
#' @importFrom glue glue

read_rxnorm_paths <-
        function(url = "https://lhncbc.nlm.nih.gov/RxNav/applications/RxNavViews.html#label:appendix",
                 check_for_updates = FALSE) {

          cache_file <-
          R.cache::findCache(key = list(url = url),
                             dirs = "metathesaurus/rxnorm_map")


          if (!is.null(cache_file)) {

            if (!check_for_updates) {

              cached_datetime <- file.info(cache_file)$ctime
              secretary::typewrite(
                glue::glue("Loading RxNorm paths table that was cached {prettyunits::time_ago(cached_datetime)}. Rerun with `check_for_updates` set to TRUE to scrape the `url` and update the cache if a diff is detected."))

              existing_tbl <-
              R.cache::loadCache(key = list(url = url),
                                 dirs = "metathesaurus/rxnorm_map")

              return(existing_tbl %>%
                       mutate_all(stringr::str_replace_all,
                                  pattern = "(^.*?)(\\[.*?$)",
                                  replacement = "\\1") %>%
                       rubix::rm_multibyte_chars() %>%
                       mutate_all(trimws, "both"))



            } else {

              secretary::typewrite(
                glue::glue("Loading RxNorm paths table that was cached {prettyunits::time_ago(cached_datetime)}."))

              existing_tbl <-
                R.cache::loadCache(key = list(url = url),
                                   dirs = "metathesaurus/rxnorm_map")


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


              new_tbl <- output_list2

              # List all the files
              cache_folder_path <-
              file.path(R.cache::getCachePath(),
                        "metathesaurus/rxnorm_map")

              new_cache_file_index <-
                length(list.files(cache_folder_path))+1

              R.cache::saveCache(object = existing_tbl,
                                 key = list(index = new_cache_file_index, url = url),
                                 dirs = "metathesaurus/rxnorm_map")

              secretary::typewrite(
                glue::glue(
                  "Loading RxNorm paths table that was cached {prettyunits::time_ago(cached_datetime)}. Rerun with `check_for_updates` set to TRUE to scrape the `url` and update the cache if a diff is detected."))


              R.cache::saveCache(object = new_tbl %>%
                                   mutate_all(stringr::str_replace_all,
                                              pattern = "(^.*?)(\\[.*?$)",
                                              replacement = "\\1") %>%
                                   rubix::rm_multibyte_chars() %>%
                                   mutate_all(trimws, "both"),
                                 key = list(url = url),
                                 dirs = "metathesaurus/rxnorm_map")

            }

          } else {

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

                R.cache::saveCache(object = output_list2,
                                   key = list(url = url),
                                   dirs = "metathesaurus/rxnorm_map")

                output_list2 %>%
                  mutate_all(stringr::str_replace_all,
                             pattern = "(^.*?)(\\[.*?$)",
                             replacement = "\\1") %>%
                  rubix::rm_multibyte_chars() %>%
                  mutate_all(trimws, "both")
          }

        }

#' @title
#' Get the RxNorm Map Between 2 TTY
#'
#' @description
#' Get the RxNorm Map Between 2 TTY. This map
#' ignores any variability from the rel/rela.
#' @inheritParams pkg_args
#' @inheritParams pg13::query
#' @param from_tty The RxNorm TTY to start from.
#' @param to_tty   The RxNorm TTY to end at.
#' @param full_path By default, regardless of path length, only the `from_tty` and `to_tty`
#' values are returned. If TRUE, the entire path is returned instead.
#' @return
#' Tibble of the mappings between the `aui`, `code`, and `str` between
#' the two start and end tty arguments. If `full_path` is TRUE, the intermediate tty
#' values are also provided. Every set is prefixed with the tty value. The returned map
#' is derived from a full join and therefore, blank mappings between tty are also included
#' in the tibble.
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
#' @family RxNorm Map
#' @export
#' @importFrom dplyr filter
#' @importFrom glue glue
#' @importFrom cli cli_inform
#' @importFrom pg13 query
get_rxnorm_map <-
        function(conn,
                 conn_fun = "pg13::local_connect()",
                 from_tty = c('BN', 'BPCK', 'DF', 'DFG', 'ET', 'GPCK', 'IN', 'MIN', 'PIN', 'PSN', 'SBD', 'SBDC', 'SBDF', 'SBDG', 'SCD', 'SCDC', 'SCDF', 'SCDG', 'SY', 'TMSY'),
                 to_tty = c('BN', 'BPCK', 'DF', 'DFG', 'ET', 'GPCK', 'IN', 'MIN', 'PIN', 'PSN', 'SBD', 'SBDC', 'SBDF', 'SBDG', 'SCD', 'SCDC', 'SCDF', 'SCDG', 'SY', 'TMSY'),
                 full_path = FALSE,
                 schema = "mth",
                 verbose = TRUE,
                 render_sql = TRUE,
                 render_only = FALSE,
                 checks = "") {

            # Match Arguments
            from_tty <-
            match.arg(arg = from_tty,
                      choices = c('BN', 'BPCK', 'DF', 'DFG', 'ET', 'GPCK', 'IN', 'MIN', 'PIN', 'PSN', 'SBD', 'SBDC', 'SBDF', 'SBDG', 'SCD', 'SCDC', 'SCDF', 'SCDG', 'SY', 'TMSY'),
                      several.ok = FALSE)

            to_tty <-
              match.arg(arg = to_tty,
                        choices = c('BN', 'BPCK', 'DF', 'DFG', 'ET', 'GPCK', 'IN', 'MIN', 'PIN', 'PSN', 'SBD', 'SBDC', 'SBDF', 'SBDG', 'SCD', 'SCDC', 'SCDF', 'SCDG', 'SY', 'TMSY'),
                        several.ok = FALSE)

            if (identical(from_tty, to_tty)) {

              cli::cli_alert_danger("`from_tty` and `to_tty` are the same value. '{from_tty}' cannot be mapped to itself.")



            } else {

              if (missing(conn)) {
                conn <- eval(rlang::parse_expr(conn_fun))
                on.exit(expr = pg13::dc(conn = conn), add = TRUE,
                        after = TRUE)
              }

                rxnorm_concept_map <-
                        read_rxnorm_paths()
                Sys.sleep(1)
                run_map <-
                        rxnorm_concept_map %>%
                        dplyr::filter(start %in% from_tty,
                                      end %in% to_tty) %>%
                        distinct() %>%
                        arrange(as.integer(path_level))

                if (nrow(run_map)==0) {
                        stop(glue::glue("A path starting from '{from_tty}' and ending at '{to_tty}' does not exist."))
                }

                max_path_level <-
                        max(run_map$path_level)
                cli::cli_inform("Path from '{from_tty}' to '{to_tty}' is {max_path_level} level{?s} in length.")

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
    FROM {schema}.MRCONSO m1
    INNER JOIN {schema}.MRREL r
    ON r.aui1 = m1.aui
    INNER JOIN {schema}.MRCONSO m2
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
                final_output <- output

                if (full_path) {

                  final_output %>%
                    reduce(full_join) %>%
                    distinct()


                } else {

                final_output %>%
                        reduce(full_join) %>%
                        select(starts_with(tolower(from_tty)),
                               starts_with(tolower(to_tty))) %>%
                        distinct()

                }
            }
        }


#' @title
#' Get RxNorm Ingredient Map
#'
#' @description
#' Iterate on all the possible paths
#' out of the RxNorm Ingredient concepts
#' using the `get_rxnorm_map` function.
#'
#' @return
#' Tibble of each unique combination of
#' from tty 'IN' and an ending tty with an extra field that
#' provides the given tty value. The final tibble
#' as includes 'IN' mappings to itself.
#'
#' @inheritParams  get_rxnorm_map
#' @rdname get_rxnorm_ingredient_map
#' @family RxNorm Map
#' @export
#' @import dplyr
#' @import tidyr
#' @import purrr
#' @import pg13

get_rxnorm_ingredient_map <-
        function(conn,
                 conn_fun = "pg13::local_connect()",
                 schema = "mth",
                 verbose = TRUE,
                 render_sql = TRUE,
                 render_only = FALSE,
                 checks = "") {

                rxnorm_concept_map <-
                        read_rxnorm_paths()

                ingr_rxnorm_concept_map <-
                        rxnorm_concept_map %>%
                        dplyr::filter(start == 'IN')

                to_ttys <-
                        unique(ingr_rxnorm_concept_map$end)

                if (missing(conn)) {
                  conn <- eval(rlang::parse_expr(conn_fun))
                  on.exit(expr = pg13::dc(conn = conn), add = TRUE,
                          after = TRUE)
                }


                rxnorm_ingredient_map <-
                        vector(mode = "list",
                               length = length(to_ttys))
                names(rxnorm_ingredient_map) <-
                        to_ttys

                for (to_tty in to_ttys) {

                        rxnorm_ingredient_map[[to_tty]] <-
                                get_rxnorm_map(from_tty =  "IN",
                                               to_tty = to_tty,
                                               conn = conn,
                                               full_path = FALSE,
                                               schema = schema,
                                               verbose = verbose,
                                               render_sql = render_sql,
                                               render_only = render_only,
                                               checks = checks) %>%
                                rename_all(str_remove_all,
                                           pattern = paste0(tolower(to_tty), "_"))
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


                final_rxnorm_ingredient_map

        }

#' @title
#' Get TTY Lookup
#' @description
#' Get custom expansions from tty derived from the
#' MRDOC table as a data frame.
#' @rdname get_rxnorm_tty_lookup
#' @family RxNorm Map
#' @export
#' @importFrom tibble tribble
#' @importFrom stringr str_replace_all
#' @importFrom dplyr mutate

get_rxnorm_tty_lookup <-
  function() {
    tibble::tribble(
      ~`tty`, ~`tty_expanded`,
      'BN',  'Brand Name',
      'SBDC','Semantic Branded Drug Component',
      'ET',  'Entry Term',
      'DFG','Dose Form Group',
      'MIN','Multi-Ingredient',
      'TMSY','Tall Man Synonym',
      'SBD','Semantic Branded Drug',
      'SY','Synonym',
      'SCDF','Semantic Clinical Drug and Form',
      'SCD','Semantic Clinical Drug',
      'SCDC','Semantic Drug Component',
      'DF','Dose Form',
      'PIN','Precise Ingredient',
      'SCDG','Semantic Clinical Drug Group',
      'GPCK','Generic Drug Delivery Device',
      'PSN','Prescribable Name',
      'IN','Ingredient',
      'BPCK','Branded Drug Delivery Device',
      'SBDG','Semantic Branded Drug Group',
      'SBDF','Semantic Branded Drug and Form'
    ) %>%
      dplyr::mutate(table_name =
                      tolower(
                      stringr::str_replace_all(tty_expanded,
                                               pattern = " |[[:punct:]]",
                                               replacement = "_"))) %>%
      dplyr::mutate(table_name =
                      sprintf("rxnorm_%s_map", table_name))

  }





rxnorm_requires_processing <-
  function(conn,
           conn_fun = "pg13::local_connect()",
           mth_version,
           mth_release_dt,
           target_table,
           verbose = TRUE,
           render_sql = TRUE,
           render_only = FALSE,
           checks = "") {

    if (missing(mth_version)|missing(target_table)) {
      stop("`mth_version` and `target_table` must be supplied!", call. = FALSE)
    }


    if (missing(conn)) {
      conn <- eval(rlang::parse_expr(conn_fun))
      on.exit(expr = pg13::dc(conn = conn), add = TRUE,
              after = TRUE)
    }


sql_statement <-
  "
  CREATE TABLE IF NOT EXISTS public.process_rxmap_log (
    process_start_datetime timestamp without time zone,
    process_stop_datetime timestamp without time zone,
    mth_version character varying(255),
    mth_release_dt character varying(255),
    sab character varying(255),
    target_schema character varying(255),
    source_table character varying(255),
    target_table character varying(255),
    source_row_ct numeric,
    target_row_ct numeric
  );
  "

pg13::send(
  conn = conn,
  sql_statement = sql_statement,
  checks = checks,
  verbose = verbose,
  render_sql = render_sql,
  render_only = render_only)

sql_statement <-
  glue::glue(
"
SELECT *
FROM public.process_rxmap_log
WHERE
  mth_version = '{mth_version}'
  AND mth_release_dt = '{mth_release_dt}'
  AND target_table = '{target_table}'
")

out <-
pg13::query(
  conn = conn,
  sql_statement = sql_statement,
  checks = checks,
  verbose = verbose,
  render_sql = render_sql,
  render_only = render_only)

nrow(out)==0

  }


rxnorm_log_processing <-
  function(process_start,
           process_stop,
           mth_version,
           mth_release_dt,
           target_schema,
           source_table,
           target_table,
           source_table_rows,
           target_table_rows,
           conn,
           conn_fun = "pg13::local_connect()",
           verbose = TRUE,
           render_sql = TRUE,
           render_only = FALSE,
           checks = "") {



    if (missing(conn)) {
      conn <- eval(rlang::parse_expr(conn_fun))
      on.exit(expr = pg13::dc(conn = conn), add = TRUE,
              after = TRUE)
    }



    sql_statement <-
      glue::glue(
        "INSERT INTO public.process_rxmap_log
        VALUES(
          '{process_start}',
          '{process_stop}',
          '{mth_version}',
          '{mth_release_dt}',
          'RXNORM',
          '{target_schema}',
          '{source_table}',
          '{target_table}',
          '{source_table_rows}',
          '{target_table_rows}'
        );
        "
      )


    pg13::send(
      conn = conn,
      sql_statement = sql_statement,
      checks = checks,
      verbose = verbose,
      render_sql = render_sql,
      render_only = render_only)
  }

#' @title
#' Write RxNorm Path Lookup
#'
#' @description
#' Write the tibble returned by `read_rxnorm_paths()` to a
#' Postgres table.
#'
#' @inheritParams pkg_args
#' @inheritParams read_rxnorm_paths
#' @inheritParams pg13::write_table
#' @rdname write_rxnorm_path_lookup
#' @family RxNorm Map
#' @export
#' @importFrom rlang parse_expr
#' @importFrom pg13 dc write_table

write_rxnorm_path_lookup <-
  function(conn,
           conn_fun = "pg13::local_connect()",
           target_schema = "rxmap",
           table_name = "lookup_rxnorm_paths",
           mth_version,
           mth_release_dt,
           url = "https://lhncbc.nlm.nih.gov/RxNav/applications/RxNavViews.html#label:appendix",
           check_for_updates = FALSE,
           verbose = TRUE,
           render_sql = TRUE,
           render_only = FALSE,
           checks = "") {

    out <-
      read_rxnorm_paths(url = url,
                        check_for_updates = check_for_updates) %>%
      dplyr::group_by(start, end) %>%
      dplyr::arrange(as.integer(path_level),
                     .by_group = TRUE) %>%
      tibble::rowid_to_column()


    if (missing(conn)) {
      conn <- eval(rlang::parse_expr(conn_fun))
      on.exit(expr = pg13::dc(conn = conn), add = TRUE,
              after = TRUE)
    }

    if (rxnorm_requires_processing(conn = conn,
                               mth_version = mth_version,
                               mth_release_dt = mth_release_dt,
                               target_table = table_name,
                               verbose = verbose,
                               render_sql = render_sql,
                               render_only = render_only,
                               checks = checks)) {


      process_start <- Sys.time()

    tmp_csv <- tempfile(fileext = ".csv")

    readr::write_csv(x = out,
                     file = tmp_csv,
                     na = "",
                     col_names = TRUE)

    on.exit(expr = unlink(tmp_csv),
            add = TRUE,
            after = TRUE)

    pg13::send(
      conn = conn,
      sql_statement =
        glue::glue(
          "
        DROP TABLE IF EXISTS {target_schema}.tmp_{table_name};
        CREATE TABLE {target_schema}.tmp_{table_name} (
          rowid int NOT NULL,
          rxnorm_start_tty varchar(10) NOT NULL,
          rxnorm_end_tty varchar(10) NOT NULL,
          path_level int NOT NULL,
          from_tty varchar(10) NOT NULL,
          to_tty varchar(10) NOT NULL
        );

        COPY {target_schema}.tmp_{table_name}
        FROM '{tmp_csv}'
        CSV HEADER QUOTE E'\\b';

        DROP TABLE IF EXISTS {target_schema}.{table_name};
        CREATE TABLE {target_schema}.{table_name} AS (
          SELECT
            tmp.rxnorm_start_tty,
            tmp.rxnorm_end_tty,
            tmp.path_level,
            tmp.from_tty,
            tmp.to_tty
          FROM {target_schema}.tmp_{table_name} tmp
          ORDER BY tmp.rowid
        );

        DROP TABLE {target_schema}.tmp_{table_name};
        ",
          checks = checks,
          verbose = verbose,
          render_sql = render_sql,
          render_only = render_only)
    )


    process_stop <-
      Sys.time()


    target_table_rows <-
      pg13::query(
        conn = conn,
        checks = checks,
        sql_statement = glue::glue("SELECT COUNT(*) FROM {target_schema}.{table_name};"),
        verbose = verbose,
        render_sql = render_sql,
        render_only = render_only) %>%
      unlist() %>%
      unname()

    rxnorm_log_processing(
      conn = conn,
      process_start = process_start,
      process_stop = process_stop,
      mth_version = mth_version,
      mth_release_dt = mth_release_dt,
      target_schema = target_schema,
      source_table = "",
      target_table = table_name,
      source_table_rows = 0,
      target_table_rows = target_table_rows
    )


    }
  }


#' @title
#' Write RxNorm TTY Lookup
#' @rdname write_rxnorm_tty_lookup
#' @export
#' @importFrom readr write_csv
#' @importFrom rlang parse_expr
#' @importFrom pg13 dc send


write_rxnorm_tty_lookup <-
  function(conn,
           conn_fun = "pg13::local_connect()",
           schema = "mth",
           target_schema = "rxmap",
           mth_version,
           mth_release_dt,
           verbose = TRUE,
           render_sql = TRUE,
           render_only = FALSE,
           checks = "") {

    if (missing(conn)) {
      conn <- eval(rlang::parse_expr(conn_fun))
      on.exit(expr = pg13::dc(conn = conn), add = TRUE,
              after = TRUE)
    }

    if (rxnorm_requires_processing(conn = conn,
                                   mth_version = mth_version,
                                   mth_release_dt = mth_release_dt,
                                   target_table = "lookup_tty",
                                   verbose = verbose,
                                   render_sql = render_sql,
                                   render_only = render_only,
                                   checks = checks)) {


      process_start <- Sys.time()

      final_output <- get_rxnorm_tty_lookup()

      tmp_csv <- tempfile(fileext = ".csv")

      readr::write_csv(x = final_output,
                       file = tmp_csv,
                       na = "",
                       quote = "all",
                       col_names = TRUE)

      on.exit(expr = unlink(tmp_csv),
              add = TRUE,
              after = TRUE)



      sql_statement <-
        glue::glue(
          "
      DROP TABLE IF EXISTS {target_schema}.tmp_lookup_tty;
      CREATE TABLE {target_schema}.tmp_lookup_tty (
        tty varchar(10),
        tty_expanded varchar(100),
        table_name varchar(63)

      );

      COPY {target_schema}.tmp_lookup_tty
      FROM '{tmp_csv}'
      CSV HEADER QUOTE E'\"';

      DROP TABLE IF EXISTS {target_schema}.tmp_lookup_tty2;
      CREATE TABLE {target_schema}.tmp_lookup_tty2 AS (
        SELECT m.tty, COUNT(*) AS tty_count
        FROM {schema}.mrconso m
        WHERE m.sab = 'RXNORM'
        GROUP BY tty
      );

      DROP TABLE IF EXISTS {target_schema}.lookup_tty;
      CREATE TABLE {target_schema}.lookup_tty AS (
      SELECT cnt.tty, cnt.tty_count, exp.tty_expanded, exp.table_name
      FROM {target_schema}.tmp_lookup_tty2 cnt
      LEFT JOIN {target_schema}.tmp_lookup_tty exp
      ON exp.tty = cnt.tty
      ORDER BY cnt.tty_count DESC
      );

      DROP TABLE {target_schema}.tmp_lookup_tty;
      DROP TABLE {target_schema}.tmp_lookup_tty2;

      ")

      pg13::send(conn = conn,
                 sql_statement = sql_statement,
                 checks = checks,
                 verbose = verbose,
                 render_sql = render_sql,
                 render_only = render_only)


      process_stop <-
        Sys.time()

      target_table_rows <-
        pg13::query(conn = conn,
                    sql_statement = glue::glue("SELECT COUNT(*) FROM {target_schema}.lookup_tty;"),
                    verbose = verbose,
                    render_sql = render_sql,
                    render_only = render_only,
                    checks = checks) %>%
        unlist() %>%
        unname()


      rxnorm_log_processing(
        conn = conn,
        process_start = process_start,
        process_stop = process_stop,
        mth_version = mth_version,
        mth_release_dt = mth_release_dt,
        target_schema = target_schema,
        source_table = "",
        target_table = "lookup_tty",
        source_table_rows = 0,
        target_table_rows = target_table_rows
      )




    }

  }


#' @title
#' Write RxNorm Ingredient Map
#' @rdname write_rxnorm_ingredient_map
#' @export
#' @importFrom readr write_csv
#' @importFrom glue glue
#' @importFrom pg13 send


write_rxnorm_ingredient_map <-
  function(conn,
           conn_fun = "pg13::local_connect()",
           schema = "mth",
           target_schema = "rxmap",
           verbose = TRUE,
           render_sql = TRUE,
           render_only = FALSE,
           checks = "") {

    .Deprecated()

    final_output <-
      get_rxnorm_ingredient_map(
        conn = conn,
        conn_fun = conn_fun,
        schema = schema,
        verbose = verbose,
        render_sql = render_sql,
        render_only = render_only,
        checks = checks
      )

    tmp_csv <- tempfile(fileext = ".csv")

    readr::write_csv(x = final_output,
                     file = tmp_csv,
                     na = "",
                     quote = "all",
                     col_names = TRUE)

    on.exit(expr = unlink(tmp_csv),
            add = TRUE,
            after = TRUE)


    sql_statement <-
    glue::glue(
      "
      DROP TABLE IF EXISTS {target_schema}.rxnorm_ingredient_map;
      CREATE TABLE {target_schema}.rxnorm_ingredient_map (
        tty varchar(10),
        aui varchar(9),
        code integer,
        str text,
        in_aui varchar(9),
        in_code integer,
        in_str text
      );

      COPY {target_schema}.rxnorm_ingredient_map
      FROM '{tmp_csv}'
      CSV HEADER QUOTE E'\"';
      ")

    if (missing(conn)) {
      conn <- eval(rlang::parse_expr(conn_fun))
      on.exit(expr = pg13::dc(conn = conn), add = TRUE,
              after = TRUE)
    }

      pg13::send(conn = conn,
                 sql_statement = sql_statement,
                 checks = checks,
                 verbose = verbose,
                 render_sql = render_sql,
                 render_only = render_only)





  }



#' @title
#' Write RxNorm Map
#' @rdname write_rxnorm_map
#' @family RxNorm Map
#' @export
#' @importFrom cli cli_alert_danger cli_inform
#' @importFrom rlang parse_expr
#' @importFrom pg13 dc query send
#' @importFrom dplyr filter
#' @importFrom glue glue
#' @importFrom readr write_csv



write_rxnorm_map <-
  function(conn,
           conn_fun = "pg13::local_connect()",
           from_tty = c('BN', 'BPCK', 'DF', 'DFG', 'ET', 'GPCK', 'IN', 'MIN', 'PIN', 'PSN', 'SBD', 'SBDC', 'SBDF', 'SBDG', 'SCD', 'SCDC', 'SCDF', 'SCDG', 'SY', 'TMSY'),
           to_tty = c('BN', 'BPCK', 'DF', 'DFG', 'ET', 'GPCK', 'IN', 'MIN', 'PIN', 'PSN', 'SBD', 'SBDC', 'SBDF', 'SBDG', 'SCD', 'SCDC', 'SCDF', 'SCDG', 'SY', 'TMSY'),
           full_path = FALSE,
           schema = "mth",
           target_schema = "rxmap",
           verbose = TRUE,
           render_sql = TRUE,
           render_only = FALSE,
           checks = "") {

    # Match Arguments
    from_tty <-
      match.arg(arg = from_tty,
                choices = c('BN', 'BPCK', 'DF', 'DFG', 'ET', 'GPCK', 'IN', 'MIN', 'PIN', 'PSN', 'SBD', 'SBDC', 'SBDF', 'SBDG', 'SCD', 'SCDC', 'SCDF', 'SCDG', 'SY', 'TMSY'),
                several.ok = FALSE)

    to_tty <-
      match.arg(arg = to_tty,
                choices = c('BN', 'BPCK', 'DF', 'DFG', 'ET', 'GPCK', 'IN', 'MIN', 'PIN', 'PSN', 'SBD', 'SBDC', 'SBDF', 'SBDG', 'SCD', 'SCDC', 'SCDF', 'SCDG', 'SY', 'TMSY'),
                several.ok = FALSE)

    if (identical(from_tty, to_tty)) {

      cli::cli_alert_danger("`from_tty` and `to_tty` are the same value. '{from_tty}' cannot be mapped to itself.")



    } else {

      if (missing(conn)) {
        conn <- eval(rlang::parse_expr(conn_fun))
        on.exit(expr = pg13::dc(conn = conn), add = TRUE,
                after = TRUE)
      }

      rxnorm_concept_map <-
        read_rxnorm_paths()
      Sys.sleep(1)
      run_map <-
        rxnorm_concept_map %>%
        dplyr::filter(start %in% from_tty,
                      end %in% to_tty) %>%
        distinct() %>%
        arrange(as.integer(path_level))

      if (nrow(run_map)==0) {
        stop(glue::glue("A path starting from '{from_tty}' and ending at '{to_tty}' does not exist."))
      }

      max_path_level <-
        max(run_map$path_level)
      cli::cli_inform("Path from '{from_tty}' to '{to_tty}' is {max_path_level} level{?s} in length.")

      output <- list()
      for (i in 1:nrow(run_map)) {
        from <- run_map$from[i]
        to   <- run_map$to[i]

        sql_statement <-
          glue::glue(
            "
    SELECT
     m1.tty  AS {from}_tty,
     m1.aui  AS {from}_aui,
     m1.code AS {from}_code,
     m1.str  AS {from}_str,
     m2.tty  AS {to}_tty,
     m2.aui  AS {to}_aui,
     m2.code AS {to}_code,
     m2.str  AS {to}_str
    FROM {schema}.MRCONSO m1
    INNER JOIN {schema}.MRREL r
    ON r.aui1 = m1.aui
    INNER JOIN {schema}.MRCONSO m2
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
      final_output <- output


      if (full_path) {

        final_output <-
        final_output %>%
          reduce(full_join) %>%
          distinct()


      } else {

        final_output <-
        final_output %>%
          reduce(full_join) %>%
          select(starts_with(tolower(from_tty)),
                 starts_with(tolower(to_tty))) %>%
          distinct()

      }
    }

    tmp_csv <- tempfile(fileext = ".csv")

    readr::write_csv(x = final_output,
                     file = tmp_csv,
                     na = "",
                     quote = "all",
                     col_names = TRUE)

    on.exit(expr = unlink(tmp_csv),
            add = TRUE,
            after = TRUE)


    new_table_field_names <- colnames(final_output)
    new_table_field_types <- vector(mode = "character",
                                    length = length(new_table_field_names))

    tty_ddl <-
      grepl(pattern = "tty",
            x = new_table_field_names) %>%
      map(function(x) ifelse(x == TRUE, "varchar(10)", NA_character_)) %>%
      unlist()

    aui_ddl <-
    grepl(pattern = "aui",
          x = new_table_field_names) %>%
      map(function(x) ifelse(x == TRUE, "varchar(9)", NA_character_)) %>%
      unlist()

    code_ddl <-
      grepl(pattern = "code",
            x = new_table_field_names) %>%
      map(function(x) ifelse(x == TRUE, "text", NA_character_)) %>%
      unlist()

    str_ddl <-
      grepl(pattern = "str",
            x = new_table_field_names) %>%
      map(function(x) ifelse(x == TRUE, "text", NA_character_)) %>%
      unlist()


    str_map <-
    tibble(field = new_table_field_names,
           tty_ddl = tty_ddl,
           aui_ddl = aui_ddl,
           code_ddl = code_ddl,
           str_ddl = str_ddl) %>%
      transmute(field,
                ddl = coalesce(tty_ddl,
                               aui_ddl,
                               code_ddl,
                               str_ddl))

    if (any(is.na(str_map$ddl))) {

      print(str_map)
      cli::cli_alert_danger("DDL is missing.")

    }

   ddl_strs <-
      str_map %>%
      transmute(ddl = sprintf("\t%s %s NOT NULL", field, ddl)) %>%
      unlist() %>%
      unname() %>%
      paste(collapse = ",\n")





   if (full_path) {

     final_table <- glue::glue("rxnorm_map_from_{from_tty}_to_{to_tty}_full")

   } else {

     final_table <- glue::glue("rxnorm_map_from_{from_tty}_to_{to_tty}")

   }

   sql_statement <-
     glue::glue(
       "
       DROP TABLE IF EXISTS {target_schema}.{final_table};
       CREATE TABLE {target_schema}.{final_table} (
       {ddl_strs}
       );

        COPY {target_schema}.{final_table}
        FROM '{tmp_csv}'
        CSV HEADER QUOTE E'\"';
       ")


       pg13::send(conn = conn,
                  sql_statement = sql_statement,
                    checks = checks,
                  verbose = verbose,
                  render_sql = render_sql,
                  render_only = render_only)





  }






#' @title
#' Write All RxNorm Maps
#' @description
#' Each map is written to the given schema.
#' @rdname write_rxnorm_all_maps
#' @family RxNorm Maps
#' @export
#' @importFrom readr write_csv
#' @importFrom glue glue
#' @importFrom pg13 send


write_rxnorm_all_maps <-
  function(conn,
           conn_fun = "pg13::local_connect()",
           schema = "mth",
           target_schema = "rxmap",
           mth_version,
           mth_release_dt,
           verbose = TRUE,
           render_sql = TRUE,
           render_only = FALSE,
           checks = "") {


    if (missing(conn)) {
      conn <- eval(rlang::parse_expr(conn_fun))
      on.exit(expr = pg13::dc(conn = conn), add = TRUE,
              after = TRUE)
    }


    sql_statement <-
      glue::glue("SELECT * FROM {target_schema}.lookup_tty;")


    tty_lookup <-
      pg13::query(conn = conn,
                  sql_statement = sql_statement,
                  checks = checks,
                  verbose = verbose,
                  render_sql = render_sql,
                  render_only = render_only)

    tty_rxnorm_concept_map <-
      read_rxnorm_paths() %>%
      rubix::rm_multibyte_chars() %>%
      mutate_all(trimws, which = "both")

    start_ttys <-
    tty_rxnorm_concept_map %>%
      dplyr::select(start) %>%
      unlist() %>%
      unname() %>%
      unique()


    for (start_tty in start_ttys) {

      target_table_name <-
        tty_lookup %>%
        dplyr::filter(tty == start_tty) %>%
        dplyr::select(table_name) %>%
        unlist() %>%
        unname()

      if (
      rxnorm_requires_processing(
        conn = conn,
        mth_version = mth_version,
        mth_release_dt = mth_release_dt,
        target_table = target_table_name,
        verbose = verbose,
        render_sql = render_sql,
        render_only = render_only,
        checks = checks
      )) {


      process_start <- Sys.time()


      rxnorm_concept_map <<-
        tty_rxnorm_concept_map %>%
        dplyr::filter(start == start_tty) %>%
        dplyr::mutate_all(trimws, "both") %>%
        dplyr::distinct()

      to_ttys <-
        unique(rxnorm_concept_map$end)

      rxnorm_tty_map <-
        vector(mode = "list",
               length = length(to_ttys))
      names(rxnorm_tty_map) <-
        to_ttys

    for (to_tty in to_ttys) {

      rxnorm_tty_map[[to_tty]] <-
        get_rxnorm_map(from_tty =  start_tty,
                       to_tty = to_tty,
                       conn = conn,
                       full_path = FALSE,
                       schema = schema,
                       verbose = verbose,
                       render_sql = render_sql,
                       render_only = render_only,
                       checks = checks) %>%
        rename_all(str_remove_all,
                   pattern = paste0(tolower(to_tty), "_"))
    }

    rxnorm_tty_map2 <-
      bind_rows(rxnorm_tty_map,
                .id = "tty") %>%
      select(
        tty,
        aui,
        code,
        str,
        starts_with(sprintf("%s_", tolower(start_tty)))) %>%
      distinct() %>%
      arrange_at(vars(starts_with(start_tty), tty, aui))

    # Add mappings to self
    rxnorm_tty_map2_b <-
      bind_cols(
    rxnorm_tty_map2 %>%
      dplyr::select_at(vars(starts_with(start_tty))) %>%
      dplyr::rename_all(
        function(x)
          stringr::str_remove_all(
            x,
            pattern = sprintf("%s_", tolower(start_tty)))) %>%
      mutate(tty = start_tty),
    rxnorm_tty_map2 %>%
      dplyr::select_at(vars(starts_with(start_tty))))

    final_rxnorm_tty_map <-
      bind_rows(rxnorm_tty_map2,
                rxnorm_tty_map2_b) %>%
      dplyr::filter_at(vars(c(aui, code, str)),
                       all_vars(!is.na(.))) %>%
      distinct()

    tmp_csv <- tempfile(fileext = ".csv")

    readr::write_csv(x = final_rxnorm_tty_map,
                     file = tmp_csv,
                     na = "",
                     quote = "all",
                     col_names = TRUE)

    on.exit(expr = unlink(tmp_csv),
            add = TRUE,
            after = TRUE)


    sql_statement <-
      glue::glue(
        "
      DROP TABLE IF EXISTS {target_schema}.{target_table_name};
      CREATE TABLE {target_schema}.{target_table_name} (
        tty varchar(10),
        aui varchar(9),
        code integer,
        str text,
        {start_tty}_aui varchar(9),
        {start_tty}_code integer,
        {start_tty}_str text
      );

      COPY {target_schema}.{target_table_name}
      FROM '{tmp_csv}'
      CSV HEADER QUOTE E'\"';
      ")


    errors <- vector()
    error_data <- list()
    x <-
    tryCatch(
    pg13::send(conn = conn,
               sql_statement = sql_statement,
               checks = checks,
               verbose = verbose,
               render_sql = render_sql,
               render_only = render_only),
    error = function(e) "Error"
    )


    if (!identical(x, "Error")) {

    process_stop <- Sys.time()


    target_table_rows <-
      pg13::query(
        conn = conn,
        checks = checks,
        sql_statement = glue::glue("SELECT COUNT(*) FROM {target_schema}.{target_table_name};"),
        verbose = verbose,
        render_sql = render_sql,
        render_only = render_only) %>%
      unlist() %>%
      unname()


    rxnorm_log_processing(
      conn = conn,
      process_start = process_start,
      process_stop =  process_stop,
      mth_version = mth_version,
      mth_release_dt =  mth_release_dt,
      target_schema = target_schema,
      source_table = "",
      target_table = target_table_name,
      source_table_rows = 0,
      target_table_rows = target_table_rows

    )


    sql_statement <-
      "
      CREATE TABLE IF NOT EXISTS public.setup_rxmap_log (
        srl_datetime TIMESTAMP WITHOUT TIME ZONE,
        mth_version varchar(25),
        mth_release_dt varchar(12),
        rxnorm_brand_name_map int,
        rxnorm_branded_drug_delivery_device_map int,
        rxnorm_generic_drug_delivery_device_map int,
        rxnorm_ingredient_map int,
        rxnorm_multi_ingredient_map int,
        rxnorm_precise_ingredient_map int,
        rxnorm_semantic_branded_drug_map int,
        rxnorm_semantic_branded_drug_and_form_map int,
        rxnorm_semantic_branded_drug_component_map int,
        rxnorm_semantic_branded_drug_group_map int,
        rxnorm_semantic_clinical_drug_map int,
        rxnorm_semantic_clinical_drug_and_form_map int,
        rxnorm_semantic_clinical_drug_group_map int,
        rxnorm_semantic_drug_component_map int
      );
      "

    pg13::send(conn = conn,
               sql_statement = sql_statement,
               checks = checks,
               verbose = verbose,
               render_sql = render_sql,
               render_only = render_only)

    sql_statement <-
      glue::glue(
        "
          SELECT *
          FROM public.setup_rxmap_log
          WHERE
            mth_version = '{mth_version}'
            AND mth_release_dt = '{mth_release_dt}'
          "
      )

    log_out <-
      pg13::query(conn = conn,
                  sql_statement = sql_statement,
                  checks = checks,
                  verbose = verbose,
                  render_sql = render_sql,
                  render_only = render_only)

    if (nrow(log_out) == 0) {
      sql_statement <-
        glue::glue(
          "
            INSERT INTO public.setup_rxmap_log(srl_datetime,mth_version,mth_release_dt)
            VALUES('{Sys.time()}', '{mth_version}', '{mth_release_dt}');
            "
        )

      pg13::send(conn = conn,
                 sql_statement = sql_statement,
                 checks = checks,
                 verbose = verbose,
                 render_sql = render_sql,
                 render_only = render_only)


    }

    sql_statement <-
      glue::glue(
        "
          UPDATE public.setup_rxmap_log
          SET {target_table_name} = {target_table_rows}
          WHERE
            mth_version = '{mth_version}'
            AND mth_release_dt = '{mth_release_dt}'
          "
      )

    pg13::send(conn = conn,
               sql_statement = sql_statement,
               checks = checks,
               verbose = verbose,
               render_sql = render_sql,
               render_only = render_only)





    } else {

      errors <<-
        unique(
        c(target_table_name,
          errors)
        )

      error_data[[length(error_data)+1]] <<-
      final_rxnorm_tty_map

    }

    if (length(errors)>0) {

      secretary::typewrite("The following maps did not load:")
      secretary::typewrite(sprintf("\t\t\t%s,\n", errors),
                           timepunched = FALSE)




    }



    }

    }

    }


#' @title
#' Setup RxMap
#' @rdname setup_rxmap
#' @family RxNorm Map
#' @export
#' @importFrom rlang parse_expr
#' @importFrom pg13 dc

setup_rxmap <-
  function(conn,
           conn_fun = "pg13::local_connect()",
           schema = "mth",
           target_schema = "rxmap",
           mth_version,
           mth_release_dt,
           url = "https://lhncbc.nlm.nih.gov/RxNav/applications/RxNavViews.html#label:appendix",
           check_for_updates = FALSE,
           verbose = TRUE,
           render_sql = TRUE,
           render_only = FALSE,
           checks = "") {


    if (missing(conn)) {
      conn <- eval(rlang::parse_expr(conn_fun))
      on.exit(expr = pg13::dc(conn = conn),
              add = TRUE,
              after = TRUE)
    }


    write_rxnorm_path_lookup(
      conn = conn,
      target_schema = target_schema,
      table_name = "lookup_rxnorm_paths",
      mth_version = mth_version,
      mth_release_dt = mth_release_dt,
      url = url,
      check_for_updates = check_for_updates,
      verbose = verbose,
      render_sql = render_sql,
      render_only = render_only,
      checks = checks)

    write_rxnorm_tty_lookup(
      conn = conn,
      schema = schema,
      target_schema = target_schema,
      mth_version = mth_version,
      mth_release_dt = mth_release_dt,
      verbose = verbose,
      render_sql = render_sql,
      render_only = render_only,
      checks = checks
    )


    write_rxnorm_all_maps(
      conn = conn,
      schema = schema,
      target_schema = target_schema,
      mth_version = mth_version,
      mth_release_dt = mth_release_dt,
      verbose = verbose,
      render_sql = render_sql,
      render_only = render_only,
      checks = checks
    )




  }
