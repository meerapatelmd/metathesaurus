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
        function(url = "https://lhncbc.nlm.nih.gov/RxNav/applications/RxNavViews.html#label:appendix") {

          cache_file <-
          R.cache::findCache(key = list(url = url),
                             dirs = "metathesaurus")


          if (!is.null(cache_file)) {

            cached_datetime <- file.info(cache_file)$ctime
            secretary::typewrite(
              glue::glue("Loading RxNorm paths table that was cached {prettyunits::time_ago(cached_datetime)}."))
            R.cache::loadCache(key = list(url = url),
                               dirs = "metathesaurus")

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
                                   dirs = "metathesaurus")

                output_list2
          }

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
           schema = "rxclass",
           table_name = "lookup_rxnorm_paths",
           url = "https://lhncbc.nlm.nih.gov/RxNav/applications/RxNavViews.html#label:appendix",
           verbose = TRUE,
           render_sql = TRUE,
           render_only = FALSE,
           checks = "") {



    out <-
      read_rxnorm_paths(url = url) %>%
      dplyr::group_by(start, end) %>%
      dplyr::arrange(as.integer(path_level),
                     .by_group = TRUE) %>%
      tibble::rowid_to_column()


    if (missing(conn)) {
      conn <- eval(rlang::parse_expr(conn_fun))
      on.exit(expr = pg13::dc(conn = conn), add = TRUE,
              after = TRUE)
    }


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
        DROP TABLE IF EXISTS {schema}.tmp_{table_name};
        CREATE TABLE {schema}.tmp_{table_name} (
          rowid int NOT NULL,
          rxnorm_start_tty varchar(10) NOT NULL,
          rxnorm_end_tty varchar(10) NOT NULL,
          path_level int NOT NULL,
          from_tty varchar(10) NOT NULL,
          to_tty varchar(10) NOT NULL
        );

        COPY {schema}.tmp_{table_name}
        FROM '{tmp_csv}'
        CSV HEADER QUOTE E'\\b';

        DROP TABLE IF EXISTS {schema}.{table_name};
        CREATE TABLE {schema}.{table_name} AS (
          SELECT
            tmp.rxnorm_start_tty,
            tmp.rxnorm_end_tty,
            tmp.path_level,
            tmp.from_tty,
            tmp.to_tty
          FROM {schema}.tmp_{table_name} tmp
          ORDER BY tmp.rowid
        );

        DROP TABLE {schema}.tmp_{table_name};
        ",
        checks = checks,
        verbose = verbose,
        render_sql = render_sql,
        render_only = render_only)
    )

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
                                               start_arg = start_arg,
                                               end_arg = end_arg,
                                               full_path = full_path,
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


#' @title FUNCTION_TITLE
#' @description FUNCTION_DESCRIPTION
#' @param conn PARAM_DESCRIPTION
#' @param conn_fun PARAM_DESCRIPTION, Default: 'pg13::local_connect()'
#' @param from_tty PARAM_DESCRIPTION, Default: c("BN", "BPCK", "DF", "DFG", "ET", "GPCK", "IN", "MIN", "PIN",
#'    "PSN", "SBD", "SBDC", "SBDF", "SBDG", "SCD", "SCDC", "SCDF",
#'    "SCDG", "SY", "TMSY")
#' @param to_tty PARAM_DESCRIPTION, Default: c("BN", "BPCK", "DF", "DFG", "ET", "GPCK", "IN", "MIN", "PIN",
#'    "PSN", "SBD", "SBDC", "SBDF", "SBDG", "SCD", "SCDC", "SCDF",
#'    "SCDG", "SY", "TMSY")
#' @param full_path PARAM_DESCRIPTION, Default: FALSE
#' @param schema PARAM_DESCRIPTION, Default: 'mth'
#' @param target_schema PARAM_DESCRIPTION, Default: 'rxclass'
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
#'  \code{\link[cli]{cli_alert}},\code{\link[cli]{cli_abort}}
#'  \code{\link[rlang]{parse_expr}}
#'  \code{\link[pg13]{dc}},\code{\link[pg13]{c("query", "query")}},\code{\link[pg13]{send}}
#'  \code{\link[dplyr]{filter}}
#'  \code{\link[glue]{glue}}
#'  \code{\link[readr]{write_delim}}
#' @rdname write_rxnorm_map
#' @family RxNorm Class
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
           target_schema = "rxclass",
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
