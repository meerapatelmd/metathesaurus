#' @title
#' Scrape UMLS Documentation into Tables
#' @description
#' These functions scrape the UMLS Web Documentation to write the \code{\link{mth_documentation_functions}}. They have been kept to enable reproducibility and other long-term maintenance efforts.
#' @keywords internal
#' @importFrom xml2 read_html
#' @importFrom rvest html_nodes html_table
#' @importFrom purrr map map2 pluck
#' @importFrom stringr str_replace_all
#' @importFrom dplyr mutate_all
#' @importFrom rubix call_mr_clean
#' @importFrom broca makeTribble
#' @importFrom secretary press_enter
#' @name documentation_scraping_functions
NULL

#' @title
#' Scrape Field Value Annotations
#' @inherit documentation_scraping_functions title description
#' @keywords internal

scrape_field_value_annotations <-
        function() {
                origin <- xml2::read_html("https://www.nlm.nih.gov/research/umls/knowledge_sources/metathesaurus/release/abbreviations.html")

                tables <-
                        origin %>%
                        rvest::html_nodes("table") %>%
                        rvest::html_table()

                names(tables) <-
                tables %>%
                        purrr::map(~colnames(.)[1]) %>%
                        purrr::map(stringr::str_replace_all, "[\r\n\t]", " ") %>%
                        stringr::str_replace_all(pattern = "(^.*?)( |[(]{1})(.*$)",
                                                 replacement = "\\1")
                tables <-
                        tables %>%
                        purrr::map(function(x) x %>%
                                                dplyr::mutate_all(stringr::str_replace_all, "[\r\n\t]", " ")) %>%
                        purrr::map(function(x) x %>%
                                           dplyr::mutate_all(stringr::str_replace_all, "[ ]{2,}", " ")) %>%
                        purrr::map(rubix::call_mr_clean)

                tribble_part <- list()
                for (i in 1:length(tables)) {
                                tribble_part[[i]] <-
                                       capture.output(broca::makeTribble(tables[[i]]))
                }
                names(tribble_part) <- names(tables)
                tribble_part <-
                        tribble_part %>%
                        purrr::map(function(x) c("tibble::tribble(",
                                                 x,
                                                 ")")) %>%
                        purrr::map(~paste(., collapse = "\n"))
                # Add names
                output <-
                tribble_part %>%
                        purrr::map2(names(tribble_part),
                                    function(x,y) paste0(y , " = ", x))

                for (i in 1:length(output)) {
                        cat(output[[i]])
                        secretary::press_enter()
                }
        }


#' @title
#' Scrape Vocabulary Annotations
#' @inherit documentation_scraping_functions title description
#' @keywords internal

scrape_vocabulary_annotations <-
        function() {
                origin <- xml2::read_html("https://www.nlm.nih.gov/research/umls/sourcereleasedocs/index.html")

                tables <-
                        origin %>%
                        rvest::html_nodes("table") %>%
                        rvest::html_table() %>%
                        purrr::pluck(1)

                broca::makeTribble(tables)
        }
