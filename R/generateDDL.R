#' @title
#' Get Data Elements
#' @description
#' Get the table found at \href{https://www.nlm.nih.gov/research/umls/knowledge_sources/metathesaurus/release/columns_data_elements.html} as a data frame
#' @seealso
#'  \code{\link[xml2]{read_xml}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_table}}
#' @rdname getDataElements
#' @export
#' @importFrom xml2 read_html
#' @importFrom rvest html_node html_table
#' @importFrom magrittr %>%

getDataElements <-
        function() {

                        url <- "https://www.nlm.nih.gov/research/umls/knowledge_sources/metathesaurus/release/columns_data_elements.html"
                        readUrl <- xml2::read_html(x = url)

                        output <-
                        readUrl %>%
                                rvest::html_node("table") %>%
                                rvest::html_table()

                        output
        }

#' @title
#' Derive DDL from Data Elements
#' @description
#' This function takes the data frame scraped by \code{getDataElements} and adds additional columns to generate new ddl scripts for UMLS Metathesaurus Tables.
#' @param dataElementsDf PARAM_DESCRIPTION
#' @return OUTPUT_DESCRIPTION
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[tibble]{rownames}}
#'  \code{\link[tidyr]{separate_rows}}
#'  \code{\link[dplyr]{filter}},\code{\link[dplyr]{rename}},\code{\link[dplyr]{mutate}}
#'  \code{\link[rubix]{call_mr_clean}}
#'  \code{\link[stringr]{str_replace}}
#' @rdname deriveDDLFromDataElements
#' @export
#' @importFrom tibble rowid_to_column
#' @importFrom tidyr separate_rows
#' @importFrom dplyr filter rename mutate
#' @importFrom rubix call_mr_clean
#' @importFrom stringr str_replace_all
#' @importFrom magrittr %>%

deriveDDLFromDataElements <-
        function(dataElementsDf) {
                output2 <-
                        dataElementsDf %>%
                        tibble::rowid_to_column("AbbreviationId") %>%
                        tidyr::separate_rows(File,
                                             sep = "[.]RRF") %>%
                        dplyr::filter(File != "") %>%
                        dplyr::rename(Table = File) %>%
                        dplyr::mutate(File = paste0(Table, ".RRF")) %>%
                        rubix::call_mr_clean()

                abbrLength <- max(as.integer(output2$AbbreviationId))

                if (abbrLength !=
                    output2 %>%
                    group_by(AbbreviationId) %>%
                    summarise(n = n(), .groups = "drop") %>%
                    nrow()) {

                        stop("Missing abbreviations")
                }

                output3 <-
                        output2 %>%
                        dplyr::mutate(ddlLine = stringr::str_replace_all(`SQL92 Datatype`,
                                                                     pattern = "(^.*?[(]{1}.*?[)]{1})(.*$)",
                                                                     replacement = "\\1"))

                output3

        }



