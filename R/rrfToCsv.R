#' @title
#' RRF to CSV
#' @seealso
#'  \code{\link[progress]{progress_bar}}
#'  \code{\link[cave]{strip_fn}}
#'  \code{\link[readr]{read_delim}}
#'  \code{\link[rubix]{rm_multibyte_chars}},\code{\link[rubix]{call_mr_clean}}
#'  \code{\link[broca]{simply_write_csv}}
#' @rdname rrfToCsv
#' @export
#' @importFrom progress progress_bar
#' @importFrom cave strip_fn
#' @importFrom readr read_delim
#' @importFrom rubix rm_multibyte_chars call_mr_clean
#' @importFrom broca simply_write_csv
#' @importFrom magrittr %>%

rrfToCsv <-
        function(path) {
                rrfFiles <- list.files(path = path, pattern = "RRF$", full.names = T)
                rrfFiles <-
                        c(grep(pattern = "[_]{1}", rrfFiles, invert = TRUE, value= TRUE),
                          grep(pattern = "ENG", rrfFiles, value= TRUE))


                pb <- progress::progress_bar$new(format = ":percent [:bar] :elapsedfull :current/:total",
                                                 total = length(rrfFiles))

                pb$tick(0)
                Sys.sleep(0.2)

                for (i in 1:length(rrfFiles)) {
                        rrfFile <- rrfFiles[i]

                        csvFile <- paste0(cave::strip_fn(rrfFile), ".csv")

                        if (!file.exists(csvFile)) {
                                        rrf <- readr::read_delim(rrfFile,
                                                                 quote = "",
                                                                 delim = "|",
                                                                 col_names = FALSE,
                                                                 col_types = cols(.default = "c"))

                                        rrf <-
                                                rrf %>%
                                                rubix::rm_multibyte_chars() %>%
                                                rubix::call_mr_clean()

                                        broca::simply_write_csv(rrf,
                                                                file = csvFile)
                        }

                        cat("\n")
                        pb$tick()
                        Sys.sleep(0.2)
                }
        }
