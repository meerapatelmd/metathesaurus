#' @importFrom glue glue

typewrite <-
        function(msg) {

                if (missing(msg)) {

                        msg <- ""

                }

                glue::glue(
                        "[{as.character(Sys.time())}] {msg}"
                )



        }

#' @importFrom cli combine_ansi_styles

italicize <-
        cli::combine_ansi_styles(
                "bold",
                "italic"
        )


#' @importFrom cli combine_ansi_styles
#' @importFrom glue glue

typewrite_warning <-
        function(msg) {


                warning_fmt <-
                        cli::combine_ansi_styles(
                                "darkorange",
                                "bold",
                                "italic"
                        )

                glue::glue(
                        "[{as.character(Sys.time())}] {warning_fmt(cli::symbol$warning)} {warning_fmt(msg)}"
                )



        }



