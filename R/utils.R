#' @title
#' Split After Semicolon
#' @family utils
#' @keywords internal

sqlsplit <-
  function(x, split, type = "remove", perl = FALSE, ...) {
    if (type == "remove") {
      out <- base::strsplit(
        x = x, split = split, perl = perl,
        ...
      )
    }
    else if (type == "before") {
      out <- base::strsplit(x = x, split = paste0(
        "(?<=.)(?=",
        split, ")"
      ), perl = TRUE, ...)
    }
    else if (type == "after") {
      out <- base::strsplit(x = x, split = paste0(
        "(?<=", split,
        ")"
      ), perl = TRUE, ...)
    }
    else {
      stop("type must be remove, after or before!")
    }
    return(out)
  }

#' Pipe operator
#'
#' See \code{magrittr::\link[magrittr:pipe]{\%>\%}} for details.
#'
#' @name %>%
#' @rdname pipe
#' @keywords internal
#' @export
#' @importFrom magrittr %>%
#' @usage lhs \%>\% rhs
NULL
