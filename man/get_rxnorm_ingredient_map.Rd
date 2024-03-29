% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/rxnorm_map.R
\name{get_rxnorm_ingredient_map}
\alias{get_rxnorm_ingredient_map}
\title{Get RxNorm Ingredient Map}
\usage{
get_rxnorm_ingredient_map(
  conn,
  conn_fun = "pg13::local_connect()",
  schema = "umls",
  verbose = TRUE,
  render_sql = TRUE,
  render_only = FALSE,
  checks = ""
)
}
\arguments{
\item{conn}{Database connection via DatabaseConnector.}

\item{conn_fun}{Instead of supplying of a direct
database connection, an expression can be supplied that
automatically connects and disconnects the connection
once the function is run.}

\item{schema}{For Postgres executions only, the
schema to write all the tables to, Default: 'umls'}

\item{verbose}{If TRUE, details on the activity are returned in the console, such as when the querying starting and was completed.}

\item{render_sql}{If TRUE, the SQL statement for the operation is returned in the console.}
}
\value{
Tibble of each unique combination of
from tty 'IN' and an ending tty with an extra field that
provides the given tty value. The final tibble
as includes 'IN' mappings to itself.
}
\description{
Iterate on all the possible paths
out of the RxNorm Ingredient concepts
using the \code{get_rxnorm_map} function.
}
\seealso{
Other RxNorm Map: 
\code{\link{get_rxnorm_map}()},
\code{\link{get_rxnorm_tty_lookup}()},
\code{\link{read_rxnorm_paths}()},
\code{\link{setup_rxmap}()},
\code{\link{write_rxnorm_map}()},
\code{\link{write_rxnorm_path_lookup}()}
}
\concept{RxNorm Map}
