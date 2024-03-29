% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/rxnorm_map.R
\name{get_rxnorm_map}
\alias{get_rxnorm_map}
\title{Get the RxNorm Map Between 2 TTY}
\usage{
get_rxnorm_map(
  conn,
  conn_fun = "pg13::local_connect()",
  from_tty = c("BN", "BPCK", "DF", "DFG", "ET", "GPCK", "IN", "MIN", "PIN", "PSN",
    "SBD", "SBDC", "SBDF", "SBDG", "SCD", "SCDC", "SCDF", "SCDG", "SY", "TMSY"),
  to_tty = c("BN", "BPCK", "DF", "DFG", "ET", "GPCK", "IN", "MIN", "PIN", "PSN", "SBD",
    "SBDC", "SBDF", "SBDG", "SCD", "SCDC", "SCDF", "SCDG", "SY", "TMSY"),
  full_path = FALSE,
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

\item{from_tty}{The RxNorm TTY to start from.}

\item{to_tty}{The RxNorm TTY to end at.}

\item{full_path}{By default, regardless of path length, only the \code{from_tty} and \code{to_tty}
values are returned. If TRUE, the entire path is returned instead.}

\item{schema}{For Postgres executions only, the
schema to write all the tables to, Default: 'umls'}

\item{verbose}{If TRUE, details on the activity are returned in the console, such as when the querying starting and was completed.}

\item{render_sql}{If TRUE, the SQL statement for the operation is returned in the console.}
}
\value{
Tibble of the mappings between the \code{aui}, \code{code}, and \code{str} between
the two start and end tty arguments. If \code{full_path} is TRUE, the intermediate tty
values are also provided. Every set is prefixed with the tty value. The returned map
is derived from a full join and therefore, blank mappings between tty are also included
in the tibble.
}
\description{
Get the RxNorm Map Between 2 TTY. This map
ignores any variability from the rel/rela.
}
\seealso{
Other RxNorm Map: 
\code{\link{get_rxnorm_ingredient_map}()},
\code{\link{get_rxnorm_tty_lookup}()},
\code{\link{read_rxnorm_paths}()},
\code{\link{setup_rxmap}()},
\code{\link{write_rxnorm_map}()},
\code{\link{write_rxnorm_path_lookup}()}
}
\concept{RxNorm Map}
