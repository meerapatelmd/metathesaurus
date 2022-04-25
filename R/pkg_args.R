#' @title
#' Run Functions
#'
#' @description
#' These functions setup the instance of the given DBMS.
#' All SQL Statements that fail to execute are printed back
#' in the console at the conclusion of execution. It is important
#' to note that the schemas are dropped and rewritten with the
#' run and all present tables will be lost.
#'
#' @param conn          Database connection. For MySQL5.5,
#' the connection made by RMySQL's dbConnect and for Postgres,
#' the connection via DatabaseConnector.
#' @param conn_fun      Instead of supplying of a direct
#' database connection, an expression can be supplied that
#' automatically connects and disconnects the connection
#' once the function is run.
#' @param schema        For Postgres executions only, the
#' schema to write all the tables to, Default: 'umls'
#' @param rrf_dir       Path to directory to the RRF Files
#' either unpackaged by direct download or produced by Metamorphosys
#' @param mrconso_only  Setup only the MRCONSO Table? Default: FALSE
#' @param omop_only     Setup the MRCONSO, MRHIER, MRMAP,
#' MRSMAP, MRSAT, AND MRREL Tables only? Default: FALSE
#' @param english_only  Setup only the ENG Tables? Default: TRUE
#'
#' @name pkg_args
NULL
