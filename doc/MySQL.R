## ---- include = FALSE, echo=FALSE---------------------------------------------
knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>"
)

## ----setup, echo=TRUE,eval=TRUE-----------------------------------------------
library(setupMetathesaurus)

## ----ddl, echo=TRUE, eval = FALSE---------------------------------------------
#  
#  ddlMeta(dbname = "umls",
#          username = Sys.getenv("umls_username"),
#          password = Sys.getenv("umls_password"))
#  

## ----load, echo = TRUE, eval = FALSE------------------------------------------
#  
#  loadMeta(path = "~/Desktop/2020AA/META",
#           username = Sys.getenv("umls_username"),
#           password = Sys.getenv("umls_password"))
#  
#  

## ----indices, echo = TRUE, eval = FALSE---------------------------------------
#  
#  runIndices(dbname = "umls",
#          username = Sys.getenv("umls_username"),
#          password = Sys.getenv("umls_password"))
#  

