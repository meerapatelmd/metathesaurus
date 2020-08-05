#' Populate META Tables
#' @param path Path to the META subdirectory of unpacked UMLS Metathesaurus
#' @export

populateMeta <-
        function(path) {

                shellPath <- paste0(system.file(package = "setupMetathesaurus"), "/shell/populate_meta_mysql_db.sh")

                system(paste0("cd ", path, "\n", shellPath))

        }
