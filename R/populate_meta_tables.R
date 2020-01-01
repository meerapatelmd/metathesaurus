
populate_meta_tables <-
        function(path_to_shell_script = "shell/populate_meta_mysql_db.sh",
                 path_to_meta = "UMLS/OUTPUT") {
                system(paste0("cd ", path_to_meta, "\n"))
                system(path_to_shell_script)
        }
