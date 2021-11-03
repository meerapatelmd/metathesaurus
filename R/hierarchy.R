render_hierarchy_sql <-
        function(schema = "umls_mrhier") {

                sql_template <-
                readLines(con = system.file(package = "metathesaurus",
                                            "sql",
                                            "hierarchy.sql"))

                sql_template <-
                        paste(sql_template,
                              collapse = "\n")


                as.character(glue::glue(sql_template))




        }


setup_hierarchy_schema <-
        function(conn,
                 conn_fun = "pg13::local_connect()",
                 schema = "umls_mrhier",
                 render_sql = TRUE,
                 verbose = TRUE) {


                sql_statement <-
                        render_hierarchy_sql(schema = schema)

                pg13::send(conn = conn,
                           conn_fun = conn_fun,
                           sql_statement = sql_statement,
                           render_sql = render_sql,
                           verbose = verbose)



        }
