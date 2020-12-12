library(metathesaurus)
conn <- chariot::connectAthena()
setup_pg_mth(conn = conn,
             schema = "mth",
             rrf_dir = "~/Desktop/2020AB/META/")
chariot::dcAthena(conn = conn)
