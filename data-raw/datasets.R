## code to prepare `datasets` dataset goes here
tty <- broca::simply_read_csv("data-raw/tty_description.csv")
tty_class <- broca::simply_read_csv("data-raw/tty_class_description.csv")
usethis::use_data(tty, tty_class, overwrite = TRUE)
