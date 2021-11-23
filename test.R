library(tidyverse)
library(pg13)

sql_statement <-
"
create or replace function notify_start(report varchar)
returns void
language plpgsql
as
$$
declare
  notice_timestamp timestamp;
begin
  SELECT get_log_timestamp()
  INTO notice_timestamp
  ;

  RAISE NOTICE '[%] Started %', notice_timestamp, report;
END;
$$;

PERFORM notify_start('Test Report');
"

send(conn_fun = "pg13::local_connect()",
     sql_statement = sql_statement)
