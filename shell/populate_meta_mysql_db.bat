::
:: For useful information on loading your Semantic Network files
:: into a MySQL database, please consult the on-line
:: documentation at:
::
:: http://www.nlm.nih.gov/research/umls/load_scripts.html
::

::
:: Database connection parameters
:: Please edit these variables to reflect your environment
::
set MYSQL_HOME=<path to MYSQL_HOME>
set user=<username>
set password=<password>
set db_name=<tns_name>

del mysql_meta.log
echo. > mysql_meta.log
echo ---------------------------------------- >> mysql_meta.log 2>&1
echo Starting ...  >> mysql_meta.log 2>&1
date /T >> mysql_meta.log 2>&1
time /T >> mysql_meta.log 2>&1
echo ---------------------------------------- >> mysql_meta.log 2>&1
echo MYSQL_HOME = %MYSQL_HOME% >> mysql_meta.log 2>&1
echo user =       %user% >> mysql_meta.log 2>&1
echo db_name =    %db_name% >> mysql_meta.log 2>&1
set error=0

echo     Create and load tables >> mysql_meta.log 2>&1
%MYSQL_HOME%\bin\mysql -vvv -u %user% -p%password% --local-infile=1 %db_name%  < mysql_meta_tables.sql >> mysql_meta.log 2>&1
IF %ERRORLEVEL% NEQ 0 (set error=1)

echo ---------------------------------------- >> mysql_meta.log 2>&1
IF %error% NEQ 0 (
echo There were one or more errors.  Please reference the mysql_meta.log file for details. >> mysql_meta.log 2>&1
) else (
echo Completed without errors. >> mysql_meta.log 2>&1
)
echo Finished ...  >> mysql_meta.log 2>&1
date /T >> mysql_meta.log 2>&1
time /T >> mysql_meta.log 2>&1
echo ---------------------------------------- >> mysql_meta.log 2>&1
