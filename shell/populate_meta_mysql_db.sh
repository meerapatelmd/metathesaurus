#!/bin/sh -f
#
# For useful information on loading your Semantic Network files
# into a MySQL database, please consult the on-line
# documentation at:
#
# http://www.nlm.nih.gov/research/umls/load_scripts.html
#

#
# Database connection parameters
# Please edit these variables to reflect your environment
#
MYSQL_HOME=/opt/local/lib/mysql55
user=root
password=RZkzCKv5
db_name=umls

/bin/rm -f mysql_meta.log
touch mysql_meta.log
ef=0

echo "See mysql_meta.log for output"
echo "----------------------------------------" >> mysql_meta.log 2>&1
echo "Starting ... `/bin/date`" >> mysql_meta.log 2>&1
echo "----------------------------------------" >> mysql_meta.log 2>&1
echo "MYSQL_HOME = $MYSQL_HOME" >> mysql_meta.log 2>&1
echo "user =       $user" >> mysql_meta.log 2>&1
echo "db_name =    $db_name" >> mysql_meta.log 2>&1

echo "    Create and load tables ... `/bin/date`" >> mysql_meta.log 2>&1
$MYSQL_HOME/bin/mysql -vvv -u $user -p$password --local-infile $db_name < mysql_meta_tables.sql >> mysql_meta.log 2>&1
if [ $? -ne 0 ]; then ef=1; fi


echo "----------------------------------------" >> mysql_meta.log 2>&1
if [ $ef -eq 1 ]
then
echo "There were one or more errors.  Please reference the mysql_meta.log file for details." >> mysql_meta.log 2>&1
else
echo "Completed without errors." >> mysql_meta.log 2>&1
fi
echo "Finished ... `/bin/date`" >> mysql_meta.log 2>&1
echo "----------------------------------------" >> mysql_meta.log 2>&1
