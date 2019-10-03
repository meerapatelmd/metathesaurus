## Requirements
1. MySQL version 5.5 server installed via MacPorts (Prerequisites are most current XCode and XCode Command Line tools). More information can be found here: https://trac.macports.org/wiki/howto/MySQL.  - /opt/local/etc/mysql55/my.cnf is a good place to customize your mysql55 installation.  
 -Socket: /opt/local/var/run/mysql55/mysqld.sock  
 -mysql -u root  -p  --local-infile umls
 -mysql> SHOW PROCESSLIST 
2. Empty MySQL5.5 'umls' database  
  
## NOTES  
- Every run of Metamorphoysis can create mySQL scripts for the NET/ outputs, but will not generate one for the LEX/ or META/ outputs. shell scripts found in ./shell can be used as templates to populate a database using the output files. (LEX/ shell script has not been written yet, but META has been.)  
- Shell scripts for META/ is modeled after OHDSI's Vocabulary 5.0, which only uses the following RRF files: MRCONSO.RRF, MRHIER.RRF, MRMAP.RRF, MRSMAP.RRF, MRSAT.RRF, MRREL.RRF. If other tables are desired in future iterations, _01A_instantiate_umls_02.R_ would need to be modified to reflect this. All the default tables in NET/ are in the umls database.





