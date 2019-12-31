## Requirements
1. MySQL version 5.5 server installed via MacPorts (Prerequisites are most current XCode and XCode Command Line tools). More information can be found here: https://trac.macports.org/wiki/howto/MySQL.  
 -/opt/local/etc/mysql55/my.cnf is a good place to customize your mysql55 installation.  
 -Socket: /opt/local/var/run/mysql55/mysqld.sock
 -mysql -u root -p
 -mysql> CREATE DATABASE umls;
 -mysql -u root -p --local-infile umls
 -mysql> SHOW PROCESSLIST  
 
 2. Download and install most current version of UMLS Metathesaurus  
  -Unzip downloaded file
  -Unzip mmys.zip and move unzipped contents into root folder
  -Execute shell script either from terminal or using the open_metamorphysis function using the path to run_mac.sh as the argument
  -Install UMLS Metamorphysis (current configurations are all English vocabularies available). Time estimations for installation are approximately 45-1 hour, but this is dependent on the configurations chosen.
  -The user designated destination directory will have the following directory tree: 
                a. original_version_folder > LEX, NET, and META subfolders
                b. RRF files
                c. Etc... files

## NOTES  
- Every run of Metamorphoysis can create mySQL scripts for the NET/ outputs, but will not generate one for the LEX/ or META/ outputs. shell scripts found in ./shell can be used as templates to populate a database using the output files. (LEX/ shell script has not been written yet, but META has been.)  
- Shell scripts for META/ is modeled after OHDSI's Vocabulary 5.0, which only uses the following RRF files: MRCONSO.RRF, MRHIER.RRF, MRMAP.RRF, MRSMAP.RRF, MRSAT.RRF, MRREL.RRF. If other tables are desired in future iterations, _01A_instantiate_umls_02.R_ would need to be modified to reflect this. All the default tables in NET/ are in the umls database.





