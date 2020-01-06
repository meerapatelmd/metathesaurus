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
  -Install UMLS Metamorphysis (current configurations are all English vocabularies available). Time estimations for installation are approximately 45 minutes-1 hour, but this is dependent on the configurations chosen.
  -The user designated destination directory will have the following directory tree: 
                a. original_version_folder > LEX, NET, and META subfolders
                b. RRF files
                c. Etc... files

## Related Packages  
I wrote a querying package called "metaorite" that can be downloaded at patelm9/metaorite that aids in mapping using Metathesaurus.

## NOTES  
The MySQL5.6 loading scripts that come packaged with the Metathesaurus have historically run errors and scripts modeled after OHDI's Vocabulary5.0 are used instead (found in shell/)  
Shell scripts are present for NET/ and META/ outputs, but not for the LEX/, which requires downloading additional tools such as lvg. The shell scripts can be invoked as follows once in the subset directory containing all the NET/ or META/ outputs:
        % cd <subset directory>  
        % chmod 775 populate_mysql_db.sh  
        % populate_mysql_db.sh &  




