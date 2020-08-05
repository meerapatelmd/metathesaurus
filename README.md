# metathesaurus-setup  
## Requirements  
1. MySQL Database `umls`  
* MySQL version 5.5 server can be installed via MacPorts (Prerequisites are most current XCode and XCode Command Line tools). More information can be found here: https://trac.macports.org/wiki/howto/MySQL.  
* /opt/local/etc/mysql55/my.cnf is a good place to customize your mysql55 installation.  
* Socket: /opt/local/var/run/mysql55/mysqld.sock   
* To create `umls` database using `root` user:   
  
         ```
         -mysql -u root -p  
         -mysql> CREATE DATABASE umls;  
         -mysql -u root -p --local-infile umls  
         -mysql> SHOW PROCESSLIST
         ```
   
 2. Download and Install UMLS Metathesaurus  
 * Latest Full Release can be downloaded at https://www.nlm.nih.gov/research/umls/licensedcontent/umlsknowledgesources.html  
  * Unzip downloaded file  
  * Unzip mmys.zip in the unpacked download and move unzipped contents into a root folder  
  * Run `openMetamorphysis()` with path to `run_mac.sh` as the argument  
  * Install UMLS Metamorphysis (current configurations are all English vocabularies available). Time estimations for installation are approximately 45 minutes-1 hour, but this depends on the configurations.  
  * The user designated destination directory will have the following directory tree:  
                a. original_version_folder > LEX, NET, and META subfolders
                b. RRF files
                c. Etc... files  

## Related Packages  
I wrote a querying package called "metaorite" that can be downloaded at patelm9/metaorite that aids in mapping using Metathesaurus.

## Settings
Metamorphosys saves output to the UMLS/OUTPUT/{version}/ Directory. MySQL scripts for the NET subdir are executed, but MySQL scripts are not
available for the META or LEX outputs and need to be generated in-house. For this iteration the following RRF files are chosen to populate
our mySQL umls database in their respective tables because these are the files used for OMOP Vocabulary 5.0:
     MRCONSO.RRF
     MRHIER.RRF
     MRMAP.RRF
     MRSMAP.RRF
     MRSAT.RRF
     MRREL.RRF
shell/mysql_meta_tables.sql was forked from the load_source_tables.sql found at https://github.com/patelm9/Vocabulary-v5.0/tree/master/UMLS
and the LOAD DATA INTO... statements were added to populate the tables. The following script will create new LOAD DATA INTO statements if
additional tables/rrfs are desired in the umls database in the future. However, the CREATE TABLE functions would still need to be written
to mysql_meta_tables.sql


## Notes    
The MySQL5.6 loading scripts that come packaged with the Metathesaurus have historically run errors and scripts modeled after OHDI's Vocabulary5.0 are used instead (found in shell/)  
Shell scripts are present for NET/ and META/ outputs, but not for the LEX/, which requires downloading additional tools such as lvg. The shell scripts can be invoked as follows once in the subset directory containing all the NET/ or META/ outputs:
        % cd <subset directory>  
        % chmod 775 populate_mysql_db.sh  
        % populate_mysql_db.sh &  




