# metathesaurus <img src="man/figures/logo.png" align="right" alt="" width="120" />  
<a href="http://www.freepik.com">Designed by macrovector / Freepik</a>  

This package sets up an instance of the UMLS Metathesaurus in either the native MySQL5.5 or Postgres DBMS sourced from the RRF files downloaded and from https://www.nlm.nih.gov/research/umls/licensedcontent/umlsknowledgesources.html. The list of instantiated tables can be customized depending on user needs. These options include      

1. All MTH Tables
1. MRCONSO Table only     
1. OMOP Only (MRCONSO, MRHIER, MRMAP, MRSMAP, MRSAT, AND MRREL)      
1. English Only (Non-English Language Tables are excluded)     

All SQL scripts used in this package can be accessed at inst/sql to run directly in the client instead. If you'd like to run Metamorphosys to set configurations prior to installation, jump to [Metamorphosys](#metamorphosys)  


## Dependencies    

This package requires a connection by `RMySQL::dbConnect` or `DatabaseConnector::dbConnect` for MySQL and Postgres, respectively.     


## Related R Packages   

* [metaorite](https://github.com/meerapatelmd/metaorite/blob/master/README.md) to query the instance.   
* [callmemaybe](https://github.com/meerapatelmd/callMeMaybe/blob/master/README.md) to make API Calls to the UMLS REST API.    


## Installation    

```
devtools::install_github("meerapatelmd/setupMetathesaurus")
```


## Prerequisites      

### MySQL5.5        

* MySQL version 5.5 server can be installed via MacPorts (Prerequisites are most current XCode and XCode Command Line tools). More information at https://trac.macports.org/wiki/howto/MySQL   
* /opt/local/etc/mysql55/my.cnf is a good place to customize your mysql55 installation    
* Socket: /opt/local/var/run/mysql55/mysqld.sock   
* Example of creating a database named `umls` using `root` as user:    

         ```
         -mysql -u root -p  
         -mysql> CREATE DATABASE umls;  
         -mysql -u root -p --local-infile umls  
         -mysql> SHOW PROCESSLIST
         ```  
         
### Downloading UMLS Files   

* UMLS Metathesaurus Files can be downloaded at https://www.nlm.nih.gov/research/umls/knowledge_sources/metathesaurus/index.html and requires account setup.   

# Notes   

* MySQL5.5 scripts are sourced from the the scripts packaged with the 2020AA download   
* Postgres scripts were forked from https://www.nlm.nih.gov/research/umls/implementation_resources/community/index.html    


# MetamorphoSys    

MetamorphoSys is the UMLS installation and customization java application for local installation of all of the UMLS Knowledge Sources (MTH, Semantic Network, and SPECIALIST Lexicon). Metamorphosys also supports the creation of custom subsets of the Metathesaurus to meet specific use cases.  

A Full Release download at https://www.nlm.nih.gov/research/umls/licensedcontent/umlsknowledgesources.html is required to run Metamorphosys, at which point the user can select various specialized configurations desired in the MySQL or Postgres Tables, such as a specific set of source vocabularies. The total time estimations of downloading, configuring, and processing the Metathesaurus tables in this way is approximately 2-3 hours. To save time, the Metamorphosys step may be skipped if the user does not desire this type of customizability and the `UMLS Metathesaurus Files` can be downloaded directly at the link and used as the source files in lieu of the Full Release.    
    

**Setup and Run Metamorphosys**        
* Unpack the Full Release download    
* Unzip mmys.zip in the unpacked download and move unzipped contents into a root folder  
* Run `openMetamorphysis()` with path to `run_mac.sh` as the argument  
* If not yet installed, install UMLS Metamorphosys (current configurations are all English vocabularies available). Time estimations for installation are approximately 45 minutes-1 hour, but this depends on the configurations.    
* Run the remainder of setup using the path to the META/ output  

## Code of Conduct

Please note that the setupMetathesaurus project is released with a [Contributor Code of Conduct](https://contributor-covenant.org/version/2/0/CODE_OF_CONDUCT.html). By contributing to this project, you agree to abide by its terms.  


