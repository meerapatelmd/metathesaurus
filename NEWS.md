# metathesaurus 4.0.0.9000   

* Added RxNorm Map family of functions  
* Introduced RxNorm processing built on the `PL/R` 
extension.  
* Added postprocessing steps are part of `run_setup()`  
* Fixed bug with `run_setup` where the `conn_fun` argument 
was not utilized.  
* Added SQL script for post-processing of the MRHIER 
table. 
* Removed deprecated `setup_pg()`, `scrape_*`  
* Removed MySQL setup  
* Change default schema from `mth` to `umls`  
* Removed dependency on `secretary` pkg  

# metathesaurus 4.0.0  

* Apply tidy style to R files  
* Add and incorporate `crosswalk` features 
as part of standard run   
* Remove deprecated functions and stale dependencies  


# metathesaurus 3.0.1  

* Add Metadata lookup page    
* Bug fix mrrank filtering for setup extension.  


# metathesaurus 3.0.0  

* Add Metathesaurus Extension  


# metathesaurus 2.2.0  

* Add logging feature to Postgres  

# metathesaurus 2.1.0  

* Add `steps` feature to `setup_pg_mth()`.  

# metathesaurus 2.0.0

* First release.  
