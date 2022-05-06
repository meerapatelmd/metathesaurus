/**************************************************************************
* Derive entire hierarchies from UMLS Metathesaurus MRHIER Table
* Authors: Meera Patel
* Date: 2021-10-27
* https://lucid.app/lucidchart/b7e40e42-ea80-43be-baf5-7f92cbfb6d6f/edit?viewport_loc=-185%2C997%2C1560%2C929%2C0_0&invitationId=inv_61a38c21-37a1-49fa-a857-d7585471e5f1
*
* | MRHIER | --> | MRHIER |
* ptr_id is added to the source table. ptr_id is the source MRHIER's row number.
* It is added as an identifier for each unique AUI-RELA-PTR (ptr: Path To Root).
* Note that unlike the identifiers provided
* by the UMLS, this one cannot be used across different Metathesaurus
* versions.
*
* | MRHIER | --> | MRHIER_STR | + | MRHIER_STR_EXCL |
* MRHIER is then processed to replace the decimal-separated `ptr` string into
* individual atoms (`aui`) and mapped to the atom's `str` value. Any missing
* `ptr` values in `MRHIER_STR` are accounted for in the `MRHIER_STR_EXCL` table.
*
* To Do:
* [X] Cleanup scripts with functions from 1387 onward, including logging to
*     the progress log and annotations
* [ ] After 2021AB update, see how the lookup and results tables can be
*     renamed with a degree of provenance.
* [ ] Add the `sab` field back to final MRHIER_STR table by doing a join
*     to the MRCONSO table at this stage
* [X] Some log entries do not have target table row counts
* [ ] Log entries from `ext_` to `pivot_` do not have `sab` value
* [X] Re-imagine the RxClass log so that it is a 1-row entry per incidence (mimic setup_umls_class_log)
* [X] Change sort order of final tables in RxClass
* [ ] Change sort order of final tables in UMLS Class
* [ ] Add indexes to final UMLS Class tables
* [X] Add indexes to final RxClass tables
**************************************************************************/
