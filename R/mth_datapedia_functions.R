

#' @description
#' Sourced from https://www.ncbi.nlm.nih.gov/books/NBK9685/table/ch03.T.concept_names_and_sources_file_mr/


mrconso_datapedia <-
        function() {
                tibble::tribble(~fields,~definitions,
                        'CUI', 'Unique identifier for concept',
                        'LAT', 'Language of term',
                        'TS', 'Term status',
                        'LUI', 'Unique identifier for term',
                        'STT', 'String type',
                        'SUI', 'Unique identifier for string',
                        'ISPREF', 'Atom status - preferred (Y) or not (N) for this string within this concept',
                        'AUI', 'Unique identifier for atom - variable length field, 8 or 9 characters',
                        'SAUI', 'Source asserted atom identifier [optional]',
                        'SCUI', 'Source asserted concept identifier [optional]',
                        'SDUI', 'Source asserted descriptor identifier [optional]',
                        'SAB', 'Abbreviated source name (SAB). Maximum field length is 20 alphanumeric characters. Two source abbreviations are assigned:
Root Source Abbreviation (RSAB) — short form, no version information, for example, AI/RHEUM, 1993, has an RSAB of "AIR"Versioned Source Abbreviation (VSAB) — includes version information, for example, AI/RHEUM, 1993, has an VSAB of "AIR93"
Official source names, RSABs, and VSABs are included on the UMLS Source Vocabulary Documentation page.',
                        'TTY', 'Abbreviation for term type in source vocabulary, for example PN (Metathesaurus Preferred Name) or CD (Clinical Drug). Possible values are listed on the Abbreviations Used in Data Elements page.',
                        'CODE', 'Most useful source asserted identifier (if the source vocabulary has more than one identifier), or a Metathesaurus-generated source entry identifier (if the source vocabulary has none)',
                        'STR', 'String',
                        'SRL', 'Source restriction level',
                        'SUPPRESS', 'Suppressible flag. Values = O, E, Y, or NO: All obsolete content, whether they are obsolesced by the source or by NLM. These will include all atoms having obsolete TTYs, and other atoms becoming obsolete that have not acquired an obsolete TTY (e.g. RxNorm SCDs no longer associated with current drugs, LNC atoms derived from obsolete LNC concepts). E: Non-obsolete content marked suppressible by an editor. These do not have a suppressible SAB/TTY combination.Y: Non-obsolete content deemed suppressible during inversion. These can be determined by a specific SAB/TTY combination explicitly listed in MRRANK.N: None of the aboveDefault suppressibility as determined by NLM (i.e., no changes at the Suppressibility tab in MetamorphoSys) should be used by most users, but may not be suitable in some specialized applications. See the MetamorphoSys Help page for information on how to change the SAB/TTY suppressibility to suit your requirements. NLM strongly recommends that users not alter editor-assigned suppressibility, and MetamorphoSys cannot be used for this purpose.',
                        'CVF', 'Content View Flag. Bit field used to flag rows included in Content View. This field is a varchar field to maximize the number of bits available for use.')
        }




table_titles <-
        function() {
                tibble::tribble(
                        ~Table Title,~RRF,
                        'Files', 'MRFILES.RRF',
                        'Data Elements', 'MRCOLS.RRF',
                        'Documentation for Abbreviated Values', 'MRDOC.RRF',
                        'Concept Names and Sources', 'MRCONSO.RRF',
                        'Simple Concept and Atom Attributes', 'MRSAT.RRF',
                        'Definitions', 'MRDEF.RRF',
                        'Semantic Types', 'MRSTY.RRF',
                        'History', 'MRHIST.RRF',
                        'Related Concepts', 'MRREL.RRF',
                        'NA', 'NA',
                        'Computable Hierarchies', 'MRHIER.RRF',
                        'Contexts', 'MRCXT.RRF',
                        'Mappings', 'MRMAP.RRF',
                        'Simple Mappings', 'MRSMAP.RRF',
                        'Source Information', 'MRSAB.RRF',
                        'Concept Name Ranking', 'MRRANK.RRF',
                        'Ambiguous Term Identifiers', 'AMBIGLUI.RRF',
                        'Ambiguous String Identifiers', 'AMBIGSUI.RRF',
                        'NA', 'NA',
                        'Word Index', 'MRXW_BAQ.RRF, MRXW_DAN.RRF, MRXW_DUT.RRF, MRXW_ENG.RRF, MRXW_FIN.RRF, MRXW_FRE.RRF, MRXW_GER.RRF, MRXW_HEB.RRF, MRXW_HUN.RRF, MRXW_ITA.RRF, MRXW_NOR.RRF, MRXW_POR.RRF, MRXW_RUS.RRF, MRXW_SPA.RRF, MRXW_SWE.RRF',
                        'Normalized Word Index', 'MRXNW_ENG.RRF',
                        'Normalized String Index', 'MRXNS_ENG.RRF')
        }




input <-
input %>%
        rvest::html_nodes("h3") %>%
        rvest::html_text() %>%
        tibble::as_tibble_col("h3") %>%
        tidyr::extract(col = h3,
                       into = c("Table Title", "RRF"),
                       regex = "^.*?([A-Za-z]{1,}.*?) [(]{1}File [=]{1} (.*RRF)[)]{1}")



%>%
        stringr::str_replace_all(pattern = "(^.* )([A-Za-z].*)")



