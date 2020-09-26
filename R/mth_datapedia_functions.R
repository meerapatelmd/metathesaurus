

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





mrsat_data_definitions <-
        function() {
                tibble::tribble(~Col.,~Description,
                                'COL', 'Column or data element name',
                                'DES', 'Descriptive Name',
                                'REF', 'Documentation Section Number',
                                'MIN', 'Minimum Length, Characters',
                                'AV', 'Average Length',
                                'MAX', 'Maximum Length, Characters',
                                'FIL', 'Physical FILENAME in which this field occurs',
                                'DTY', 'SQL-92 data type for this column')
        }



mrdef_data_definitions <-
        function() {
                tibble::tribble(~Col.,~Description,
                                'DOCKEY', 'Data element or attribute',
                                'VALUE', 'Abbreviation that is one of its values',
                                'TYPE', 'Type of information in EXPL column',
                                'EXPL', 'Explanation of VALUE')
        }



mrhist_data_definitions <-
        function() {
                tibble::tribble(~Col.,~Description,
                                'CUI', 'Unique identifier of concept',
                                'TUI', 'Unique identifier of Semantic Type',
                                'STN', 'Semantic Type tree number',
                                'STY', 'Semantic Type. The valid values are defined in the Semantic Network.',
                                'ATUI', 'Unique identifier for attribute',
                                'CVF', 'Content View Flag. Bit field used to flag rows included in Content View. This field is a varchar field to maximize the number of bits available for use.')
        }


mrrel_data_definitions <-
        function() {
                tibble::tribble(
                        ~Col.,~Description,
                        'LUI', 'Lexical Unique Identifier',
                        'CUI', 'Concept Unique Identifier'
                )
        }



mrcoc_data_definitions <-
        function() {
                tibble::tribble(
                        ~Col.,~Description,
                        'SUI', 'String Unique Identifier',
                        'CUI', 'Concept Unique Identifier'
                )
        }

mrhier_data_definitions <-
        function() {

                tibble::tribble(
                        ~Col.,~Description,
                        'PCUI', 'Concept Unique Identifier in the previous Metathesaurus',
                        'PSTR', 'Preferred name of this concept in the previous Metathesaurus'
                )

        }


mrcxt_data_definitions <-
        function() {

                tibble::tribble(
                        ~Col.,~Description,
                        'PCUI1', 'Concept Unique Identifier in the previous Metathesaurus',
                        'CUI', 'Concept Unique Identifier in this Metathesaurus in format C#######'
                )

        }

map_data_definitions <-
        function() {

                tibble::tribble(
                        ~Col.,~Description,
                        'PLUI', 'Lexical Unique Identifier in the previous Metathesaurus',
                        'PSTR', 'Preferred Name of Term in the previous Metathesaurus'
                )

        }


map_data_definitions2 <-
        function() {

                tibble::tribble(
                        ~Col.,~Description,
                        'PLUI', 'Lexical Unique Identifier in the previous Metathesaurus but not present in this Metathesaurus',
                        'LUI', 'Lexical Unique Identifier into which it was merged in this Metathesaurus'
                )




        }



mrsab_data_definitions <-
        function() {

                tibble::tribble(
                        ~Col.,~Description,
                        'PSUI', 'String Unique Identifier in the previous Metathesaurus that is not present in this Metathesaurus',
                        'PSTR', 'Preferred Name of Term in the previous Metathesaurus that is not present in this Metathesaurus'
                )

        }


#' @title
#' Word Index (File = MRXW_BAQ.RRF, MRXW_DAN.RRF, MRXW_DUT.RRF, MRXW_ENG.RRF, MRXW_FIN.RRF, MRXW_FRE.RRF, MRXW_GER.RRF, MRXW_HEB.RRF, MRXW_HUN.RRF, MRXW_ITA.RRF, MRXW_NOR.RRF, MRXW_POR.RRF, MRXW_RUS.RRF, MRXW_SPA.RRF, MRXW_SWE.RRF)
#'
#' @details
#' There is one row in these tables for each word found in each unique Metathesaurus string (ignoring upper-lower case). All Metathesaurus entries have entries in the word index. The entries are sorted in ASCII order.
#'
#' @seealso
#'  \code{\link[tibble]{tribble}}
#' @export
#' @importFrom tibble tribble

MRXW_def <-
        function() {

                tibble::tribble(
                        ~Col.,~Description,
                        'LAT', 'Abbreviation of language of the string in which the word appears',
                        'WD', 'Word in lowercase',
                        'CUI', 'Concept identifier',
                        'LUI', 'Term identifier',
                        'SUI', 'String identifier'
                )

        }


#' @title
#' Normalized Word Index (File = MRXNW_ENG.RRF)
#'
#' @details
#' There is one row in this table for each normalized word found in each unique English-language Metathesaurus string. All English-language Metathesaurus entries have entries in the normalized word index. There are no normalized string indexes for other languages in the Metathesaurus.
#'
#' @seealso
#'  \code{\link[tibble]{tribble}}
#' @export
#' @importFrom tibble tribble


MRXNW_ENG_def <-
        function() {

                tibble::tribble(
                        ~Col.,~Description,
                        'LAT', 'Abbreviation of language of the string in which the word appears (always ENG in this edition of the Metathesaurus)',
                        'NWD', 'Normalized word in lowercase (described in Section 2.7.2.1)',
                        'CUI', 'Concept identifier',
                        'LUI', 'Term identifier',
                        'SUI', 'String identifier'
                )



        }



#' @title
#' Normalized String Index (File = MRXNS_ENG.RRF)
#' @details
#' There is one row in this table for each normalized string found in each unique English-language Metathesaurus string (ignoring upper-lower case). All English-language Metathesaurus entries have entries in the normalized string index. There are no normalized word indexes for other languages in this edition of the Metathesaurus.
#'
#' @seealso
#'  \code{\link[tibble]{tribble}}
#' @rdname MRXNS_ENG_def
#' @export
#' @importFrom tibble tribble


MRXNS_ENG_def <-
        function() {

                tibble::tribble(
                        ~Col.,~Description,
                        'LAT', 'Abbreviation of language of the string (always ENG in this edition of the Metathesaurus)',
                        'NSTR', 'Normalized string in lowercase (described in Section 2.7.3.1)',
                        'CUI', 'Concept identifier',
                        'LUI', 'Term identifier',
                        'SUI', 'String identifier'
                )

        }




