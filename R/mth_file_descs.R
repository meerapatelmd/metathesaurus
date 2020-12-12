#' @title
#' Metathesaurus File Descriptions
#'
#' @description
#' This family of functions returns the RRF file descriptions found at
#' \href{https://www.ncbi.nlm.nih.gov/books/NBK9685/}{https://www.ncbi.nlm.nih.gov/books/NBK9685/} as a tibble.
#'
#' @name mth_file_description_functions
NULL




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




