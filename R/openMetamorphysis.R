#' Open Metamorphosys
#' @param path_to_mmsys Path to `run_mac.sh` in unpacked Metamorphysis download
#' @export

openMetamorphosys <-
        function(path_to_mmsys) {

                command <- paste(path_to_mmsys,
                                 "/run_mac.sh",
                                 collapse = " ")

                system(command)
        }

