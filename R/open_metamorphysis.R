
open_metamorphysis <-
        function(path_to_mmsys) {
                command <- mirroR::create_path_to_file(path_to_mmsys,
                                                       "run_mac.sh"
                                                       )
                system(command)
        }

