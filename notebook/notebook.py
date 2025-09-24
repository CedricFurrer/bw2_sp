import pathlib
here: pathlib.Path = pathlib.Path(__file__).parent

if __name__ == "__main__":
    import os
    os.chdir(here.parent)

from lcia import (import_SimaPro_LCIA_methods,
                  register_biosphere,
                  register_SimaPro_LCIA_methods,
                  add_damage_normalization_weighting,
                  write_biosphere_flows_and_method_names_to_XLSX)

LCIA_SimaPro_CSV_folderpath: pathlib.Path = here.parent / "data" / "lcia"
output_path: pathlib.Path = here / "notebook_data"

biosphere_db_name: str = "biosphere3"
unlinked_biosphere_db_name: str = biosphere_db_name + " - unlinked"

methods: list[dict] = import_SimaPro_LCIA_methods(path_to_SimaPro_CSV_LCIA_files = LCIA_SimaPro_CSV_folderpath,
                                                  encoding = "latin-1",
                                                  delimiter = "\t",
                                                  verbose = True)

register_biosphere(Brightway_project_name = "notebook",
                   BRIGHTWAY2_DIR = output_path,
                   biosphere_db_name = biosphere_db_name,
                   imported_methods = methods,
                   verbose = True)

register_SimaPro_LCIA_methods(imported_methods = methods,
                              biosphere_db_name = biosphere_db_name,
                              logs_output_path = output_path,
                              verbose = True)

add_damage_normalization_weighting(original_method = ("SALCA v2.01", "CEENE - Fossil fuels"),
                                   normalization_factor = 2,
                                   weighting_factor = None,
                                   damage_factor = None,
                                   new_method = ("SALCA v2.01", "CEENE - Fossil fuels", "normalized"),
                                   new_method_unit = "MJeq",
                                   new_method_description = "added a normalization factor of 2 to the original method",
                                   verbose = True)

write_biosphere_flows_and_method_names_to_XLSX(biosphere_db_name = biosphere_db_name,
                                               output_path = output_path,
                                               verbose = True)




