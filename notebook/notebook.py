import pathlib
here: pathlib.Path = pathlib.Path(__file__).parent

if __name__ == "__main__":
    import os
    os.chdir(here.parent)

import utils
import bw2io
import bw2data
import linking
from functools import partial
from lcia import (import_SimaPro_LCIA_methods,
                  register_biosphere,
                  register_SimaPro_LCIA_methods,
                  add_damage_normalization_weighting,
                  write_biosphere_flows_and_method_names_to_XLSX)

from lci import (unregionalize,
                 import_SimaPro_LCI_inventories,
                 migrate_from_excel_file)



LCIA_SimaPro_CSV_folderpath: pathlib.Path = here.parent / "data" / "lcia"
ecoinvent_simapro_folderpath: pathlib.Path = here.parent / "data" / "lci" / "ECO_fromSimaPro"
output_path: pathlib.Path = here / "notebook_data"

biosphere_db_name: str = "biosphere3"
unlinked_biosphere_db_name: str = biosphere_db_name + " - unlinked"
ecoinvent_db_name_simapro: str = "ecoinvent v3.10 - SimaPro"

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

ecoinvent_db_simapro: bw2io.importers.base_lci.LCIImporter = import_SimaPro_LCI_inventories(SimaPro_CSV_LCI_filepaths = [ecoinvent_simapro_folderpath / "ECO.CSV"],
                                                                                            db_name = ecoinvent_db_name_simapro,
                                                                                            encoding = "latin-1",
                                                                                            delimiter = "\t",
                                                                                            verbose = True)
ecoinvent_db_simapro.apply_strategy(unregionalize)

ecoinvent_db_simapro.apply_strategy(partial(linking.link_biosphere_flows_externally,
                                            biosphere_db_name = biosphere_db_name,
                                            biosphere_db_name_unlinked = unlinked_biosphere_db_name,
                                            other_biosphere_databases = None,
                                            linking_order = None,
                                            relink = False,
                                            strip = True,
                                            case_insensitive = True,
                                            remove_special_characters = False,
                                            verbose = True), verbose = True)

ecoinvent_db_simapro.apply_strategy(partial(migrate_from_excel_file,
                                            excel_migration_filepath = ecoinvent_simapro_folderpath / "custom_migration_ECO.xlsx"),
                                    verbose = True)
ecoinvent_db_simapro.apply_strategy(partial(linking.link_activities_internally,
                                            production_exchanges = True,
                                            substitution_exchanges = True,
                                            technosphere_exchanges = True,
                                            relink = False,
                                            strip = True,
                                            case_insensitive = True,
                                            remove_special_characters = False,
                                            verbose = True), verbose = True)
ecoinvent_db_simapro.statistics()

# Make a new biosphere database for the flows which are currently not linked
# Add unlinked biosphere flows with a custom function
unlinked_biosphere_flows: dict = utils.add_unlinked_flows_to_biosphere_database(db = ecoinvent_db_simapro,
                                                                                biosphere_db_name_unlinked = unlinked_biosphere_db_name,
                                                                                biosphere_db_name = biosphere_db_name,
                                                                                add_to_existing_database = True,
                                                                                verbose = True)
ecoinvent_db_simapro.statistics()

# Write database
print("------- Write database: " + ecoinvent_db_name_simapro)
ecoinvent_db_simapro.write_database()

print(bw2data.databases)


# Next steps
# Import AGB
# Import ECO XML
# Correspondence files
# Update AGB ECO background