import pathlib
here: pathlib.Path = pathlib.Path(__file__).parent

if __name__ == "__main__":
    import os
    os.chdir(here.parent)

import json
import utils
import bw2io
import bw2data
import link
from functools import partial
from lcia import (import_SimaPro_LCIA_methods,
                  register_biosphere,
                  register_SimaPro_LCIA_methods,
                  add_damage_normalization_weighting,
                  write_biosphere_flows_and_method_names_to_XLSX)

from lci import (unregionalize,
                 import_SimaPro_LCI_inventories,
                 import_XML_LCI_inventories,
                 migrate_from_excel_file,
                 migrate_from_JSON_file,
                 create_XML_biosphere)

from harmonization import (create_harmonized_biosphere_migration,
                           create_dataframe_of_elementary_flows_that_are_not_used_in_methods)


#%% File- and folderpaths, key variables

# LCI and LCIA data
LCIA_SimaPro_CSV_folderpath: pathlib.Path = here.parent / "data" / "lcia"
ecoinvent_simapro_folderpath: pathlib.Path = here.parent / "data" / "lci" / "ECO_fromSimaPro"
ecoinvent_xml_folderpath: pathlib.Path = here.parent / "data" / "lci" / "ECO_fromXML"
agribalyse_simapro_folderpath: pathlib.Path = here.parent / "data" / "lci" / "AGB_fromSimaPro"

# Generic and Brightway
output_path: pathlib.Path = here / "notebook_data"
output_path.mkdir(exist_ok = True)
project_name: str = "notebook"

# Migration
filename_biosphere_migration_data: str = "biosphere_migration.json"
filepath_biosphere_migration_data: pathlib.Path = output_path / filename_biosphere_migration_data

#%% Defaults for key variables
biosphere_db_name: str = "biosphere3"
unlinked_biosphere_db_name: str = biosphere_db_name + " - unlinked"
ecoinvent_db_name_simapro: str = "ecoinvent v3.10 - SimaPro"
ecoinvent_db_name_xml: str = "ecoinvent v3.10 - XML"
agribalyse_db_name_simapro: str = "Agribalyse v3.1 - SimaPro"

#%% Import SimaPro LCIA methods and create biosphere database
methods: list[dict] = import_SimaPro_LCIA_methods(path_to_SimaPro_CSV_LCIA_files = LCIA_SimaPro_CSV_folderpath,
                                                  encoding = "latin-1",
                                                  delimiter = "\t",
                                                  verbose = True)

register_biosphere(Brightway_project_name = project_name,
                   BRIGHTWAY2_DIR = output_path,
                   biosphere_db_name = biosphere_db_name,
                   imported_methods = methods,
                   verbose = True)

register_SimaPro_LCIA_methods(imported_methods = methods,
                              biosphere_db_name = biosphere_db_name,
                              Brightway_project_name = project_name,
                              BRIGHTWAY2_DIR = output_path,
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

#%% Import ecoinvent LCI database 
ecoinvent_db_simapro: bw2io.importers.base_lci.LCIImporter = import_SimaPro_LCI_inventories(SimaPro_CSV_LCI_filepaths = [ecoinvent_simapro_folderpath / "ECO.CSV"],
                                                                                            db_name = ecoinvent_db_name_simapro,
                                                                                            encoding = "latin-1",
                                                                                            delimiter = "\t",
                                                                                            verbose = True)
ecoinvent_db_simapro.apply_strategy(unregionalize)

ecoinvent_db_simapro.apply_strategy(partial(link.link_biosphere_flows_externally,
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
ecoinvent_db_simapro.apply_strategy(partial(link.link_activities_internally,
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

# Delete ecoinvent database if already existing
if ecoinvent_db_name_simapro in bw2data.databases:
    print("------- Delete database: " + ecoinvent_db_name_simapro)
    del bw2data.databases[ecoinvent_db_name_simapro]

# Write database
print("------- Write database: " + ecoinvent_db_name_simapro)
ecoinvent_db_simapro.write_database()


#%% Import Agribalyse LCI database 
agribalyse_db_simapro: bw2io.importers.base_lci.LCIImporter = import_SimaPro_LCI_inventories(SimaPro_CSV_LCI_filepaths = [agribalyse_simapro_folderpath / "AGB.CSV"],
                                                                                             db_name = agribalyse_db_name_simapro,
                                                                                             encoding = "latin-1",
                                                                                             delimiter = "\t",
                                                                                             verbose = True)
agribalyse_db_simapro.apply_strategy(unregionalize)

agribalyse_db_simapro.apply_strategy(partial(link.link_biosphere_flows_externally,
                                             biosphere_db_name = biosphere_db_name,
                                             biosphere_db_name_unlinked = unlinked_biosphere_db_name,
                                             other_biosphere_databases = None,
                                             linking_order = None,
                                             relink = False,
                                             strip = True,
                                             case_insensitive = True,
                                             remove_special_characters = False,
                                             verbose = True), verbose = True)

agribalyse_db_simapro.apply_strategy(partial(migrate_from_excel_file,
                                             excel_migration_filepath = agribalyse_simapro_folderpath / "custom_migration_AGB.xlsx"),
                                    verbose = True)
agribalyse_db_simapro.apply_strategy(partial(link.link_activities_internally,
                                             production_exchanges = True,
                                             substitution_exchanges = True,
                                             technosphere_exchanges = True,
                                             relink = False,
                                             strip = True,
                                             case_insensitive = True,
                                             remove_special_characters = False,
                                             verbose = True), verbose = True)
agribalyse_db_simapro.statistics()

# Make a new biosphere database for the flows which are currently not linked
# Add unlinked biosphere flows with a custom function
unlinked_biosphere_flows: dict = utils.add_unlinked_flows_to_biosphere_database(db = agribalyse_db_simapro,
                                                                                biosphere_db_name_unlinked = unlinked_biosphere_db_name,
                                                                                biosphere_db_name = biosphere_db_name,
                                                                                add_to_existing_database = True,
                                                                                verbose = True)
agribalyse_db_simapro.statistics()

# Delete ecoinvent database if already existing
if agribalyse_db_name_simapro in bw2data.databases:
    print("------- Delete database: " + agribalyse_db_name_simapro)
    del bw2data.databases[agribalyse_db_name_simapro]

# Write database
print("------- Write database: " + agribalyse_db_name_simapro)
agribalyse_db_simapro.write_database()


#%% Create JSON files containing biosphere flow data

# Create the biosphere from XML files
biosphere_flows_from_XML: list[dict] = create_XML_biosphere(filepath_ElementaryExchanges = ecoinvent_xml_folderpath / "ecoinvent 3.10_cutoff_ecoSpold02" / "MasterData" / "ElementaryExchanges.xml",
                                                            biosphere_db_name = biosphere_db_name)

# Create the JSON object to be written
biosphere_flows_from_XML_json: dict = json.dumps({idx: m for idx, m in enumerate(biosphere_flows_from_XML)}, indent = 4)

# Write the unlinked biosphere data dictionary to a JSON file
with open(output_path / ("biosphere_flows_from_XML.json"), "w") as outfile:
    outfile.write(biosphere_flows_from_XML_json)

# Create the biosphere from registered biosphere database (here from SimaPro)
biosphere_flows_from_SimaPro: list[dict] = bw2data.Database(biosphere_db_name)

# Create the JSON object to be written
biosphere_flows_from_SimaPro_json: dict = json.dumps({idx: dict(m) for idx, m in enumerate(biosphere_flows_from_SimaPro)}, indent = 4)

# Write the unlinked biosphere data dictionary to a JSON file
with open(output_path / ("biosphere_flows_from_SimaPro.json"), "w") as outfile:
    outfile.write(biosphere_flows_from_SimaPro_json)

manually_checked_SBERTs: None = None

# Harmonize the two biospheres
biosphere_migration_data: dict = create_harmonized_biosphere_migration(biosphere_flows_1 = biosphere_flows_from_SimaPro,
                                                                       biosphere_flows_2 = biosphere_flows_from_XML,
                                                                       manually_checked_SBERTs = None,
                                                                       output_path = output_path,
                                                                       ecoquery_username = None,
                                                                       ecoquery_password = None)

# Create the JSON object to be written
biosphere_migration_data_in_json_format = json.dumps(biosphere_migration_data, indent = 3)

# Write the biosphere dictionary to a JSON file
with open(filepath_biosphere_migration_data, "w") as outfile:
   outfile.write(biosphere_migration_data_in_json_format)

#%% Import ecoinvent LCI database (XML)
ecoinvent_db_xml: bw2io.importers.ecospold2.SingleOutputEcospold2Importer = import_XML_LCI_inventories(XML_LCI_filepath = ecoinvent_xml_folderpath / "ecoinvent 3.10_cutoff_ecoSpold02" / "datasets",
                                                                                                       db_name = ecoinvent_db_name_xml,
                                                                                                       biosphere_db_name = biosphere_db_name,
                                                                                                       db_model_type_name = "cutoff",
                                                                                                       db_process_type_name = "unit",
                                                                                                       verbose = True)
# Apply biosphere migration
ecoinvent_db_xml.apply_strategy(partial(migrate_from_JSON_file,
                                        JSON_migration_filepath = filepath_biosphere_migration_data),
                                verbose = True)

ecoinvent_db_xml.apply_strategy(partial(link.remove_linking,
                                        production_exchanges = True,
                                        substitution_exchanges = True,
                                        technosphere_exchanges = True,
                                        biosphere_exchanges = True), verbose = True)

# Apply internal linking of activities
ecoinvent_db_xml.apply_strategy(partial(link.link_activities_internally,
                                        production_exchanges = True,
                                        substitution_exchanges = True,
                                        technosphere_exchanges = True,
                                        relink = False,
                                        case_insensitive = True,
                                        strip = True,
                                        remove_special_characters = False,
                                        verbose = True), verbose = True)

# Apply external linking of biosphere flows
ecoinvent_db_xml.apply_strategy(partial(link.link_biosphere_flows_externally,
                                        biosphere_connected_to_methods = True,
                                        biosphere_NOT_connected_to_methods = False,
                                        relink = False,
                                        case_insensitive = True,
                                        strip = True,
                                        remove_special_characters = False,
                                        verbose = True), verbose = True)

# Write unlinked biosphere flows to XLSX
print("\n-----------Write unlinked flows")
ecoinvent_db_xml.write_excel(only_unlinked = True)
    
# Show statistic of current linking of database import
print("\n-----------Linking statistics of current database import")
ecoinvent_db_xml.statistics()
print()
 
# Make a new biosphere database for the flows which are currently not linked
# Add unlinked biosphere flows with a custom function
unlinked_biosphere_flows: dict = utils.add_unlinked_flows_to_biosphere_database(db = ecoinvent_db_xml,
                                                                                biosphere_db_name_unlinked = unlinked_biosphere_db_name,
                                                                                biosphere_db_name = biosphere_db_name,
                                                                                add_to_existing_database = True,
                                                                                verbose = True)
# Show statistic of current linking of database import
print("\n-----------Linking statistics of current database import")
ecoinvent_db_xml.statistics()
print()
    
# Write database
print("\n-----------Write database: " + ecoinvent_db_name_xml)
ecoinvent_db_xml.write_database()
print()
        


#%% Next steps
# Correspondence files
# Update AGB ECO background