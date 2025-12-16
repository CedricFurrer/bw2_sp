import pathlib
here: pathlib.Path = pathlib.Path(__file__).parent

if __name__ == "__main__":
    import os
    os.chdir(here.parent)

import copy
import json
import utils
import bw2io
import bw2data
import link
import pandas as pd
from ast import literal_eval
from functools import partial
from lcia import (import_SimaPro_LCIA_methods,
                  import_XML_LCIA_methods,
                  register_biosphere,
                  register_SimaPro_LCIA_methods,
                  register_XML_LCIA_methods,
                  # add_damage_normalization_weighting,
                  write_biosphere_flows_and_method_names_to_XLSX)

from lci import (unregionalize_biosphere,
                 import_SimaPro_LCI_inventories,
                 import_XML_LCI_inventories,
                 migrate_from_excel_file,
                 migrate_from_json_file,
                 create_structured_migration_dictionary_from_excel,
                 create_XML_biosphere_from_elmentary_exchanges_file,
                 create_XML_biosphere_from_LCI,
                 select_inventory_using_regex)

from harmonization import (create_harmonized_biosphere_migration,
                           create_harmonized_activity_migration,
                           elementary_flows_that_are_not_used_in_XML_methods)

from harmonization_class import (ActivityHarmonization,
                                 ActivityDefinition)

# from correspondence.correspondence import (create_correspondence_mapping)
from correspondence.correspondence import Correspondence

from utils import (change_brightway_project_directory,
                   change_database_name)
from calculation import LCA_Calculation


#%% File- and folderpaths, key variables

# LCI and LCIA data
LCI_ecoinvent_simapro_folderpath: pathlib.Path = here.parent / "data" / "lci" / "ECO_fromSimaPro"
LCI_ecoinvent_xml_folderpath: pathlib.Path = here.parent / "data" / "lci" / "ECO_fromXML"
LCI_ecoinvent_xml_data_folderpath_temp: pathlib.Path = LCI_ecoinvent_xml_folderpath / "ecoinvent 3.11_cutoff_ecoSpold02"
LCI_ecoinvent_xml_data_folderpath: pathlib.Path = LCI_ecoinvent_xml_folderpath / "ecoinvent 3.12_cutoff_ecoSpold02"
LCI_ecoinvent_xml_datasets_folderpath_temp: pathlib.Path = LCI_ecoinvent_xml_data_folderpath_temp / "datasets"
LCI_ecoinvent_xml_datasets_folderpath: pathlib.Path = LCI_ecoinvent_xml_data_folderpath / "datasets"
LCI_agribalyse_simapro_folderpath: pathlib.Path = here.parent / "data" / "lci" / "AGB_fromSimaPro"
LCI_agrifootprint_simapro_folderpath: pathlib.Path = here.parent / "data" / "lci" / "AGF_fromSimaPro"
LCI_wfldb_simapro_folderpath: pathlib.Path = here.parent / "data" / "lci" / "WFLDB_fromSimaPro"
LCI_salca_simapro_folderpath: pathlib.Path = here.parent / "data" / "lci" / "SALCA_fromSimaPro"
LCIA_SimaPro_CSV_folderpath: pathlib.Path = here.parent / "data" / "lcia" / "fromSimaPro"
LCIA_XML_folderpath_temp: pathlib.Path = here.parent / "data" / "lcia" / "fromXML" / "ecoinvent 3.11_LCIA_implementation"
LCIA_XML_folderpath: pathlib.Path = here.parent / "data" / "lcia" / "fromXML" / "ecoinvent 3.12_LCIA_implementation"
LCIA_XML_filename_temp: str = "LCIA Implementation 3.11.xlsx"
LCIA_XML_filename: str = "LCIA Implementation 3.12.xlsx"
LCIA_XML_filepath_temp: pathlib.Path = LCIA_XML_folderpath_temp / LCIA_XML_filename_temp
LCIA_XML_filepath: pathlib.Path = LCIA_XML_folderpath / LCIA_XML_filename
LCIA_XML_sheetname: str = "CFs"
LCIA_XML_elementary_exchanges_filename: str = "ElementaryExchanges.xml"
LCIA_XML_elementary_exchanges_filepath_temp: pathlib.Path = LCI_ecoinvent_xml_data_folderpath_temp / "MasterData" / LCIA_XML_elementary_exchanges_filename
LCIA_XML_elementary_exchanges_filepath: pathlib.Path = LCI_ecoinvent_xml_data_folderpath / "MasterData" / LCIA_XML_elementary_exchanges_filename

# Generic and Brightway
if bw2data.__version__[0] >= 4:
    project_path: pathlib.Path = here  / "Brightway2.5_projects"
else:
    project_path: pathlib.Path = here / "Brightway2_projects"
    
project_path.mkdir(exist_ok = True)
project_name: str = "Brightway paper"

# Correspondence files
folderpath_correspondence_files: pathlib.Path = here.parent / "correspondence" / "data"

#%% Change brightway project directory and setup project
change_brightway_project_directory(project_path)

# If project already exists, raise error
if project_name in bw2data.projects:
    raise ValueError("Project '{}' already exists and does not need any setup.".format(project_name))
    bw2data.projects.delete_project(name = project_name, delete_dir = True)
    # bw2data.projects.delete_project(name = "test111", delete_dir = True)
    # bw2data.projects.rename_project("Brightway paper")

# Set project
bw2data.projects.set_current(project_name)


#%% File- and folderpaths

# Setup output path
output_path: pathlib.Path = pathlib.Path(bw2data.projects.output_dir)

# Migration
filename_biosphere_migration_data: str = "biosphere_migration.json"
filepath_biosphere_migration_data: pathlib.Path = output_path / filename_biosphere_migration_data
filepath_AGB_background_ei_migration_data: pathlib.Path = output_path / "AGB_background_ei_migration_data.json"
filepath_WFLDB_background_ei_migration_data: pathlib.Path = output_path / "WFLDB_background_ei_migration_data.json"
filepath_SALCA_background_ei_migration_data: pathlib.Path = output_path / "SALCA_background_ei_migration_data.json"
filepath_AGF_background_ei_migration_data: pathlib.Path = output_path / "AGF_background_ei_migration_data.json"

# Logs
filename_biosphere_flows_not_specified_in_XML_methods: str = "biosphere_flows_not_specified_in_LCIA_methods_from_XML.xlsx"

# Other
filename_SBERT_biosphere_names_validated: str = "manually_checked_SBERT_biosphere_names.xlsx"
filename_SBERT_activity_names_validated: str = "manually_checked_SBERT_activity_names.xlsx"

#%% Defaults for key variables
biosphere_db_name_simapro: str = "biosphere3 - from SimaPro"
biosphere_db_name_xml: str = "biosphere3 - from XML"
unlinked_biosphere_db_name: str = biosphere_db_name_simapro + " - unlinked"

ecoinvent_db_name_simapro: str = "ecoinvent v3.11 - SimaPro - regionalized"
ecoinvent_db_name_simapro_unreg: str = "ecoinvent v3.11 - SimaPro - unregionalized"
ecoinvent_db_name_xml: str = "ecoinvent v3.11 - XML - unregionalized"
ecoinvent_db_name_xml_migrated: str = "ecoinvent v3.12 - XML - unregionalized (migrated to SimaPro biosphere)"

agribalyse_db_name_simapro: str = "Agribalyse v3.2 - SimaPro - unregionalized (background ecoinvent v3.9.1)"
agribalyse_db_name_updated_simapro: str = "Agribalyse v3.2 - SimaPro  - unregionalized (XML background ecoinvent v3.12)"

agrifootprint_db_name_simapro: str = "AgriFootprint v6.3 - SimaPro - unregionalized (background ecoinvent v3.8)"
agrifootprint_db_name_updated_simapro: str = "AgriFootprint v6.3 - SimaPro - unregionalized (XML background ecoinvent v3.12)"

wfldb_db_name_simapro: str = "World Food LCA Database v3.5 - SimaPro - unregionalized (background ecoinvent v3.5)"
wfldb_db_name_updated_simapro: str = "World Food LCA Database v3.5 - SimaPro - unregionalized (XML background ecoinvent v3.12)"

salca_db_name_simapro: str = "SALCA Database v4 - SimaPro - unregionalized (background ecoinvent v3.11)"
salca_db_name_updated_simapro: str = "SALCA Database v4 - SimaPro - unregionalized (XML background ecoinvent v3.12)"

#%% Import SimaPro LCIA methods and create SimaPro biosphere database
methods: list[dict] = import_SimaPro_LCIA_methods(path_to_SimaPro_CSV_LCIA_files = LCIA_SimaPro_CSV_folderpath,
                                                  encoding = "latin-1",
                                                  delimiter = "\t",
                                                  verbose = True)

# Delete biosphere database if already existing
if biosphere_db_name_simapro in bw2data.databases:
    print("\n------- Delete database: " + biosphere_db_name_simapro)
    del bw2data.databases[biosphere_db_name_simapro]

register_biosphere(Brightway_project_name = project_name,
                   BRIGHTWAY2_DIR = project_path,
                   biosphere_db_name = biosphere_db_name_simapro,
                   imported_methods = methods,
                   verbose = True)

register_SimaPro_LCIA_methods(imported_methods = methods,
                              biosphere_db_name = biosphere_db_name_simapro,
                              Brightway_project_name = project_name,
                              BRIGHTWAY2_DIR = project_path,
                              logs_output_path = output_path,
                              verbose = True)

# add_damage_normalization_weighting(original_method = ("SALCA v2.01", "CEENE - Fossil fuels"),
#                                    normalization_factor = 2,
#                                    weighting_factor = None,
#                                    damage_factor = None,
#                                    new_method = ("SALCA v2.01", "CEENE - Fossil fuels", "normalized"),
#                                    new_method_unit = "MJeq",
#                                    new_method_description = "added a normalization factor of 2 to the original method",
#                                    verbose = True)

write_biosphere_flows_and_method_names_to_XLSX(biosphere_db_name = biosphere_db_name_simapro,
                                               output_path = output_path,
                                               verbose = True)

# Free up memory
del methods


#%% Import the original ecoinvent database extract from SimaPro
original_ecoinvent_db_simapro: bw2io.importers.base_lci.LCIImporter = import_SimaPro_LCI_inventories(SimaPro_CSV_LCI_filepaths = [LCI_ecoinvent_simapro_folderpath / "ECO.CSV"],
                                                                                                     db_name = ecoinvent_db_name_simapro,
                                                                                                     encoding = "latin-1",
                                                                                                     delimiter = "\t",
                                                                                                     verbose = True)

original_ecoinvent_db_simapro.apply_strategy(partial(migrate_from_excel_file,
                                                     excel_migration_filepath = LCI_ecoinvent_simapro_folderpath / "custom_migration_ECO.xlsx",
                                                     migrate_activities = False,
                                                     migrate_exchanges = True),
                                             verbose = True)

# Remove linking
original_ecoinvent_db_simapro.apply_strategy(partial(link.remove_linking,
                                                     production_exchanges = True,
                                                     substitution_exchanges = True,
                                                     technosphere_exchanges = True,
                                                     biosphere_exchanges = True))

# Link biosphere flows
original_ecoinvent_db_simapro.apply_strategy(partial(link.link_biosphere_flows_externally,
                                                     biosphere_db_name = biosphere_db_name_simapro,
                                                     biosphere_db_name_unlinked = unlinked_biosphere_db_name,
                                                     other_biosphere_databases = None,
                                                     linking_order = None,
                                                     relink = False,
                                                     strip = True,
                                                     case_insensitive = True,
                                                     remove_special_characters = False,
                                                     verbose = True), verbose = True)

# Make a new biosphere database for the flows which are currently not linked
# Add unlinked biosphere flows with a custom function
unlinked_biosphere_flows: dict = utils.add_unlinked_flows_to_biosphere_database(db = original_ecoinvent_db_simapro,
                                                                                biosphere_db_name_unlinked = unlinked_biosphere_db_name,
                                                                                biosphere_db_name = biosphere_db_name_simapro,
                                                                                add_to_existing_database = True,
                                                                                verbose = True)

print("\n------- Statistics")
original_ecoinvent_db_simapro.statistics()

# Free up memory
del unlinked_biosphere_flows  

#%% Patterns to identify inventories from different databases

# Specific patterns that are used to identify SALCA inventories
SALCA_patterns: list[str] = ["SALCA", # abbreviation to identify SALCA inventories
                            "SLACA", # WOW... I mean come on...
                            "at plant/CH mix", # some CH mixes that were created without the SALCA abbreviation
                            "maize silage, conservation, sect.", # This inventory does not contain the SALCA abbreviation in the SimaPro name but we still have to include it.
                            "maize silage, horiz. silo, IP, conservation, sect", # This inventory does not contain the SALCA abbreviation in the SimaPro name but we still have to include it.
                            "maize silage, tow. silo, IP, conservation, sect", # This inventory does not contain the SALCA abbreviation in the SimaPro name but we still have to include it.
                            ]

# Specific patterns that are used to identify WFLDB inventories
WFLDB_patterns: list[str] = ["WFLDB", # because why not finding WFLDB inventories in SALCA/ecoinvent?
                             "Diesel combustion, in tractor/kg/", # Specific SALCA inventory that we need since it is internally referred to
                             "Shed, large, wood, non-insulated, fire-unprotected, at farm/m2/" # Specific SALCA inventory that we need since it is internally referred to
                             ]

#%% Import regionalized ecoinvent LCI database from SimaPro
ecoinvent_db_simapro: bw2io.importers.base_lci.LCIImporter = bw2io.importers.base_lci.LCIImporter(ecoinvent_db_name_simapro)
ecoinvent_db_simapro.data: list[dict] = select_inventory_using_regex(db_var = copy.deepcopy(original_ecoinvent_db_simapro.data),
                                                                     exclude = True,
                                                                     include = False,
                                                                     patterns = SALCA_patterns + WFLDB_patterns,
                                                                     case_sensitive = True)
ecoinvent_db_simapro.apply_strategy(partial(change_database_name,
                                            new_db_name = ecoinvent_db_name_simapro,
                                            ))         

ecoinvent_db_simapro.apply_strategy(partial(link.link_activities_internally,
                                            production_exchanges = True,
                                            substitution_exchanges = True,
                                            technosphere_exchanges = True,
                                            relink = False,
                                            strip = True,
                                            case_insensitive = True,
                                            remove_special_characters = False,
                                            verbose = True), verbose = True)    

print("\n------- Statistics")
ecoinvent_db_simapro.statistics()

# Delete ecoinvent database if already existing
if ecoinvent_db_name_simapro in bw2data.databases:
    print("\n------- Delete database: " + ecoinvent_db_name_simapro)
    del bw2data.databases[ecoinvent_db_name_simapro]

# Write database
print("\n------- Write database: " + ecoinvent_db_name_simapro)
ecoinvent_db_simapro.write_database()


#%% Import unregionalized ecoinvent LCI database from SimaPro

ecoinvent_db_simapro_unreg: bw2io.importers.base_lci.LCIImporter = copy.deepcopy(ecoinvent_db_simapro)
ecoinvent_db_simapro_unreg.apply_strategy(partial(change_database_name,
                                                  new_db_name = ecoinvent_db_name_simapro_unreg,
                                                  ))
ecoinvent_db_simapro_unreg.apply_strategy(unregionalize_biosphere)
ecoinvent_db_simapro_unreg.apply_strategy(partial(link.link_biosphere_flows_externally,
                                                  biosphere_db_name = biosphere_db_name_simapro,
                                                  biosphere_db_name_unlinked = unlinked_biosphere_db_name,
                                                  other_biosphere_databases = None,
                                                  linking_order = None,
                                                  relink = True,
                                                  strip = True,
                                                  case_insensitive = True,
                                                  remove_special_characters = False,
                                                  verbose = True), verbose = True)

print("\n------- Statistics")
ecoinvent_db_simapro_unreg.statistics()

# Delete ecoinvent database if already existing
if ecoinvent_db_name_simapro_unreg in bw2data.databases:
    print("\n------- Delete database: " + ecoinvent_db_name_simapro_unreg)
    del bw2data.databases[ecoinvent_db_name_simapro_unreg]

# Write database
print("\n------- Write database: " + ecoinvent_db_name_simapro_unreg)
ecoinvent_db_simapro_unreg.write_database()


#%% Import WFLDB LCI database from SimaPro
wfldb_db_simapro: bw2io.importers.base_lci.LCIImporter = bw2io.importers.base_lci.LCIImporter(wfldb_db_name_simapro)
wfldb_db_simapro.data: list[dict] = select_inventory_using_regex(db_var = copy.deepcopy(original_ecoinvent_db_simapro.data),
                                                                 exclude = False,
                                                                 include = True,
                                                                 patterns = WFLDB_patterns,
                                                                 case_sensitive = True)

wfldb_db_simapro.data += import_SimaPro_LCI_inventories(SimaPro_CSV_LCI_filepaths = [LCI_wfldb_simapro_folderpath / "WFLDB.CSV"],
                                                        db_name = wfldb_db_name_simapro,
                                                        encoding = "latin-1",
                                                        delimiter = "\t",
                                                        verbose = True)

wfldb_db_simapro.apply_strategy(partial(change_database_name,
                                        new_db_name = wfldb_db_name_simapro,
                                        ))

wfldb_db_simapro.apply_strategy(unregionalize_biosphere)

wfldb_db_simapro.apply_strategy(partial(migrate_from_excel_file,
                                        excel_migration_filepath = LCI_wfldb_simapro_folderpath / "custom_migration_WFLDB.xlsx",
                                        migrate_activities = False,
                                        migrate_exchanges = True),
                                verbose = True)

wfldb_db_simapro.apply_strategy(partial(link.link_biosphere_flows_externally,
                                        biosphere_db_name = biosphere_db_name_simapro,
                                        biosphere_db_name_unlinked = unlinked_biosphere_db_name,
                                        other_biosphere_databases = None,
                                        linking_order = None,
                                        relink = False,
                                        strip = True,
                                        case_insensitive = True,
                                        remove_special_characters = False,
                                        verbose = True), verbose = True)

wfldb_db_simapro.apply_strategy(partial(link.link_activities_internally,
                                        production_exchanges = True,
                                        substitution_exchanges = True,
                                        technosphere_exchanges = True,
                                        relink = False,
                                        strip = True,
                                        case_insensitive = True,
                                        remove_special_characters = False,
                                        verbose = True), verbose = True)

wfldb_db_simapro.apply_strategy(partial(link.link_activities_externally,
                                        link_to_databases = (ecoinvent_db_name_simapro_unreg,),
                                        link_production_exchanges = False,
                                        link_substitution_exchanges = False,
                                        link_technosphere_exchanges = True,
                                        relink = False,
                                        strip = True,
                                        case_insensitive = True,
                                        remove_special_characters = False,
                                        verbose = True), verbose = True)

print("\n------- Statistics")
wfldb_db_simapro.statistics()

# wfldb_db_simapro.write_excel(only_unlinked = True)
# Make a new biosphere database for the flows which are currently not linked
# Add unlinked biosphere flows with a custom function
unlinked_biosphere_flows: dict = utils.add_unlinked_flows_to_biosphere_database(db = wfldb_db_simapro,
                                                                                biosphere_db_name_unlinked = unlinked_biosphere_db_name,
                                                                                biosphere_db_name = biosphere_db_name_simapro,
                                                                                add_to_existing_database = True,
                                                                                verbose = True)
print("\n------- Statistics")
wfldb_db_simapro.statistics()

# Delete wfldb database if already existing
if wfldb_db_name_simapro in bw2data.databases:
    print("\n------- Delete database: " + wfldb_db_name_simapro)
    del bw2data.databases[wfldb_db_name_simapro]

# Write database
print("\n------- Write database: " + wfldb_db_name_simapro)
wfldb_db_simapro.write_database()

# Create a database object that will be used afterwards to update the background
wfldb_db_updated_simapro: bw2io.importers.base_lci.LCIImporter = bw2io.importers.base_lci.LCIImporter(wfldb_db_name_updated_simapro)
wfldb_db_updated_simapro.data: list[dict] = copy.deepcopy(wfldb_db_simapro.data)

# Rename the database with the new name
wfldb_db_updated_simapro.apply_strategy(partial(change_database_name,
                                                new_db_name = wfldb_db_name_updated_simapro,
                                                ))

# Free up memory
del wfldb_db_simapro

#%% Import SALCA LCI database from SimaPro
salca_db_simapro: bw2io.importers.base_lci.LCIImporter = bw2io.importers.base_lci.LCIImporter(salca_db_name_simapro)
salca_db_simapro.data: list[dict] = select_inventory_using_regex(db_var = copy.deepcopy(original_ecoinvent_db_simapro.data),
                                                                 exclude = False,
                                                                 include = True,
                                                                 patterns = SALCA_patterns,
                                                                 case_sensitive = True)
salca_db_simapro.apply_strategy(partial(change_database_name,
                                        new_db_name = salca_db_name_simapro,
                                        ))

salca_db_simapro.apply_strategy(unregionalize_biosphere)

salca_db_simapro.apply_strategy(partial(migrate_from_excel_file,
                                        excel_migration_filepath = LCI_salca_simapro_folderpath / "custom_migration_SALCA.xlsx",
                                        migrate_activities = False,
                                        migrate_exchanges = True),
                                verbose = True)

salca_db_simapro.apply_strategy(partial(link.link_activities_internally,
                                        production_exchanges = True,
                                        substitution_exchanges = True,
                                        technosphere_exchanges = True,
                                        relink = False,
                                        strip = True,
                                        case_insensitive = True,
                                        remove_special_characters = False,
                                        verbose = True), verbose = True) 

salca_db_simapro.apply_strategy(partial(link.link_activities_externally,
                                        link_to_databases = (ecoinvent_db_name_simapro_unreg, wfldb_db_name_simapro),
                                        link_production_exchanges = False,
                                        link_substitution_exchanges = False,
                                        link_technosphere_exchanges = True,
                                        relink = False,
                                        strip = True,
                                        case_insensitive = True,
                                        remove_special_characters = False,
                                        verbose = True), verbose = True)

print("\n------- Statistics")
salca_db_simapro.statistics()
# salca_db_simapro.write_excel(only_unlinked = True)

# Delete ecoinvent database if already existing
if salca_db_name_simapro in bw2data.databases:
    print("\n------- Delete database: " + salca_db_name_simapro)
    del bw2data.databases[salca_db_name_simapro]

# Write database
print("\n------- Write database: " + salca_db_name_simapro)
salca_db_simapro.write_database()

# Create a database object that will be used afterwards to update the background
salca_db_updated_simapro: bw2io.importers.base_lci.LCIImporter = bw2io.importers.base_lci.LCIImporter(salca_db_name_updated_simapro)
salca_db_updated_simapro.data: list[dict] = copy.deepcopy(salca_db_simapro.data)

# Rename the database with the new name
salca_db_updated_simapro.apply_strategy(partial(change_database_name,
                                                new_db_name = salca_db_name_updated_simapro,
                                                ))

# Free up memory
del salca_db_simapro



#%% Import Agribalyse LCI database from SimaPro
agribalyse_db_simapro: bw2io.importers.base_lci.LCIImporter = import_SimaPro_LCI_inventories(SimaPro_CSV_LCI_filepaths = [LCI_agribalyse_simapro_folderpath / "AGB.CSV"],
                                                                                             db_name = agribalyse_db_name_simapro,
                                                                                             encoding = "latin-1",
                                                                                             delimiter = "\t",
                                                                                             verbose = True)
agribalyse_db_simapro.apply_strategy(unregionalize_biosphere)

agribalyse_db_simapro.apply_strategy(partial(link.link_biosphere_flows_externally,
                                             biosphere_db_name = biosphere_db_name_simapro,
                                             biosphere_db_name_unlinked = unlinked_biosphere_db_name,
                                             other_biosphere_databases = None,
                                             linking_order = None,
                                             relink = False,
                                             strip = True,
                                             case_insensitive = True,
                                             remove_special_characters = False,
                                             verbose = True), verbose = True)

agribalyse_db_simapro.apply_strategy(partial(migrate_from_excel_file,
                                             excel_migration_filepath = LCI_agribalyse_simapro_folderpath / "custom_migration_AGB.xlsx",
                                             migrate_activities = False,
                                             migrate_exchanges = True),
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

print("\n------- Statistics")
agribalyse_db_simapro.statistics()

# Make a new biosphere database for the flows which are currently not linked
# Add unlinked biosphere flows with a custom function
unlinked_biosphere_flows: dict = utils.add_unlinked_flows_to_biosphere_database(db = agribalyse_db_simapro,
                                                                                biosphere_db_name_unlinked = unlinked_biosphere_db_name,
                                                                                biosphere_db_name = biosphere_db_name_simapro,
                                                                                add_to_existing_database = True,
                                                                                verbose = True)
print("\n------- Statistics")
agribalyse_db_simapro.statistics()

# Delete agribalyse database if already existing
if agribalyse_db_name_simapro in bw2data.databases:
    print("\n------- Delete database: " + agribalyse_db_name_simapro)
    del bw2data.databases[agribalyse_db_name_simapro]

# Write database
print("\n------- Write database: " + agribalyse_db_name_simapro)
agribalyse_db_simapro.write_database()

# Create a database object that will be used afterwards to update the background
agribalyse_db_updated_simapro: bw2io.importers.base_lci.LCIImporter = bw2io.importers.base_lci.LCIImporter(agribalyse_db_name_updated_simapro)
agribalyse_db_updated_simapro.data: list[dict] = copy.deepcopy(agribalyse_db_simapro.data)

# Rename the database with the new name
agribalyse_db_updated_simapro.apply_strategy(partial(change_database_name,
                                                     new_db_name = agribalyse_db_name_updated_simapro,
                                                     ))

# Free up memory
del agribalyse_db_simapro, unlinked_biosphere_flows


#%% Import AgriFootprint LCI database from SimaPro
agrifootprint_db_simapro: bw2io.importers.base_lci.LCIImporter = import_SimaPro_LCI_inventories(SimaPro_CSV_LCI_filepaths = [LCI_agrifootprint_simapro_folderpath / "AGF.CSV"],
                                                                                                db_name = agrifootprint_db_name_simapro,
                                                                                                encoding = "latin-1",
                                                                                                delimiter = "\t",
                                                                                                verbose = True)
agrifootprint_db_simapro.apply_strategy(unregionalize_biosphere)

agrifootprint_db_simapro.apply_strategy(partial(link.link_biosphere_flows_externally,
                                                biosphere_db_name = biosphere_db_name_simapro,
                                                biosphere_db_name_unlinked = unlinked_biosphere_db_name,
                                                other_biosphere_databases = None,
                                                linking_order = None,
                                                relink = False,
                                                strip = True,
                                                case_insensitive = True,
                                                remove_special_characters = False,
                                                verbose = True), verbose = True)

agrifootprint_db_simapro.apply_strategy(partial(migrate_from_excel_file,
                                                excel_migration_filepath = LCI_agrifootprint_simapro_folderpath / "custom_migration_AGF_technosphere.xlsx",
                                                migrate_activities = False,
                                                migrate_exchanges = True),
                                        verbose = True)

agrifootprint_db_simapro.apply_strategy(partial(migrate_from_excel_file,
                                                excel_migration_filepath = LCI_agrifootprint_simapro_folderpath / "custom_migration_AGF_substitution.xlsx",
                                                migrate_activities = False,
                                                migrate_exchanges = True),
                                        verbose = True)

agrifootprint_db_simapro.apply_strategy(partial(link.link_activities_internally,
                                                production_exchanges = True,
                                                substitution_exchanges = True,
                                                technosphere_exchanges = True,
                                                relink = False,
                                                strip = True,
                                                case_insensitive = True,
                                                remove_special_characters = False,
                                                verbose = True), verbose = True)

print("\n------- Statistics")
agrifootprint_db_simapro.statistics()

# Make a new biosphere database for the flows which are currently not linked
# Add unlinked biosphere flows with a custom function
unlinked_biosphere_flows: dict = utils.add_unlinked_flows_to_biosphere_database(db = agrifootprint_db_simapro,
                                                                                biosphere_db_name_unlinked = unlinked_biosphere_db_name,
                                                                                biosphere_db_name = biosphere_db_name_simapro,
                                                                                add_to_existing_database = True,
                                                                                verbose = True)
print("\n------- Statistics")
agrifootprint_db_simapro.statistics()

# Delete agrifootprint database if already existing
if agrifootprint_db_name_simapro in bw2data.databases:
    print("\n------- Delete database: " + agrifootprint_db_name_simapro)
    del bw2data.databases[agrifootprint_db_name_simapro]

# Write database
print("\n------- Write database: " + agrifootprint_db_name_simapro)
agrifootprint_db_simapro.write_database()

# Create a database object that will be used afterwards to update the background
agrifootprint_db_updated_simapro: bw2io.importers.base_lci.LCIImporter = bw2io.importers.base_lci.LCIImporter(agrifootprint_db_name_updated_simapro)
agrifootprint_db_updated_simapro.data: list[dict] = copy.deepcopy(agrifootprint_db_simapro.data)

# Rename the database with the new name
agrifootprint_db_updated_simapro.apply_strategy(partial(change_database_name,
                                                        new_db_name = agrifootprint_db_name_updated_simapro,
                                                        ))

# Free up memory
del agrifootprint_db_simapro, unlinked_biosphere_flows


#%% Import ecoinvent data from ecoinvent XML setup
ecoinvent_db_xml: bw2io.importers.ecospold2.SingleOutputEcospold2Importer = import_XML_LCI_inventories(XML_LCI_filepath = LCI_ecoinvent_xml_datasets_folderpath_temp,
                                                                                                       db_name = ecoinvent_db_name_xml,
                                                                                                       biosphere_db_name = biosphere_db_name_xml,
                                                                                                       db_model_type_name = "cutoff",
                                                                                                       db_process_type_name = "unit",
                                                                                                       verbose = True)

print("\n-----------Linking statistics of current database import")
ecoinvent_db_xml.statistics()
print()

# Create the biosphere from XML file containing all elementary exchanges
biosphere_flows_from_xml_elementary_exchanges: list[dict] = create_XML_biosphere_from_elmentary_exchanges_file(filepath_ElementaryExchanges = LCIA_XML_elementary_exchanges_filepath_temp,
                                                                                                               biosphere_db_name = biosphere_db_name_xml)

# Delete biosphere database from XML if already existing
if biosphere_db_name_xml in bw2data.databases:
    print("\n------- Delete database: " + biosphere_db_name_xml)
    del bw2data.databases[biosphere_db_name_xml]

# Register the XML biosphere database
print("\n-----------Write database: " + biosphere_db_name_xml)
bw2data.Database(biosphere_db_name_xml).write({(m["database"], m["code"]): m for m in biosphere_flows_from_xml_elementary_exchanges})

# Link biosphere flows to imported ecoinvent biosphere (from XML files)
ecoinvent_db_xml.apply_strategy(partial(link.link_biosphere_flows_externally,
                                        biosphere_db_name = biosphere_db_name_xml,
                                        biosphere_db_name_unlinked = unlinked_biosphere_db_name,
                                        other_biosphere_databases = None,
                                        linking_order = None,
                                        relink = False,
                                        strip = False,
                                        case_insensitive = False,
                                        remove_special_characters = False,
                                        verbose = True), verbose = True)

print("\n-----------Linking statistics of current database import")
ecoinvent_db_xml.statistics()
print()

# Delete database first, if existing
if ecoinvent_db_name_xml in bw2data.databases:
    print("\n-----------Delete database: " + ecoinvent_db_name_xml)
    del bw2data.databases[ecoinvent_db_name_xml]

print("\n-----------Write database: " + ecoinvent_db_name_xml)
ecoinvent_db_xml.write_database(overwrite = False)
print()

# Import ecoinvent LCIA methods from Excel file
xml_lcia_methods: list[dict] = import_XML_LCIA_methods(XML_LCIA_filepath = LCIA_XML_filepath_temp,
                                                       biosphere_db_name = biosphere_db_name_xml,
                                                       ecoinvent_version = None)

print("\n-----------Write methods from ecoinvent Excel")
register_XML_LCIA_methods(methods = xml_lcia_methods)

# Free up memory
del ecoinvent_db_xml, xml_lcia_methods, biosphere_flows_from_xml_elementary_exchanges



#%% Create JSON files containing biosphere flow data
ecoinvent_db_xml_migrated: bw2io.importers.ecospold2.SingleOutputEcospold2Importer = import_XML_LCI_inventories(XML_LCI_filepath = LCI_ecoinvent_xml_datasets_folderpath,
                                                                                                                db_name = ecoinvent_db_name_xml_migrated,
                                                                                                                biosphere_db_name = biosphere_db_name_simapro,
                                                                                                                db_model_type_name = "cutoff",
                                                                                                                db_process_type_name = "unit",
                                                                                                                verbose = True)

# Create biosphere from XML LCI data
biosphere_flows_from_XML_LCI_data: list[dict] = create_XML_biosphere_from_LCI(db = ecoinvent_db_xml_migrated,
                                                                              biosphere_db_name = biosphere_db_name_simapro)

# Read excel file containing the LCIA methods from ecoinvent
df_LCIA_methods_ecoinvent: pd.DataFrame = pd.read_excel(LCIA_XML_filepath, sheet_name = LCIA_XML_sheetname)

# Create a list with biosphere flows that are not used in any of the ecoinvent methods
not_used_flows: dict = elementary_flows_that_are_not_used_in_XML_methods(elementary_flows = biosphere_flows_from_XML_LCI_data, method_df = df_LCIA_methods_ecoinvent)

# Write dataframe of flows that are not used
pd.DataFrame(list(not_used_flows.values())).to_excel(output_path / filename_biosphere_flows_not_specified_in_XML_methods, index = None)

# Exclude the flows that are not used, those do not need to be migrated
biosphere_flows_from_XML_used: list[dict] = [m for m in biosphere_flows_from_XML_LCI_data if (m["database"], m["code"]) not in not_used_flows]

# Create the JSON object to be written
biosphere_flows_from_XML_used_json: dict = json.dumps({idx: m for idx, m in enumerate(biosphere_flows_from_XML_used)}, indent = 4)

# Write the unlinked biosphere data dictionary to a JSON file
with open(output_path / ("biosphere_flows_from_XML.json"), "w") as outfile:
    outfile.write(biosphere_flows_from_XML_used_json)

# Create the biosphere from registered biosphere database (here from SimaPro)
biosphere_flows_from_SimaPro: list[dict] = [dict(m) for m in bw2data.Database(biosphere_db_name_simapro)]

# Create the JSON object to be written
biosphere_flows_from_SimaPro_json: dict = json.dumps({idx: dict(m) for idx, m in enumerate(biosphere_flows_from_SimaPro)}, indent = 4)

# Write the unlinked biosphere data dictionary to a JSON file
with open(output_path / ("biosphere_flows_from_SimaPro.json"), "w") as outfile:
    outfile.write(biosphere_flows_from_SimaPro_json)
    
# Free up memory
del biosphere_flows_from_SimaPro_json, biosphere_flows_from_XML_used_json

#%% Harmonize biospheres and create migration JSON

# Load dataframe with manually checked biosphere flows
manually_checked_SBERT_biosphere_names: pd.DataFrame = pd.read_excel(LCI_ecoinvent_xml_folderpath / filename_SBERT_biosphere_names_validated)

# Harmonize the two biospheres
biosphere_harmonization: dict = create_harmonized_biosphere_migration(biosphere_flows_1 = biosphere_flows_from_XML_used,
                                                                      biosphere_flows_2 = biosphere_flows_from_SimaPro,
                                                                      manually_checked_SBERTs = manually_checked_SBERT_biosphere_names)

# Write excels
pd.DataFrame(biosphere_harmonization["successfully_migrated_biosphere_flows"]).to_excel(output_path / "biosphere_flows_successfully_migrated.xlsx")
pd.DataFrame(biosphere_harmonization["unsuccessfully_migrated_biosphere_flows"]).to_excel(output_path / "biosphere_flows_unuccessfully_migrated.xlsx")
pd.DataFrame(biosphere_harmonization["SBERT_to_map"]).to_excel(output_path / "biosphere_flows_SBERT_to_map.xlsx")

# Retrieve the biosphere harmonization dictionary
biosphere_migration_data: dict = biosphere_harmonization["biosphere_migration"]

# Create the JSON object to be written
biosphere_migration_data_in_json_format = json.dumps(biosphere_migration_data, indent = 3)

# Write the biosphere dictionary to a JSON file
with open(filepath_biosphere_migration_data, "w") as outfile:
   outfile.write(biosphere_migration_data_in_json_format)

# Free up memory
del biosphere_harmonization, biosphere_migration_data, biosphere_migration_data_in_json_format

#%% Remove unused flows
def remove_unused_biosphere_flows(db):
    for ds in db:
        ds["exchanges"]: list = [m for m in ds["exchanges"] if not (m["type"] == "biosphere" and (biosphere_db_name_simapro, m["code"]) in not_used_flows)]
    return db

# Apply strategy and remove unused flows
ecoinvent_db_xml_migrated.apply_strategy(remove_unused_biosphere_flows)

# The amount of XML biosphere flows should be the same as in the statistics
ecoinvent_db_xml_migrated.statistics()

#%% Replace the XML code with the one from SimaPro
# def replace_XML_code_field_with_SimaPro_code(XML_db):
    
#     # Create a mapping file from activity and reference product codes that are found in the ecoinvent SimaPro database
#     mapping: dict = {m["activity_code"] + "_" + m["reference_product_code"]: m["code"] for m in ecoinvent_db_simapro if m["activity_code"] is not None and m["reference_product_code"] is not None}
    
#     # Initialize counter
#     counter: int = 0
    
#     # Loop through each inventory from the XML database
#     for ds in XML_db:
        
#         # Try to find the corresponding SimaPro code using the mapping created beforehand
#         new_code: (str | None) = mapping.get(ds["code"])
        
#         # Go to next if no new code was found
#         if new_code is None:
#             continue
        
#         # Update the code field
#         ds["code"]: str = new_code
        
#         # Increase counter
#         counter += 1
        
#         # Loop through the exchanges to update the production exchange as well
#         for exc in ds["exchanges"]:
            
#             # If the current exchange is not of type production, continue
#             if exc["type"] != "production":
#                 continue
            
#             # Update code and input fields
#             exc["code"]: str = new_code
#             exc["input"]: tuple = (ds["database"], new_code)
    
#     # Print how many codes were changed
#     print("{} from {} code(s) were updated with the respective SimaPro code".format(counter, len(XML_db)))
    
#     return XML_db

# # Apply strategy and update the code of XML inventories with the respecting code from SimaPro inventories
# ecoinvent_db_xml_migrated.apply_strategy(replace_XML_code_field_with_SimaPro_code)

#%% Migrate biosphere flows and register ecoinvent XML database

# Apply biosphere migration
ecoinvent_db_xml_migrated.apply_strategy(partial(migrate_from_json_file,
                                                 json_migration_filepath = filepath_biosphere_migration_data,
                                                 migrate_activities = False,
                                                 migrate_exchanges = True),
                                         verbose = True)

ecoinvent_db_xml_migrated.apply_strategy(partial(link.remove_linking,
                                                 production_exchanges = True,
                                                 substitution_exchanges = True,
                                                 technosphere_exchanges = True,
                                                 biosphere_exchanges = True), verbose = True)

ecoinvent_db_xml_migrated.apply_strategy(partial(link.link_activities_internally,
                                                 production_exchanges = True,
                                                 substitution_exchanges = True,
                                                 technosphere_exchanges = True,
                                                 relink = False,
                                                 strip = True,
                                                 case_insensitive = True,
                                                 remove_special_characters = False,
                                                 verbose = True), verbose = True)

# Apply external linking of biosphere flows
ecoinvent_db_xml_migrated.apply_strategy(partial(link.link_biosphere_flows_externally,
                                                 biosphere_db_name = biosphere_db_name_simapro,
                                                 biosphere_db_name_unlinked = unlinked_biosphere_db_name,
                                                 other_biosphere_databases = None,
                                                 linking_order = None,
                                                 relink = False,
                                                 strip = True,
                                                 case_insensitive = True,
                                                 remove_special_characters = False,
                                                 verbose = True), verbose = True)

# Write unlinked biosphere flows to XLSX
print("\n-----------Write unlinked flows to excel file")
ecoinvent_db_xml_migrated.write_excel(only_unlinked = True)
    
# Show statistic of current linking of database import
print("\n-----------Linking statistics of current database import")
ecoinvent_db_xml_migrated.statistics()
print()
 
# Make a new biosphere database for the flows which are currently not linked
# Add unlinked biosphere flows with a custom function
unlinked_biosphere_flows: dict = utils.add_unlinked_flows_to_biosphere_database(db = ecoinvent_db_xml_migrated,
                                                                                biosphere_db_name_unlinked = unlinked_biosphere_db_name,
                                                                                biosphere_db_name = biosphere_db_name_simapro,
                                                                                add_to_existing_database = True,
                                                                                verbose = True)
# Show statistic of current linking of database import
print("\n-----------Linking statistics of current database import")
ecoinvent_db_xml_migrated.statistics()
print()

# Delete database first, if existing
if ecoinvent_db_name_xml_migrated in bw2data.databases:
    print("\n-----------Delete database: " + ecoinvent_db_name_xml_migrated)
    del bw2data.databases[ecoinvent_db_name_xml_migrated]

# Write database
print("\n-----------Write database: " + ecoinvent_db_name_xml_migrated)
ecoinvent_db_xml_migrated.write_database(overwrite = False)
print()

# Free up memory
del ecoinvent_db_xml_migrated


#%% Create correspondence mapping

correspondence_files_and_versions: list[tuple] = [("Correspondence-File-v3.1-v3.2.xlsx", (3, 1), (3, 2)),
                                                  ("Correspondence-File-v3.2-v3.3.xlsx", (3, 2), (3, 3)),
                                                  ("Correspondence-File-v3.3-v3.4.xlsx", (3, 3), (3, 4)),
                                                  ("Correspondence-File-v3.4-v3.5.xlsx", (3, 4), (3, 5)),
                                                  ("Correspondence-File-v3.5-v3.6.xlsx", (3, 5), (3, 6)),
                                                  ("Correspondence-File-v3.6-v3.7.1.xlsx", (3, 6), (3, 7, 1)),
                                                  ("Correspondence-File-v3.7.1-v3.8.xlsx", (3, 7, 1), (3, 8)),
                                                  ("Correspondence-File-v3.8-v3.9.1.xlsx", (3, 8), (3, 9, 1)),
                                                  ("Correspondence-File-v3.8-v3.9.xlsx", (3, 8), (3, 9)),
                                                  ("Correspondence-File-v3.9.1-v3.10.xlsx", (3, 9, 1), (3, 10)),
                                                  ("Correspondence-File-v3.10.1-v3.11.xlsx", (3, 10, 1), (3, 11)),
                                                  ("Correspondence-File-v3.10-v3.10.1.xlsx", (3, 10), (3, 10, 1)),
                                                  ("Correspondence-File-v3.11-v3.12.xlsx", (3, 11), (3, 12)),
                                                  ]

correspondence_obj: Correspondence = Correspondence(ecoinvent_model_type = "cutoff")

for filename, FROM_version, TO_version in correspondence_files_and_versions:
    correspondence_obj.read_correspondence_dataframe(filepath_correspondence_excel = here.parent / "correspondence" / "data" / filename,
                                                     FROM_version = FROM_version,
                                                     TO_version = TO_version
                                                 )

#%% Specify the activities that can be mapped to
activities_to_migrate_to: list[dict] = [m.as_dict() for m in bw2data.Database(ecoinvent_db_name_xml_migrated)]

# Initialize the activity harmonization class
ah: ActivityHarmonization = ActivityHarmonization(model = "Cut-off",
                                                  system = "Unit")

# Add the items to which should be migrated to
for ds in activities_to_migrate_to:
    source: ActivityDefinition = ActivityDefinition(activity_code = ds.get("activity_code"), 
                                                    reference_product_code = ds.get("reference_product_code"),
                                                    activity_name = ds.get("activity_name"),
                                                    reference_product_name = ds.get("reference_product_name"),
                                                    name = ds.get("name"),
                                                    simapro_name = ds.get("SimaPro_name"),
                                                    location = ds.get("location"),
                                                    unit = ds.get("unit")
                                                    )
 
    # ah.add_TO(source = copy.deepcopy(source), target = copy.deepcopy(ds), multiplier = 1)
    ah.add_TO(source = source, target = copy.deepcopy(ds), multiplier = 1)


#%% Create default variables to log data
unsuccessfully_migrated: list[dict] = []
successfully_migrated: list[dict] = []
SBERT_to_map: list[dict] = []

#%% Create the Agribalyse background activity migration --> all ecoinvent v3.8 found in the background from Agribalyse should be updated to ecoinvent v3.10, if possible
agribalyse_exchanges_to_migrate_to_ecoinvent: dict = {(exc["name"], exc["unit"], exc["location"]): exc for ds in list(agribalyse_db_updated_simapro) for exc in ds["exchanges"] if exc["type"] not in ["production", "biosphere"] and exc.get("is_ecoinvent", False)}

# Load dataframe with manually checked activity flows
manually_checked_SBERT_activity_names: pd.DataFrame = pd.read_excel(LCI_ecoinvent_xml_folderpath / filename_SBERT_activity_names_validated)

AGB_background_ei_migration: dict = create_harmonized_activity_migration(flows_1 = list(agribalyse_exchanges_to_migrate_to_ecoinvent.values()),
                                                                         flows_2 = list(bw2data.Database(ecoinvent_db_name_xml_migrated)),
                                                                         # manually_checked_SBERTs = manually_checked_SBERT_activity_names,
                                                                         manually_checked_SBERTs = None,
                                                                         ecoinvent_correspondence_mapping = correspondence_mapping)

# SBERT to map for activities
unsuccessfully_migrated += [{**{"FROM_" + k: v for k, v in m.items()}, **{"Database": agribalyse_db_name_updated_simapro}} for m, _ in AGB_background_ei_migration["unsuccessfully_migrated_activity_flows"]]
successfully_migrated += [{**{"FROM_" + k: v for k, v in m.items()}, **{"TO_" + k: v for k, v in n.items()}, **{"Database": agribalyse_db_name_updated_simapro}} for m, n in AGB_background_ei_migration["successfully_migrated_activity_flows"]]
SBERT_to_map += [{**m, **{"Database": agribalyse_db_name_updated_simapro}} for m in AGB_background_ei_migration["SBERT_to_map"]]

# Create the JSON object to be written
AGB_background_ei_migration_in_json_format = json.dumps(AGB_background_ei_migration["activity_migration"], indent = 3)

# Write the activity dictionary to a JSON file
with open(filepath_AGB_background_ei_migration_data, "w") as outfile:
   outfile.write(AGB_background_ei_migration_in_json_format)

# Free up memory
del agribalyse_exchanges_to_migrate_to_ecoinvent, AGB_background_ei_migration, AGB_background_ei_migration_in_json_format

#%% Update the ecoinvent background activities in the Agribalyse database from v3.8 to v3.10 and register database
agribalyse_db_updated_simapro.apply_strategy(partial(link.remove_linking,
                                                     production_exchanges = False,
                                                     substitution_exchanges = True,
                                                     technosphere_exchanges = True,
                                                     biosphere_exchanges = False), verbose = True)

# Apply activity migration
agribalyse_db_updated_simapro.apply_strategy(partial(migrate_from_json_file,
                                                     json_migration_filepath = filepath_AGB_background_ei_migration_data,
                                                     migrate_activities = False,
                                                     migrate_exchanges = True),
                                             verbose = True)

# Link to ecoinvent externally
agribalyse_db_updated_simapro.apply_strategy(partial(link.link_activities_externally,
                                                     link_to_databases = (ecoinvent_db_name_xml_migrated,),
                                                     link_production_exchanges = False,
                                                     link_substitution_exchanges = True,
                                                     link_technosphere_exchanges = True,
                                                     relink = False,
                                                     case_insensitive = True,
                                                     strip = True,
                                                     remove_special_characters = False,
                                                     verbose = True), verbose = True)

# Link remaining ones that were not updated
agribalyse_db_updated_simapro.apply_strategy(partial(link.link_activities_internally,
                                                     production_exchanges = False,
                                                     substitution_exchanges = True,
                                                     technosphere_exchanges = True,
                                                     relink = False,
                                                     strip = True,
                                                     case_insensitive = True,
                                                     remove_special_characters = False,
                                                     verbose = True), verbose = True)


# Show statistic of current linking of database import
print("\n-----------Linking statistics of current database import")
agribalyse_n_datasets, agribalyse_n_exchanges, agribalyse_n_unlinked = agribalyse_db_updated_simapro.statistics()
print()

# Write unlinked biosphere flows to XLSX
print("\n-----------Write unlinked flows to excel file")
agribalyse_db_updated_simapro.write_excel(only_unlinked = True)

# Delete database first, if existing
if agribalyse_db_name_updated_simapro in bw2data.databases:
    print("\n-----------Delete database: " + agribalyse_db_name_updated_simapro)
    del bw2data.databases[agribalyse_db_name_updated_simapro]

# # Write database
# print("\n-----------Write database: " + agribalyse_db_name_updated_simapro)
# agribalyse_db_updated_simapro.write_database(overwrite = False)
# print()

# # Free up memory
# del agribalyse_db_updated_simapro



#%% Create the WFDLB background activity migration --> all ecoinvent v3.5 found in the background from WFLDB should be updated to ecoinvent v3.10, if possible
wfldb_exchanges_to_migrate_to_ecoinvent: dict = {(exc["name"], exc["unit"], exc["location"]): exc for ds in list(wfldb_db_updated_simapro) for exc in ds["exchanges"] if exc["type"] not in ["production", "biosphere"] and exc.get("is_ecoinvent", False)}

# Load dataframe with manually checked activity flows
manually_checked_SBERT_activity_names: pd.DataFrame = pd.read_excel(LCI_ecoinvent_xml_folderpath / filename_SBERT_activity_names_validated)

WFLDB_background_ei_migration: dict = create_harmonized_activity_migration(flows_1 = list(wfldb_exchanges_to_migrate_to_ecoinvent.values()),
                                                                           flows_2 = list(bw2data.Database(ecoinvent_db_name_xml_migrated)),
                                                                           # manually_checked_SBERTs = manually_checked_SBERT_activity_names,
                                                                           manually_checked_SBERTs = None,
                                                                           ecoinvent_correspondence_mapping = correspondence_mapping)

# SBERT to map for activities
unsuccessfully_migrated += [{**{"FROM_" + k: v for k, v in m.items()}, **{"Database": wfldb_db_name_updated_simapro}} for m, _ in WFLDB_background_ei_migration["unsuccessfully_migrated_activity_flows"]]
successfully_migrated += [{**{"FROM_" + k: v for k, v in m.items()}, **{"TO_" + k: v for k, v in n.items()}, **{"Database": wfldb_db_name_updated_simapro}} for m, n in WFLDB_background_ei_migration["successfully_migrated_activity_flows"]]
SBERT_to_map += [{**m, **{"Database": wfldb_db_name_updated_simapro}} for m in WFLDB_background_ei_migration["SBERT_to_map"]]

# Create the JSON object to be written
WFLDB_background_ei_migration_in_json_format = json.dumps(WFLDB_background_ei_migration["activity_migration"], indent = 3)

# Write the activity dictionary to a JSON file
with open(filepath_WFLDB_background_ei_migration_data, "w") as outfile:
   outfile.write(WFLDB_background_ei_migration_in_json_format)

# Free up memory
del wfldb_exchanges_to_migrate_to_ecoinvent, WFLDB_background_ei_migration, WFLDB_background_ei_migration_in_json_format


#%% Update the ecoinvent background activities in the World Food LCA database from v3.5 to v3.10 and register database
wfldb_db_updated_simapro.apply_strategy(partial(link.remove_linking,
                                                production_exchanges = False,
                                                substitution_exchanges = True,
                                                technosphere_exchanges = True,
                                                biosphere_exchanges = False), verbose = True)

# Apply activity migration
wfldb_db_updated_simapro.apply_strategy(partial(migrate_from_json_file,
                                                json_migration_filepath = filepath_WFLDB_background_ei_migration_data,
                                                migrate_activities = False,
                                                migrate_exchanges = True),
                                        verbose = True)

# Link to ecoinvent externally
wfldb_db_updated_simapro.apply_strategy(partial(link.link_activities_externally,
                                                link_to_databases = (ecoinvent_db_name_xml_migrated,),
                                                link_production_exchanges = False,
                                                link_substitution_exchanges = True,
                                                link_technosphere_exchanges = True,
                                                relink = False,
                                                case_insensitive = True,
                                                strip = True,
                                                remove_special_characters = False,
                                                verbose = True), verbose = True)

# Link remaining ones that were not updated
wfldb_db_updated_simapro.apply_strategy(partial(link.link_activities_internally,
                                                production_exchanges = False,
                                                substitution_exchanges = True,
                                                technosphere_exchanges = True,
                                                relink = False,
                                                strip = True,
                                                case_insensitive = True,
                                                remove_special_characters = False,
                                                verbose = True), verbose = True)


# Show statistic of current linking of database import
print("\n-----------Linking statistics of current database import")
wfldb_n_datasets, wfldb_n_exchanges, wfldb_n_unlinked = wfldb_db_updated_simapro.statistics()
print()

# Write unlinked biosphere flows to XLSX
print("\n-----------Write unlinked flows to excel file")
wfldb_db_updated_simapro.write_excel(only_unlinked = True)

# Delete database first, if existing
if wfldb_db_name_updated_simapro in bw2data.databases:
    print("\n-----------Delete database: " + wfldb_db_name_updated_simapro)
    del bw2data.databases[wfldb_db_name_updated_simapro]

# # Write database
# print("\n-----------Write database: " + wfldb_db_name_updated_simapro)
# wfldb_db_updated_simapro.write_database(overwrite = False)
# print()

# # Free up memory
# del wfldb_db_updated_simapro



#%% Create the SALCA background activity migration --> all ecoinvent v3.10 found in the background from SALCA should be updated to ecoinvent v3.10 from XML, if possible
salca_exchanges_to_migrate_to_ecoinvent: dict = {(exc["name"], exc["unit"], exc["location"]): exc for ds in list(salca_db_updated_simapro) for exc in ds["exchanges"] if exc["type"] not in ["production", "biosphere"] and exc.get("is_ecoinvent", False)}

# Load dataframe with manually checked activity flows
manually_checked_SBERT_activity_names: pd.DataFrame = pd.read_excel(LCI_ecoinvent_xml_folderpath / filename_SBERT_activity_names_validated)

SALCA_background_ei_migration: dict = create_harmonized_activity_migration(flows_1 = list(salca_exchanges_to_migrate_to_ecoinvent.values()),
                                                                           flows_2 = list(bw2data.Database(ecoinvent_db_name_xml_migrated)),
                                                                           # manually_checked_SBERTs = manually_checked_SBERT_activity_names,
                                                                           manually_checked_SBERTs = None,
                                                                           ecoinvent_correspondence_mapping = correspondence_mapping)

# SBERT to map for activities
unsuccessfully_migrated += [{**{"FROM_" + k: v for k, v in m.items()}, **{"Database": salca_db_name_updated_simapro}} for m, _ in SALCA_background_ei_migration["unsuccessfully_migrated_activity_flows"]]
successfully_migrated += [{**{"FROM_" + k: v for k, v in m.items()}, **{"TO_" + k: v for k, v in n.items()}, **{"Database": salca_db_name_updated_simapro}} for m, n in SALCA_background_ei_migration["successfully_migrated_activity_flows"]]
SBERT_to_map += [{**m, **{"Database": salca_db_name_updated_simapro}} for m in SALCA_background_ei_migration["SBERT_to_map"]]

# Create the JSON object to be written
SALCA_background_ei_migration_in_json_format = json.dumps(SALCA_background_ei_migration["activity_migration"], indent = 3)

# Write the activity dictionary to a JSON file
with open(filepath_SALCA_background_ei_migration_data, "w") as outfile:
   outfile.write(SALCA_background_ei_migration_in_json_format)

# Free up memory
del salca_exchanges_to_migrate_to_ecoinvent, SALCA_background_ei_migration, SALCA_background_ei_migration_in_json_format

#%% Update the ecoinvent background activities in the SALCA database from v3.10 to v3.10 (XML) and register database
salca_db_updated_simapro.apply_strategy(partial(link.remove_linking,
                                                production_exchanges = False,
                                                substitution_exchanges = True,
                                                technosphere_exchanges = True,
                                                biosphere_exchanges = False), verbose = True)

# Apply activity migration
salca_db_updated_simapro.apply_strategy(partial(migrate_from_json_file,
                                                json_migration_filepath = filepath_SALCA_background_ei_migration_data,
                                                migrate_activities = False,
                                                migrate_exchanges = True),
                                        verbose = True)

# Link to ecoinvent and wfldb externally
salca_db_updated_simapro.apply_strategy(partial(link.link_activities_externally,
                                                link_to_databases = (ecoinvent_db_name_xml_migrated, wfldb_db_name_updated_simapro),
                                                link_production_exchanges = False,
                                                link_substitution_exchanges = True,
                                                link_technosphere_exchanges = True,
                                                relink = False,
                                                strip = True,
                                                case_insensitive = True,
                                                remove_special_characters = False,
                                                verbose = True), verbose = True)

# Link remaining ones that were not updated
salca_db_updated_simapro.apply_strategy(partial(link.link_activities_internally,
                                                production_exchanges = False,
                                                substitution_exchanges = True,
                                                technosphere_exchanges = True,
                                                relink = False,
                                                strip = True,
                                                case_insensitive = True,
                                                remove_special_characters = False,
                                                verbose = True), verbose = True)

# Show statistic of current linking of database import
print("\n-----------Linking statistics of current database import")
salca_n_datasets, salca_n_exchanges, salca_n_unlinked = salca_db_updated_simapro.statistics()
print()

# Write unlinked biosphere flows to XLSX, if existing
if salca_n_unlinked > 0:
    print("\n-----------Write unlinked flows to excel file")
    salca_db_updated_simapro.write_excel(only_unlinked = True)

# Delete database first, if existing
if salca_db_name_updated_simapro in bw2data.databases:
    print("\n-----------Delete database: " + salca_db_name_updated_simapro)
    del bw2data.databases[salca_db_name_updated_simapro]
    
# # Write database
# print("\n-----------Write database: " + salca_db_name_updated_simapro)
# salca_db_updated_simapro.write_database(overwrite = False)
# print()

# # Free up memory
# del salca_db_updated_simapro



#%% Create the Agrifootprint activity migration --> all ecoinvent v3.8 found in the background from AgriFootprint should be updated to ecoinvent v3.10, if possible
agrifootprint_exchanges_to_migrate_to_ecoinvent: dict = {(exc["name"], exc["unit"], exc["location"]): exc for ds in list(agrifootprint_db_updated_simapro) for exc in ds["exchanges"] if exc["type"] not in ["production", "biosphere"] and exc.get("is_ecoinvent", False)}

# Initialize the activity harmonization class
agrifootprint_ah: ActivityHarmonization = copy.deepcopy(ah)

# Interlink correspondence files
correspondence_obj.interlink_correspondence_files((3, 8), (3, 12))
ecoinvent_correspondence_v38_to_v312: pd.DataFrame = correspondence_obj.df_interlinked_data[((3, 8), (3, 12))]
ecoinvent_correspondence_v38_to_v312.to_excel(output_path / "interlinked_correspondence_files_38_312.xlsx")

# Add correspondence mappings to activity harmonization class
for idx, row in ecoinvent_correspondence_v38_to_v312.iterrows():
    
    source: ActivityDefinition = ActivityDefinition(activity_code = row.get("FROM_activity_uuid"),
                                                    reference_product_code = row.get("FROM_reference_product_uuid"),
                                                    activity_name = row.get("FROM_activity_name"),
                                                    reference_product_name = row.get("FROM_reference_product_name"),
                                                    name = None,
                                                    simapro_name = None,
                                                    location = row.get("FROM_location"),
                                                    unit = row.get("FROM_unit")
                                                    )
    
    target: ActivityDefinition = ActivityDefinition(activity_code = row.get("TO_activity_uuid"),
                                                    reference_product_code = row.get("TO_reference_product_uuid"),
                                                    activity_name = row.get("TO_activity_name"),
                                                    reference_product_name = row.get("TO_reference_product_name"),
                                                    name = None,
                                                    simapro_name = None,
                                                    location = row.get("TO_location"),
                                                    unit = row.get("TO_unit")
                                                    )
    
    agrifootprint_ah.add_to_correspondence_mapping(source = source,
                                                   target = target,
                                                   multiplier = row.get("Multiplier")
                                                   )

# Load dataframe with custom migration
agrifootprint_custom_migration_df: pd.DataFrame = pd.read_excel(LCI_ecoinvent_xml_folderpath / filename_SBERT_activity_names_validated)

# Add custom mappings to activity harmonization class
for idx, row in agrifootprint_custom_migration_df.iterrows():
    
    source: ActivityDefinition = ActivityDefinition(activity_code = None,
                                                    reference_product_code = None,
                                                    activity_name = None,
                                                    reference_product_name = None,
                                                    name = row.get("FROM_name"),
                                                    simapro_name = None,
                                                    location = row.get("FROM_location"),
                                                    unit = row.get("FROM_unit")
                                                    )
    
    target: ActivityDefinition = ActivityDefinition(activity_code = None,
                                                    reference_product_code = None,
                                                    activity_name = None,
                                                    reference_product_name = None,
                                                    name = row.get("TO_name"),
                                                    simapro_name = None,
                                                    location = row.get("TO_location"),
                                                    unit = row.get("TO_unit")
                                                    )
    
    agrifootprint_ah.add_to_custom_mapping(source = source,
                                           target = target,
                                           multiplier = row.get("multiplier")
                                           )

direct_mapping: dict = agrifootprint_ah.direct_mapping
custom_mapping: dict = agrifootprint_ah.custom_mapping
correspondence_mapping: dict = agrifootprint_ah.correspondence_mapping
sbert_mapping: dict = agrifootprint_ah.SBERT_mapping
# !!!

agrifootprint_successful: tuple = ()
agrifootprint_unsuccessful_queries: tuple = ()
agrifootprint_unsuccessful: tuple = ()

for _, exc in agrifootprint_exchanges_to_migrate_to_ecoinvent.items():
    
    query: ActivityDefinition = ActivityDefinition(activity_code = exc.get("activity_code"), 
                                                   reference_product_code = exc.get("reference_product_code"), 
                                                   activity_name = exc.get("activity_name"),
                                                   reference_product_name = exc.get("reference_product_name"),
                                                   name = exc.get("name"),
                                                   simapro_name = exc.get("SimaPro_name"),
                                                   location = exc.get("location"),
                                                   unit = exc.get("unit")
                                                   )
    
    if agrifootprint_ah.map_directly(query = query) != ():
        agrifootprint_successful += agrifootprint_ah.map_directly(query = query) + ("Direct mapping",)
        
    elif agrifootprint_ah.map_using_custom_mapping(query = query) != ():
        agrifootprint_successful += agrifootprint_ah.map_using_custom_mapping(query = query) + ("Custom mapping",)
    
    elif agrifootprint_ah.map_using_correspondence_mapping(query = query) != ():
        agrifootprint_successful += agrifootprint_ah.map_using_correspondence_mapping(query = query) + ("Correspondence mapping",)
        
    else:
        agrifootprint_unsuccessful_queries += (query,)
            
print("{} ecoinvent background exchanges from {} migrated".format(len(agrifootprint_successful), agrifootprint_db_name_updated_simapro))

if len(agrifootprint_unsuccessful_queries) > 0:
    agrifootprint_ah.map_using_SBERT(queries = agrifootprint_unsuccessful_queries)
    # agrifootprint_successful += agrifootprint_ah.map_using_correspondence_mapping(query = query) + ("Correspondence mapping",)


AGF_background_ei_migration: dict = create_harmonized_activity_migration(flows_1 = list(agrifootprint_exchanges_to_migrate_to_ecoinvent.values()),
                                                                         flows_2 = activities_to_migrate_to,
                                                                         df_custom_mapping = None,
                                                                         df_ecoinvent_correspondence_mapping = ecoinvent_correspondence_v38_to_v312,
                                                                         use_SBERT_for_mapping = True)

# SBERT to map for activities
unsuccessfully_migrated += [{**{"FROM_" + k: v for k, v in m.items()}, **{"Database": agrifootprint_db_name_updated_simapro}} for m, _ in AGF_background_ei_migration["unsuccessfully_migrated_activity_flows"]]
successfully_migrated += [{**{"FROM_" + k: v for k, v in m.items()}, **{"TO_" + k: v for k, v in n.items()}, **{"Database": agrifootprint_db_name_updated_simapro}} for m, n in AGF_background_ei_migration["successfully_migrated_activity_flows"]]
SBERT_to_map += [{**m, **{"Database": agrifootprint_db_name_updated_simapro}} for m in AGF_background_ei_migration["SBERT_to_map"]]

# Create the JSON object to be written
AGF_background_ei_migration_in_json_format = json.dumps(AGF_background_ei_migration["activity_migration"], indent = 3)

# Write the activity dictionary to a JSON file
with open(filepath_AGF_background_ei_migration_data, "w") as outfile:
   outfile.write(AGF_background_ei_migration_in_json_format)

# Free up memory
del agrifootprint_exchanges_to_migrate_to_ecoinvent, AGF_background_ei_migration, AGF_background_ei_migration_in_json_format


#%% Update the ecoinvent background activities in the AgriFootprint database from v3.8 to v3.10 and register database
agrifootprint_db_updated_simapro.apply_strategy(partial(link.remove_linking,
                                                        production_exchanges = False,
                                                        substitution_exchanges = True,
                                                        technosphere_exchanges = True,
                                                        biosphere_exchanges = False), verbose = True)

# Apply activity migration
agrifootprint_db_updated_simapro.apply_strategy(partial(migrate_from_json_file,
                                                        json_migration_filepath = filepath_AGF_background_ei_migration_data,
                                                        migrate_activities = False,
                                                        migrate_exchanges = True),
                                        verbose = True)

# Link to ecoinvent externally
agrifootprint_db_updated_simapro.apply_strategy(partial(link.link_activities_externally,
                                                        link_to_databases = (ecoinvent_db_name_xml_migrated,),
                                                        link_production_exchanges = False,
                                                        link_substitution_exchanges = True,
                                                        link_technosphere_exchanges = True,
                                                        relink = False,
                                                        case_insensitive = True,
                                                        strip = True,
                                                        remove_special_characters = False,
                                                        verbose = True), verbose = True)

# Link remaining ones that were not updated
agrifootprint_db_updated_simapro.apply_strategy(partial(link.link_activities_internally,
                                                        production_exchanges = False,
                                                        substitution_exchanges = True,
                                                        technosphere_exchanges = True,
                                                        relink = False,
                                                        strip = True,
                                                        case_insensitive = True,
                                                        remove_special_characters = False,
                                                        verbose = True), verbose = True)


# Show statistic of current linking of database import
print("\n-----------Linking statistics of current database import")
agrifootprint_n_datasets, agrifootprint_n_exchanges, agrifootprint_n_unlinked = agrifootprint_db_updated_simapro.statistics()
print()

# Write unlinked biosphere flows to XLSX
print("\n-----------Write unlinked flows to excel file")
agrifootprint_db_updated_simapro.write_excel(only_unlinked = True)

# Delete database first, if existing
if agrifootprint_db_name_updated_simapro in bw2data.databases:
    print("\n-----------Delete database: " + agrifootprint_db_name_updated_simapro)
    del bw2data.databases[agrifootprint_db_name_updated_simapro]

# # Write database
# print("\n-----------Write database: " + agrifootprint_db_name_updated_simapro)
# agrifootprint_db_updated_simapro.write_database(overwrite = False)
# print()

# # Free up memory
# del agrifootprint_db_updated_simapro


#%% Create migration tables
cols_order: tuple[str] = ("Database",
                          "FROM_type",
                          "FROM_activity_code",
                          "FROM_reference_product_code",
                          "FROM_activity_name",
                          "FROM_reference_product_name",
                          "FROM_name",
                          "FROM_SimaPro_name",
                          "FROM_unit",
                          "FROM_location",
                          "TO_code",
                          "TO_type",
                          "TO_activity_code",
                          "TO_reference_product_code",
                          "TO_activity_name",
                          "TO_reference_product_name",
                          "TO_SimaPro_name",
                          "TO_unit",
                          "TO_location",
                          "TO_multiplier"
                          )

pd.DataFrame([{n: m.get(n) for n in cols_order} for m in successfully_migrated]).to_excel(output_path / "background_ei_flows_successfully_migrated.xlsx")
pd.DataFrame([{n: m.get(n) for n in cols_order} for m in unsuccessfully_migrated]).to_excel(output_path / "background_ei_flows_unsuccessfully_migrated.xlsx")
pd.DataFrame(SBERT_to_map).to_excel(output_path / "background_ei_flows_SBERT_to_map.xlsx")

#%% Run LCA calculation

# Methods to use for the LCA calculation
simapro_EF_LCIA_name: str = "Environmental Footprint v3.1"
ecoinvent_EF_LCIA_name: str = "EF v3.1"
simapro_methods: list[tuple[str]] = [m for m in bw2data.methods if m[0] == simapro_EF_LCIA_name]
ecoinvent_methods: list[tuple[str]] = [m for m in bw2data.methods if m[0] == ecoinvent_EF_LCIA_name]
methods_all: list[tuple] = list(bw2data.methods)

# Check if all specified methods are registered in the Brightway background
for method in simapro_methods + ecoinvent_methods:
    error: bool = False
    if method not in bw2data.methods:
        error: bool = True
        print("Method not registered: '{}'".format(method))

# If unregistered methods have been detected, raise error
if error:
    raise ValueError("Unregistered methods detected.")


#%% Comparison 1

# Extract all inventories
# ... from ecoinvent (SimaPro)
ecoinvent_simapro_inventories: list = [m for m in bw2data.Database(ecoinvent_db_name_simapro)]

# ... from ecoinvent (XML)
ecoinvent_xml_inventories: list = [m for m in bw2data.Database(ecoinvent_db_name_xml)]

# Run LCA calculation
# LCA_results_ecoinvent_simapro: dict[str, pd.DataFrame] = run_LCA(activities = ecoinvent_simapro_inventories,
#                                                                  methods = simapro_methods,
#                                                                  write_LCI_exchanges = False,
#                                                                  write_LCI_exchanges_as_emissions = False,
#                                                                  write_LCIA_impacts_of_activity_exchanges = False,
#                                                                  write_LCIA_process_contribution = False,
#                                                                  write_LCIA_emission_contribution = False,
#                                                                  write_characterization_factors = False,
#                                                                  cutoff_process = 0.001,
#                                                                  cutoff_emission = 0.001,
#                                                                  write_results_to_file = True,
#                                                                  local_output_path = output_path,
#                                                                  filename_without_ending = "ecoinvent_SimaPro",
#                                                                  use_timestamp_in_filename = True,
#                                                                  print_progress_bar = True)

lca_ecoinvent_simapro: LCA_Calculation = LCA_Calculation(activities = ecoinvent_simapro_inventories,
                                                         methods = simapro_methods)
lca_ecoinvent_simapro.calculate(calculate_LCIA_scores = True)
LCA_results_ecoinvent_simapro: dict[str, pd.DataFrame] = lca_ecoinvent_simapro.get_results(extended = True)
lca_ecoinvent_simapro.write_results(path = output_path,
                                    filename = "LCA_results_ecoinvent_simapro",
                                    use_timestamp_in_filename = True,
                                    extended = True)

# Run LCA calculation
# LCA_results_ecoinvent_xml: dict[str, pd.DataFrame] = run_LCA(activities = ecoinvent_xml_inventories,
#                                                              methods = ecoinvent_methods,
#                                                              write_LCI_exchanges = False,
#                                                              write_LCI_exchanges_as_emissions = False,
#                                                              write_LCIA_impacts_of_activity_exchanges = False,
#                                                              write_LCIA_process_contribution = False,
#                                                              write_LCIA_emission_contribution = False,
#                                                              write_characterization_factors = False,
#                                                              cutoff_process = 0.001,
#                                                              cutoff_emission = 0.001,
#                                                              write_results_to_file = True,
#                                                              local_output_path = output_path,
#                                                              filename_without_ending = "ecoinvent_XML",
#                                                              use_timestamp_in_filename = True,
#                                                              print_progress_bar = True)

lca_ecoinvent_xml: LCA_Calculation = LCA_Calculation(activities = ecoinvent_xml_inventories,
                                                     methods = ecoinvent_methods)
lca_ecoinvent_xml.calculate(calculate_LCIA_scores = True)
LCA_results_ecoinvent_xml: dict[str, pd.DataFrame] = lca_ecoinvent_xml.get_results(extended = True)
lca_ecoinvent_xml.write_results(path = output_path,
                                filename = "LCA_results_ecoinvent_xml",
                                use_timestamp_in_filename = True,
                                extended = True)

ecoinvent_xml_code_to_simapro_code_mapping: dict = {m["activity_code"] + "_" + m["reference_product_code"]: m["code"] for m in ecoinvent_simapro_inventories if m["activity_code"] is not None and m["reference_product_code"] is not None}

for m in LCA_results_ecoinvent_xml["LCIA_scores"]:
    m["Activity_code"]: str = ecoinvent_xml_code_to_simapro_code_mapping[m["Activity_code"]]

df_1: pd.DataFrame = pd.DataFrame(LCA_results_ecoinvent_simapro["LCIA_scores"] + LCA_results_ecoinvent_xml["LCIA_scores"])
df_1.to_csv(output_path / "comparison_ecoinvent_SimaPro_XML.csv")

df_LCIA_mapping: pd.DataFrame = pd.read_excel(here / "LCIA_method_mapping.xlsx")

LCIA_mapping: dict = {}

for idx, line in df_LCIA_mapping.iterrows():
    LCIA_mapping[literal_eval(line["SimaPro"])] = line["Standardized_name"]
    LCIA_mapping[literal_eval(line["XML"])] = line["Standardized_name"]

df_1["Method_standardized"] = [LCIA_mapping.get(m, "") for m in list(df_1["Method"])]
df_1.query("Method_standardized != ''").to_csv(output_path / "comparison_ecoinvent_SimaPro_XML_filtered.csv")

cfs_simapro: list[dict] = lca_ecoinvent_simapro.get_characterization_factors(extended = True)
cfs_xml: list[dict] = lca_ecoinvent_xml.get_characterization_factors(extended = True)

df_cfs_simapro: pd.DataFrame = pd.DataFrame(cfs_simapro)
df_cfs_xml: pd.DataFrame = pd.DataFrame(cfs_xml)

df_cfs_simapro.to_excel(output_path / "cfs_simapro.xlsx")
df_cfs_xml.to_excel(output_path / "cfs_xml.xlsx")

# Free up memory
# del LCA_results_ecoinvent_simapro, LCA_results_ecoinvent_xml

# code = "b0364f820cd591d7698e0df3f24f2f93"
# database = "ecoinvent v3.11 - SimaPro - regionalized"
# act = bw2data.Database(database).get(code)
# lca_specific_act_simapro: LCA_Calculation = LCA_Calculation(activities = [act],
#                                                          methods = simapro_methods,
#                                                          functional_amount = 1,
#                                                          cut_off_percentage = 0.001,
#                                                          exchange_level = 1,)
# lca_specific_act_simapro.calculate(calculate_LCIA_scores = True,
#                                    extract_LCI_exchanges = True,
#                                    extract_LCI_emission_contribution = True,
#                                    extract_LCI_process_contribution = True,
#                                    calculate_LCIA_scores_of_exchanges = True,
#                                    calculate_LCIA_emission_contribution = True,
#                                    calculate_LCIA_process_contribution = True)
# LCA_results_specific_act_simapro: dict[str, pd.DataFrame] = lca_specific_act_simapro.get_results(extended = True)
# lca_specific_act_simapro.write_results(output_path)

#%% Comparison 2

# Extract all inventories
# ... from Agribalyse (SimaPro) with ecoinvent v3.8 background
agribalyse_simapro_inventories: list = [m for m in bw2data.Database(agribalyse_db_name_simapro)]

# ... from Agribalyse (SimaPro) with ecoinvent v3.10 background
agribalyse_updated_simapro_inventories: list = [m for m in bw2data.Database(agribalyse_db_name_updated_simapro)]

# ... from WFLDB (SimaPro) with ecoinvent v3.5 background
wfldb_simapro_inventories: list = [m for m in bw2data.Database(wfldb_db_name_simapro)]

# ... from WFLDB (SimaPro) with ecoinvent v3.10 background
wfldb_updated_simapro_inventories: list = [m for m in bw2data.Database(wfldb_db_name_updated_simapro)]

# ... from SALCA (SimaPro) with ecoinvent v3.10 background
salca_simapro_inventories: list = [m for m in bw2data.Database(salca_db_name_simapro)]

# ... from SALCA (SimaPro) with ecoinvent v3.10 background
salca_updated_simapro_inventories: list = [m for m in bw2data.Database(salca_db_name_updated_simapro)]

# ... from AgriFootprint (SimaPro) with ecoinvent v3.8 background
agrifootprint_simapro_inventories: list = [m for m in bw2data.Database(agrifootprint_db_name_simapro)]

# ... from AgriFootprint (SimaPro) with ecoinvent v3.10 background
agrifootprint_updated_simapro_inventories: list = [m for m in bw2data.Database(agrifootprint_db_name_updated_simapro)]

# Run LCA calculation
# LCA_results_agribalyse_simapro: dict[str, pd.DataFrame] = run_LCA(activities = agribalyse_simapro_inventories,
#                                                                   methods = simapro_methods,
#                                                                   write_LCI_exchanges = False,
#                                                                   write_LCI_exchanges_as_emissions = False,
#                                                                   write_LCIA_impacts_of_activity_exchanges = False,
#                                                                   write_LCIA_process_contribution = False,
#                                                                   write_LCIA_emission_contribution = False,
#                                                                   write_characterization_factors = False,
#                                                                   cutoff_process = 0.001,
#                                                                   cutoff_emission = 0.001,
#                                                                   write_results_to_file = True,
#                                                                   local_output_path = output_path,
#                                                                   filename_without_ending = "agribalyse_SimaPro",
#                                                                   use_timestamp_in_filename = True,
#                                                                   print_progress_bar = True)

lca_agribalyse_simapro: LCA_Calculation = LCA_Calculation(activities = agribalyse_simapro_inventories,
                                                          methods = simapro_methods)
lca_agribalyse_simapro.calculate(calculate_LCIA_scores = True)
LCA_results_agribalyse_simapro: dict[str, pd.DataFrame] = lca_agribalyse_simapro.get_results(extended = True)


# Run LCA calculation
# LCA_results_agribalyse_updated_simapro: dict[str, pd.DataFrame] = run_LCA(activities = agribalyse_updated_simapro_inventories,
#                                                                           methods = ecoinvent_methods,
#                                                                           write_LCI_exchanges = False,
#                                                                           write_LCI_exchanges_as_emissions = False,
#                                                                           write_LCIA_impacts_of_activity_exchanges = False,
#                                                                           write_LCIA_process_contribution = False,
#                                                                           write_LCIA_emission_contribution = False,
#                                                                           write_characterization_factors = False,
#                                                                           cutoff_process = 0.001,
#                                                                           cutoff_emission = 0.001,
#                                                                           write_results_to_file = True,
#                                                                           local_output_path = output_path,
#                                                                           filename_without_ending = "agribalyse_updated_SimaPro",
#                                                                           use_timestamp_in_filename = True,
#                                                                           print_progress_bar = True)

lca_agribalyse_updated_simapro: LCA_Calculation = LCA_Calculation(activities = agribalyse_updated_simapro_inventories,
                                                                  methods = simapro_methods)
lca_agribalyse_updated_simapro.calculate(calculate_LCIA_scores = True)
LCA_results_agribalyse_updated_simapro: dict[str, pd.DataFrame] = lca_agribalyse_updated_simapro.get_results(extended = True)

# Run LCA calculation
# LCA_results_wfldb_simapro: dict[str, pd.DataFrame] = run_LCA(activities = wfldb_simapro_inventories,
#                                                              methods = simapro_methods,
#                                                              write_LCI_exchanges = False,
#                                                              write_LCI_exchanges_as_emissions = False,
#                                                              write_LCIA_impacts_of_activity_exchanges = False,
#                                                              write_LCIA_process_contribution = False,
#                                                              write_LCIA_emission_contribution = False,
#                                                              write_characterization_factors = False,
#                                                              cutoff_process = 0.001,
#                                                              cutoff_emission = 0.001,
#                                                              write_results_to_file = True,
#                                                              local_output_path = output_path,
#                                                              filename_without_ending = "wfldb_SimaPro",
#                                                              use_timestamp_in_filename = True,
#                                                              print_progress_bar = True)

lca_wfldb_simapro: LCA_Calculation = LCA_Calculation(activities = wfldb_simapro_inventories,
                                                     methods = simapro_methods)
lca_wfldb_simapro.calculate(calculate_LCIA_scores = True)
LCA_results_wfldb_simapro: dict[str, pd.DataFrame] = lca_wfldb_simapro.get_results(extended = True)

# Run LCA calculation
# LCA_results_wfldb_updated_simapro: dict[str, pd.DataFrame] = run_LCA(activities = wfldb_updated_simapro_inventories,
#                                                                      methods = ecoinvent_methods,
#                                                                      write_LCI_exchanges = False,
#                                                                      write_LCI_exchanges_as_emissions = False,
#                                                                      write_LCIA_impacts_of_activity_exchanges = False,
#                                                                      write_LCIA_process_contribution = False,
#                                                                      write_LCIA_emission_contribution = False,
#                                                                      write_characterization_factors = False,
#                                                                      cutoff_process = 0.001,
#                                                                      cutoff_emission = 0.001,
#                                                                      write_results_to_file = True,
#                                                                      local_output_path = output_path,
#                                                                      filename_without_ending = "wfldb_updated_SimaPro",
#                                                                      use_timestamp_in_filename = True,
#                                                                      print_progress_bar = True)

lca_wfldb_updated_simapro: LCA_Calculation = LCA_Calculation(activities = wfldb_updated_simapro_inventories,
                                                             methods = simapro_methods)
lca_wfldb_updated_simapro.calculate(calculate_LCIA_scores = True)
LCA_results_wfldb_updated_simapro: dict[str, pd.DataFrame] = lca_wfldb_updated_simapro.get_results(extended = True)

# Run LCA calculation
# LCA_results_salca_simapro: dict[str, pd.DataFrame] = run_LCA(activities = salca_simapro_inventories,
#                                                              methods = simapro_methods,
#                                                              write_LCI_exchanges = False,
#                                                              write_LCI_exchanges_as_emissions = False,
#                                                              write_LCIA_impacts_of_activity_exchanges = False,
#                                                              write_LCIA_process_contribution = False,
#                                                              write_LCIA_emission_contribution = False,
#                                                              write_characterization_factors = False,
#                                                              cutoff_process = 0.001,
#                                                              cutoff_emission = 0.001,
#                                                              write_results_to_file = True,
#                                                              local_output_path = output_path,
#                                                              filename_without_ending = "salca_SimaPro",
#                                                              use_timestamp_in_filename = True,
#                                                              print_progress_bar = True)

lca_salca_simapro: LCA_Calculation = LCA_Calculation(activities = salca_simapro_inventories,
                                                     methods = simapro_methods)
lca_salca_simapro.calculate(calculate_LCIA_scores = True)
LCA_results_salca_simapro: dict[str, pd.DataFrame] = lca_salca_simapro.get_results(extended = True)

# Run LCA calculation
# LCA_results_salca_updated_simapro: dict[str, pd.DataFrame] = run_LCA(activities = salca_updated_simapro_inventories,
#                                                                      methods = ecoinvent_methods,
#                                                                      write_LCI_exchanges = False,
#                                                                      write_LCI_exchanges_as_emissions = False,
#                                                                      write_LCIA_impacts_of_activity_exchanges = False,
#                                                                      write_LCIA_process_contribution = False,
#                                                                      write_LCIA_emission_contribution = False,
#                                                                      write_characterization_factors = False,
#                                                                      cutoff_process = 0.001,
#                                                                      cutoff_emission = 0.001,
#                                                                      write_results_to_file = True,
#                                                                      local_output_path = output_path,
#                                                                      filename_without_ending = "salca_updated_SimaPro",
#                                                                      use_timestamp_in_filename = True,
#                                                                      print_progress_bar = True)

lca_salca_updated_simapro: LCA_Calculation = LCA_Calculation(activities = salca_updated_simapro_inventories,
                                                             methods = simapro_methods)
lca_salca_updated_simapro.calculate(calculate_LCIA_scores = True)
LCA_results_salca_updated_simapro: dict[str, pd.DataFrame] = lca_salca_updated_simapro.get_results(extended = True)

# Run LCA calculation
# LCA_results_agrifootprint_simapro: dict[str, pd.DataFrame] = run_LCA(activities = agrifootprint_simapro_inventories,
#                                                              methods = simapro_methods,
#                                                              write_LCI_exchanges = False,
#                                                              write_LCI_exchanges_as_emissions = False,
#                                                              write_LCIA_impacts_of_activity_exchanges = False,
#                                                              write_LCIA_process_contribution = False,
#                                                              write_LCIA_emission_contribution = False,
#                                                              write_characterization_factors = False,
#                                                              cutoff_process = 0.001,
#                                                              cutoff_emission = 0.001,
#                                                              write_results_to_file = True,
#                                                              local_output_path = output_path,
#                                                              filename_without_ending = "agrifootprint_SimaPro",
#                                                              use_timestamp_in_filename = True,
#                                                              print_progress_bar = True)

lca_agrifootprint_simapro: LCA_Calculation = LCA_Calculation(activities = agrifootprint_simapro_inventories,
                                                     methods = simapro_methods)
lca_agrifootprint_simapro.calculate(calculate_LCIA_scores = True)
LCA_results_agrifootprint_simapro: dict[str, pd.DataFrame] = lca_agrifootprint_simapro.get_results(extended = True)

# Run LCA calculation
# LCA_results_agrifootprint_updated_simapro: dict[str, pd.DataFrame] = run_LCA(activities = agrifootprint_updated_simapro_inventories,
#                                                                      methods = ecoinvent_methods,
#                                                                      write_LCI_exchanges = False,
#                                                                      write_LCI_exchanges_as_emissions = False,
#                                                                      write_LCIA_impacts_of_activity_exchanges = False,
#                                                                      write_LCIA_process_contribution = False,
#                                                                      write_LCIA_emission_contribution = False,
#                                                                      write_characterization_factors = False,
#                                                                      cutoff_process = 0.001,
#                                                                      cutoff_emission = 0.001,
#                                                                      write_results_to_file = True,
#                                                                      local_output_path = output_path,
#                                                                      filename_without_ending = "agrifootprint_updated_SimaPro",
#                                                                      use_timestamp_in_filename = True,
#                                                                      print_progress_bar = True)

lca_agrifootprint_updated_simapro: LCA_Calculation = LCA_Calculation(activities = agrifootprint_updated_simapro_inventories,
                                                                     methods = simapro_methods)
lca_agrifootprint_updated_simapro.calculate(calculate_LCIA_scores = True)
LCA_results_agrifootprint_updated_simapro: dict[str, pd.DataFrame] = lca_agrifootprint_updated_simapro.get_results(extended = True)

LCA_results_to_join: list[dict] = (LCA_results_agribalyse_simapro["LCIA_scores"] +
                                   LCA_results_agribalyse_updated_simapro["LCIA_scores"] +
                                   LCA_results_wfldb_simapro["LCIA_scores"] +
                                   LCA_results_wfldb_updated_simapro["LCIA_scores"] +
                                   LCA_results_salca_simapro["LCIA_scores"] +
                                   LCA_results_salca_updated_simapro["LCIA_scores"] +
                                   LCA_results_agrifootprint_simapro["LCIA_scores"] +
                                   LCA_results_agrifootprint_updated_simapro["LCIA_scores"]
                                   )

df_2: pd.DataFrame = pd.DataFrame(LCA_results_to_join)
df_2.to_csv(output_path / "comparison_updated_background.csv")

df_2["Method_standardized"] = [LCIA_mapping.get(m, "") for m in list(df_2["Method"])]
df_2.query("Method_standardized != ''").to_excel(output_path / "comparison_updated_background_filtered.xlsx")

#%%
for method_name, _ in LCIA_mapping.items():
    method = bw2data.Method(method_name).load()
    print(str(len(method)) + " char. factors, " + str(method_name))
    print()

#%%
# import pandas as pd
water_use_method: tuple = ('Environmental Footprint v3.1', 'Water use')
lca_calculation: LCA_Calculation = LCA_Calculation(activities = [], methods = [water_use_method])
water_use_cfs: list[dict] = lca_calculation.get_characterization_factors([water_use_method])
water_use_cfs_df: pd.DataFrame = pd.DataFrame(water_use_cfs)
water_use_cfs_df.to_excel(output_path / "water_use_cfs.xlsx")



#%%
files: list[tuple] = [# ("Correspondence-File-v3.1-v3.2.xlsx", (3, 1), (3, 2)),
                      # ("Correspondence-File-v3.2-v3.3.xlsx", (3, 2), (3, 3)),
                      # ("Correspondence-File-v3.3-v3.4.xlsx", (3, 3), (3, 4)),
                      # ("Correspondence-File-v3.4-v3.5.xlsx", (3, 4), (3, 5)),
                      # ("Correspondence-File-v3.5-v3.6.xlsx", (3, 5), (3, 6)),
                      # ("Correspondence-File-v3.6-v3.7.1.xlsx", (3, 6), (3, 7, 1)),
                      # ("Correspondence-File-v3.7.1-v3.8.xlsx", (3, 7, 1), (3, 8)),
                      ("Correspondence-File-v3.8-v3.9.1.xlsx", (3, 8), (3, 9, 1)),
                      # ("Correspondence-File-v3.8-v3.9.xlsx", (3, 8), (3, 9)),
                      ("Correspondence-File-v3.9.1-v3.10.xlsx", (3, 9, 1), (3, 10)),
                      ("Correspondence-File-v3.10.1-v3.11.xlsx", (3, 10, 1), (3, 11)),
                      ("Correspondence-File-v3.10-v3.10.1.xlsx", (3, 10), (3, 10, 1)),
                      ("Correspondence-File-v3.11-v3.12.xlsx", (3, 11), (3, 12)),
                      ]

correspondence: Correspondence = Correspondence(ecoinvent_model_type = "cutoff")

for filename, FROM_version, TO_version in files:
    correspondence.read_correspondence_dataframe(filepath_correspondence_excel = here.parent / "correspondence" / "data" / filename,
                                                 FROM_version = FROM_version,
                                                 TO_version = TO_version
                                                 )

# correspondence_raw_data: dict = correspondence.raw_data
# correspondence_interlinked_data: dict = correspondence.df_interlinked_data
correspondence.interlink_correspondence_files((3, 8), (3, 12))

#%%

df_interlinked_v38: pd.DataFrame = correspondence.df_interlinked_data[((3, 8), (3, 12))]
correspondence_fields_v38, correspondence_mapping_v38 = create_structured_migration_dictionary_from_excel(df_interlinked_v38)

#%%
agrifootprint_exchanges_to_migrate_to_ecoinvent: dict = {(exc["name"], exc["unit"], exc["location"]): exc for ds in list(agrifootprint_db_updated_simapro) for exc in ds["exchanges"] if exc["type"] not in ["production", "biosphere"] and exc.get("is_ecoinvent", False)}


#%%

def create_activity_IDs(activity_code: (str | None),
                        reference_product_code: (str | None),
                        SimaPro_name: (str | None),
                        activity_name: (str | None),
                        reference_product_name: (str | None),
                        location: (str | None),
                        unit: (str | None),
                        models: list,
                        systems: list,
                        ) -> list[tuple[(str | None)]]:
    
    
    if not isinstance(activity_code, str):
        activity_code = None
        
    if not isinstance(reference_product_code, str):
        reference_product_code = None
        
    if not isinstance(SimaPro_name, str):
        SimaPro_name = None
        
    if not isinstance(activity_name, str):
        activity_name = None
        
    if not isinstance(reference_product_name, str):
        reference_product_name = None
    
    if not isinstance(location, str):
        location = None
        
    if not isinstance(unit, str):
        unit = None
    
    # IDs are tuples of format
    # ("activity_code", "reference_product_code", "SimaPro_name", "activity_name", "reference_product_name", "location", "unit")
    IDs: list = []
     
    if SimaPro_name is not None and unit is not None:
        IDs += [(None, None, SimaPro_name, None, None, None, unit)]
    
    if reference_product_name is not None and activity_name is not None and location is not None and unit is not None and models != [] and systems != []:
        
        for model in models:
            
            if not isinstance(model, str):
                continue
            
            for system in systems:
                
                if not isinstance(system, str):
                    continue
            
                created_SimaPro_name: (str | None) = "{} {{{}}}|{} | {}, {}".format(reference_product_name, location, activity_name, model, system)
                IDs += [(None, None, created_SimaPro_name, None, None, None, unit)]
    
    if activity_name is None and reference_product_name is not None and location is not None and unit is not None:
        IDs += [(None, None, None, None, reference_product_name, location, unit)]
    
    if activity_name is not None and reference_product_name is not None and location is not None and unit is not None:
        IDs += [(None, None, None, activity_name, reference_product_name, location, unit)]
        IDs += [(None, None, None, None, reference_product_name + " " + activity_name, location, unit)]
        IDs += [(None, None, None, None, activity_name + " " + reference_product_name, location, unit)]
    
    if activity_code is not None and reference_product_code is not None:
        IDs += [(activity_code, reference_product_code, None, None, None, None, None)]

    return list(set(IDs))


def find_activity(IDs: list[tuple], multiplier: (float | int), mapping: dict) -> list[tuple]:
    
    for ID in IDs:
        found: (list[tuple] | None) = mapping.get(ID)
        
        if found is not None:
            return [(m[0], m[1] * multiplier) for m in found]
    
    return []


flows_1: list[dict] = copy.deepcopy(list(agrifootprint_exchanges_to_migrate_to_ecoinvent.values()))
flows_2: list[dict] = activities_to_migrate_to
df_custom_mapping: (pd.DataFrame | None) = None
df_ecoinvent_correspondence_mapping: (pd.DataFrame | None) = ecoinvent_correspondence_v38_to_v312
use_SBERT_for_mapping: bool = True

models: list[str] = ["Cut-off", "Cut-Off", "cutoff"]
systems: list[str] = ["U", "Unit", "S", "System"]

direct_mapping: dict = {}
custom_mapping: dict = {}
correspondence_mapping: dict = {}
SBERT_mapping: dict = {}

# Loop through each of the activity that can be mapped to
for act in flows_2:
    
    FROM_IDs: list[tuple] = create_activity_IDs(activity_code = act.get("activity_code"),
                                                reference_product_code = act.get("reference_product_code"),
                                                SimaPro_name = act.get("SimaPro_name"),
                                                activity_name = act.get("activity_name"),
                                                reference_product_name = act.get("reference_product_name"),
                                                location = act.get("location"),
                                                unit = act.get("unit"),
                                                models = models,
                                                systems = systems)
    
    for FROM_ID in FROM_IDs:
        if direct_mapping.get(FROM_ID) is None:
            direct_mapping[FROM_ID] = [(copy.deepcopy(act), 1)]

# print(set([len(m) for _, m in direct_mapping.items()]))

if isinstance(df_ecoinvent_correspondence_mapping, pd.DataFrame):
    
    direct_mapping_copied = copy.deepcopy(direct_mapping)
    
    for idx, row in df_ecoinvent_correspondence_mapping.iterrows():
        TO_IDs: list[tuple] = create_activity_IDs(activity_code = row["TO_activity_uuid"],
                                                  reference_product_code = row["TO_reference_product_uuid"],
                                                  SimaPro_name = None,
                                                  activity_name = row["TO_activity_name"],
                                                  reference_product_name = row["TO_reference_product_name"],
                                                  location = row["TO_location"],
                                                  unit = row["TO_unit"],
                                                  models = models,
                                                  systems = systems)
        
        found: list[tuple] = find_activity(IDs = TO_IDs, multiplier = row["Multiplier"], mapping = direct_mapping_copied)
        
        if found != []:
            FROM_IDs: list[tuple] = create_activity_IDs(activity_code = row["FROM_activity_uuid"],
                                                        reference_product_code = row["FROM_reference_product_uuid"],
                                                        SimaPro_name = None,
                                                        activity_name = row["FROM_activity_name"],
                                                        reference_product_name = row["FROM_reference_product_name"],
                                                        location = row["FROM_location"],
                                                        unit = row["FROM_unit"],
                                                        models = models,
                                                        systems = systems)
        
            for FROM_ID in FROM_IDs:
                if correspondence_mapping.get(FROM_ID) is None:
                    correspondence_mapping[FROM_ID]: list[tuple] = copy.deepcopy(found)
                
                else:
                    correspondence_mapping[FROM_ID] += copy.deepcopy(found)
                    
    
    # Check that all multipliers sum up to exactly 1
    not_summing_to_1: list[tuple] = [str(k) + " --> " + str(sum([m for _, m in v])) for k, v in correspondence_mapping.items() if sum([m for _, m in v]) < 0.99 or sum([m for _, m in v]) > 1.01]
    
    if not_summing_to_1 != []:
        print("\n - ".join(set(not_summing_to_1)))
        raise ValueError()

print(set([len(m) for _, m in correspondence_mapping.items()]))
                    

if isinstance(df_custom_mapping, pd.DataFrame):
    
    direct_mapping_copied = copy.deepcopy(direct_mapping)
    
    for idx, row in df_custom_mapping.iterrows():
        TO_IDs: list[tuple] = create_activity_IDs(activity_code = row.get("TO_activity_uuid"),
                                                  reference_product_code = row.get("TO_reference_product_uuid"),
                                                  SimaPro_name = row.get("TO_SimaPro_name"),
                                                  activity_name = row.get("TO_activity_name"),
                                                  reference_product_name = row.get("TO_reference_product_name"),
                                                  location = row.get("TO_location"),
                                                  unit = row.get("TO_unit"),
                                                  models = models,
                                                  systems = systems)
        
        found: (list[tuple] | None) = find_activity(IDs = TO_IDs, multiplier = row.get("multiplier"), mapping = direct_mapping_copied)
        
        if found is not None:
            FROM_IDs: list[tuple] = create_activity_IDs(activity_code = row.get("FROM_activity_uuid"),
                                                        reference_product_code = row.get("FROM_reference_product_uuid"),
                                                        SimaPro_name = row.get("FROM_SimaPro_name"),
                                                        activity_name = row.get("FROM_activity_name"),
                                                        reference_product_name = row.get("FROM_reference_product_name"),
                                                        location = row.get("FROM_location"),
                                                        unit = row.get("FROM_unit"),
                                                        models = models,
                                                        systems = systems)
        
            for FROM_ID in FROM_IDs:
                if custom_mapping.get(FROM_ID) is None:
                    custom_mapping[FROM_ID] = found
                
                else:
                    custom_mapping[FROM_ID] += found


if use_SBERT_for_mapping:
    
    best_n_SBERT: int = 5
    SBERT_cutoff_for_inclusion: float = 0.95
    keys_to_use_for_SBERT_mapping: tuple[str] = ("SimaPro_name", "unit")
    
    FROM_SBERTs: tuple = tuple(set([tuple([m.get(n) for n in keys_to_use_for_SBERT_mapping]) for m in flows_1]))
    TO_SBERTs: tuple = tuple(set([tuple([m.get(n) for n in keys_to_use_for_SBERT_mapping]) for m in flows_2]))
    
    SBERTs = [(a, b) for a, b in zip(FROM_SBERTs, TO_SBERTs) if all([m is not None for m in a]) and all([n is not None for n in b])]
    FROM_SBERTs_cleaned: tuple = tuple([m for m, _ in SBERTs])
    TO_SBERTs_cleaned: tuple = tuple([m for _, m in SBERTs])
    
    # Map using SBERT
    df_SBERT_mapping: pd.DataFrame = map_using_SBERT(FROM_SBERTs_cleaned, TO_SBERTs_cleaned, best_n_SBERT)    
    
    for idx, row in df_SBERT_mapping.iterrows():
        
        if row["ranking"] != best_n_SBERT and row["score"] < SBERT_cutoff_for_inclusion:
            continue
        
        TO_IDs: list[tuple] = create_activity_IDs(activity_code = None,
                                                  reference_product_code = None,
                                                  SimaPro_name = row["mapped"][0],
                                                  activity_name = None,
                                                  reference_product_name = None,
                                                  location = None,
                                                  unit = row["mapped"][1],
                                                  models = models,
                                                  systems = systems)
        
        found: (list[tuple] | None) = find_activity(IDs = TO_IDs, multiplier = 1, mapping = direct_mapping)
        
        if found is not None:
            
            FROM_IDs: list[tuple] = create_activity_IDs(activity_code = None,
                                                        reference_product_code = None,
                                                        SimaPro_name = row["orig"][0],
                                                        activity_name = None,
                                                        reference_product_name = None,
                                                        location = None,
                                                        unit = row["orig"][1],
                                                        models = models,
                                                        systems = systems)
            
            for FROM_ID in FROM_IDs:
                if SBERT_mapping.get(FROM_ID) is None:
                    SBERT_mapping[FROM_ID] = found


# Initialize a list to store migration data --> tuple of FROM and TO dicts
successful_migration_data: list = []
unsuccessful_migration_data: list = []

# Loop through each flow
# Check if it should be mapped
# Map if possible
for exc in flows_1:
        
    exc_SimaPro_name = exc.get("SimaPro_name")
    exc_name = exc.get("name")
    exc_unit = exc.get("unit")
    exc_location = exc.get("location")
    exc_activity_name = exc.get("activity_name")
    exc_activity_code = exc.get("activity_code")
    exc_reference_product_name = exc.get("reference_product_name")
    exc_reference_product_code = exc.get("reference_product_code")
    
    if (exc_name, exc_location, exc_unit) in successful_migration_data:
        continue
    
    FROM_IDs: list[tuple] = create_activity_IDs(activity_code = exc_activity_code,
                                                reference_product_code = exc_reference_product_code,
                                                SimaPro_name = exc_SimaPro_name,
                                                activity_name = exc_activity_name,
                                                reference_product_name = exc_reference_product_name,
                                                location = exc_location,
                                                unit = exc_unit,
                                                models = models,
                                                systems = systems)
    
    found_from_direct_mapping = find_activity(IDs = FROM_IDs, multiplier = 1, mapping = direct_mapping)
    found_from_custom_mapping = find_activity(IDs = FROM_IDs, multiplier = 1, mapping = custom_mapping)
    found_from_correspondence_mapping = find_activity(IDs = FROM_IDs, multiplier = 1, mapping = correspondence_mapping)
    found_from_SBERT_mapping = find_activity(IDs = FROM_IDs, multiplier = 1, mapping = SBERT_mapping)
    
    if found_from_direct_mapping is not None:
        successful_migration_data += [(exc, found_from_direct_mapping, "Direct mapping")]
        
    elif found_from_custom_mapping is not None:
        successful_migration_data += [(exc, found_from_custom_mapping, "Custom mapping")]

    elif found_from_correspondence_mapping is not None:
        successful_migration_data += [(exc, found_from_correspondence_mapping, "Correspondence mapping")]
        
    elif found_from_SBERT_mapping is not None:
        successful_migration_data += [(exc, found_from_SBERT_mapping, "SBERT mapping")]
    
    else:
        unsuccessful_migration_data += [(exc, [], "Remains unmapped")]
        
# Specify the field from which we want to map
FROM_fields: tuple = ("name", "unit", "location")
    
# Specify the fields to which should be mapped to
TO_fields: tuple = ("code",
                    "name",
                    "SimaPro_name",
                    "categories",
                    # "top_category",
                    # "sub_category",
                    "SimaPro_categories",
                    "unit",
                    "SimaPro_unit",
                    "location"
                    )

# Initialize empty migration dictionary
successful_migration_dict: dict = {"fields": FROM_fields,
                                   "data": {}}
successful_migration_df: list = []
unsuccessful_migration_df = [{**{"FROM_" + k: v for k, v in m.items()}, "Used_Mapping": used_mapping} for m, _, used_mapping in unsuccessful_migration_data]

for FROM, TOs, used_mapping in successful_migration_data:
    
    FROM_tuple: tuple = tuple([FROM[m] for m in FROM_fields]) + ("technosphere",)
    TO_list_of_dicts: list[dict] = []
    
    for TO, multiplier in TOs:
        TO_dict: dict = {k: v for k, v in TO.items() if k in TO_fields}
        TO_list_of_dicts += [{**TO_dict, "multiplier": multiplier}]
    
    if str(FROM_tuple) not in successful_migration_dict:
        successful_migration_dict["data"][str(FROM_tuple)]: list = []
        
    successful_migration_dict["data"][str(FROM_tuple)] += TO_list_of_dicts

    FROM_dict_for_df: dict = {"FROM_" + m: FROM[m] for m in FROM_fields}
    TO_dicts_for_df: dict = [{("TO_" + k if k != "multiplier" else k): v for k, v in m.items()} for m in TO_list_of_dicts]
    successful_migration_df += [{**FROM_dict_for_df, **m} for m in TO_dicts_for_df]
    
# Add 'type' key
successful_migration_dict["fields"] += ("type",)

# Statistic message for console
statistic_msg: str = """    
    A total of {} unique activity flows were detected that need to be linked:
        - {} unique activity flows were successfully linked
        - {} unique activity flows remain unlinked
    """.format(len(successful_migration_data) + len(unsuccessful_migration_data), len(successful_migration_data), len(unsuccessful_migration_data))
print(statistic_msg)
    
# Return
return {"activity_migration": successful_migration_dict,
        "successfully_migrated_activity_flows": pd.DataFrame(successful_migration_df),
        "unsuccessfully_migrated_activity_flows": pd.DataFrame(unsuccessful_migration_df)
        }


