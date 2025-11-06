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
                 create_XML_biosphere_from_elmentary_exchanges_file,
                 create_XML_biosphere_from_LCI,
                 select_inventory_using_regex)

from harmonization import (create_harmonized_biosphere_migration,
                           create_harmonized_activity_migration,
                           elementary_flows_that_are_not_used_in_XML_methods)

from correspondence.correspondence import (create_correspondence_mapping)

from utils import (change_brightway_project_directory,
                   change_database_name)
from calculation import run_LCA


#%% File- and folderpaths, key variables

# LCI and LCIA data
LCI_ecoinvent_simapro_folderpath: pathlib.Path = here.parent / "data" / "lci" / "ECO_fromSimaPro"
LCI_ecoinvent_xml_folderpath: pathlib.Path = here.parent / "data" / "lci" / "ECO_fromXML"
LCI_ecoinvent_xml_data_folderpath: pathlib.Path = LCI_ecoinvent_xml_folderpath / "ecoinvent 3.10_cutoff_ecoSpold02"
LCI_ecoinvent_xml_datasets_folderpath: pathlib.Path = LCI_ecoinvent_xml_data_folderpath / "datasets"
LCI_agribalyse_simapro_folderpath: pathlib.Path = here.parent / "data" / "lci" / "AGB_fromSimaPro"
LCI_agrifootprint_simapro_folderpath: pathlib.Path = here.parent / "data" / "lci" / "AGF_fromSimaPro"
LCI_wfldb_simapro_folderpath: pathlib.Path = here.parent / "data" / "lci" / "WFLDB_fromSimaPro"
LCI_salca_simapro_folderpath: pathlib.Path = here.parent / "data" / "lci" / "SALCA_fromSimaPro"
LCIA_SimaPro_CSV_folderpath: pathlib.Path = here.parent / "data" / "lcia" / "fromSimaPro"
LCIA_XML_folderpath: pathlib.Path = here.parent / "data" / "lcia" / "fromXML" / "ecoinvent 3.10_LCIA_implementation"
LCIA_XML_filename: str = "LCIA Implementation 3.10.xlsx"
LCIA_XML_filepath: pathlib.Path = LCIA_XML_folderpath / LCIA_XML_filename
LCIA_XML_sheetname: str = "CFs"
LCIA_XML_elementary_exchanges_filename: str = "ElementaryExchanges.xml"
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

ecoinvent_db_name_simapro: str = "ecoinvent v3.10 - SimaPro - regionalized"
ecoinvent_db_name_xml: str = "ecoinvent v3.10 - XML - unregionalized"
ecoinvent_db_name_xml_migrated: str = ecoinvent_db_name_xml + " (migrated to SimaPro biosphere)"

agribalyse_db_name_simapro: str = "Agribalyse v3.1 - SimaPro - unregionalized (background ecoinvent v3.8)"
agribalyse_db_name_updated_simapro: str = "Agribalyse v3.1 - SimaPro  - unregionalized (XML background ecoinvent v3.10)"

agrifootprint_db_name_simapro: str = "AgriFootprint v6.3 - SimaPro - unregionalized (background ecoinvent v3.8)"
agrifootprint_db_name_updated_simapro: str = "AgriFootprint v6.3 - SimaPro - unregionalized (XML background ecoinvent v3.10)"

wfldb_db_name_simapro: str = "World Food LCA Database v3.5 - SimaPro - unregionalized (background ecoinvent v3.5)"
wfldb_db_name_updated_simapro: str = "World Food LCA Database v3.10 - SimaPro - unregionalized (XML background ecoinvent v3.10)"

salca_db_name_simapro: str = "SALCA Database v3.10 - SimaPro - unregionalized (background ecoinvent v3.10)"
salca_db_name_updated_simapro: str = "SALCA Database v3.10 - SimaPro - unregionalized (XML background ecoinvent v3.10)"

#%% Import SimaPro LCIA methods and create SimaPro biosphere database
methods: list[dict] = import_SimaPro_LCIA_methods(path_to_SimaPro_CSV_LCIA_files = LCIA_SimaPro_CSV_folderpath,
                                                  encoding = "latin-1",
                                                  delimiter = "\t",
                                                  verbose = True)

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

#%% Import WFLDB LCI database from SimaPro
wfldb_db_simapro: bw2io.importers.base_lci.LCIImporter = import_SimaPro_LCI_inventories(SimaPro_CSV_LCI_filepaths = [LCI_wfldb_simapro_folderpath / "WFLDB.CSV"],
                                                                                        db_name = wfldb_db_name_simapro,
                                                                                        encoding = "latin-1",
                                                                                        delimiter = "\t",
                                                                                        verbose = True)
wfldb_db_simapro.apply_strategy(unregionalize_biosphere)

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

wfldb_db_simapro.apply_strategy(partial(migrate_from_excel_file,
                                        excel_migration_filepath = LCI_wfldb_simapro_folderpath / "custom_migration_WFLDB.xlsx",
                                        migrate_activities = False,
                                        migrate_exchanges = True),
                                verbose = True)

wfldb_db_simapro.apply_strategy(partial(link.link_activities_internally,
                                        production_exchanges = True,
                                        substitution_exchanges = True,
                                        technosphere_exchanges = True,
                                        relink = False,
                                        strip = True,
                                        case_insensitive = True,
                                        remove_special_characters = False,
                                        verbose = True), verbose = True)

print("\n------- Statistics")
wfldb_db_simapro.statistics()

# Make a new biosphere database for the flows which are currently not linked
# Add unlinked biosphere flows with a custom function
unlinked_biosphere_flows: dict = utils.add_unlinked_flows_to_biosphere_database(db = wfldb_db_simapro,
                                                                                biosphere_db_name_unlinked = unlinked_biosphere_db_name,
                                                                                biosphere_db_name = biosphere_db_name_simapro,
                                                                                add_to_existing_database = True,
                                                                                verbose = True)
print("\n------- Statistics")
wfldb_db_simapro.statistics()

# Delete world food lca database if already existing
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
del wfldb_db_simapro, unlinked_biosphere_flows


#%% Import ecoinvent LCI database from SimaPro
ecoinvent_db_simapro: bw2io.importers.base_lci.LCIImporter = import_SimaPro_LCI_inventories(SimaPro_CSV_LCI_filepaths = [LCI_ecoinvent_simapro_folderpath / "ECO.CSV"],
                                                                                            db_name = ecoinvent_db_name_simapro,
                                                                                            encoding = "latin-1",
                                                                                            delimiter = "\t",
                                                                                            verbose = True)

# Unregionalized the ecoinvent Database
ecoinvent_db_simapro.apply_strategy(unregionalize_biosphere)

ecoinvent_db_simapro.apply_strategy(partial(migrate_from_excel_file,
                                            excel_migration_filepath = LCI_ecoinvent_simapro_folderpath / "custom_migration_ECO.xlsx",
                                            migrate_activities = False,
                                            migrate_exchanges = True),
                                    verbose = True)

# Make a deepcopy to use for importing SALCA inventories
salca_db_simapro: bw2io.importers.base_lci.LCIImporter = bw2io.importers.base_lci.LCIImporter(salca_db_name_simapro)
salca_db_simapro.data: list[dict] = copy.deepcopy(ecoinvent_db_simapro.data)

# Specific patterns that are used to identify the SALCA inventories
SALCA_patterns_to_exclude: list[str] = [
                          "SALCA", # abbreviation to identify SALCA inventories
                          "SLACA", # WOW... I mean come on...
                          "WFLDB", # because why not finding WFLDB inventories in SALCA/ecoinvent?
                          "maize silage, conservation, sect.", # This inventory does not contain the SALCA abbreviation in the SimaPro name but we still have to exclude it.
                          "maize silage, horiz. silo, IP, conservation, sect", # This inventory does not contain the SALCA abbreviation in the SimaPro name but we still have to exclude it.
                          "maize silage, tow. silo, IP, conservation, sect", # This inventory does not contain the SALCA abbreviation in the SimaPro name but we still have to exclude it.
                          "EI3AS", # specific inventories from ecoinvent that were adapted and do not really belong to ecoinvent
                          "Phosphate rock, as P2O5, beneficiated, dry", # Well...
                          ]

ecoinvent_db_simapro.apply_strategy(partial(select_inventory_using_regex,
                                            exclude = True,
                                            include = False,
                                            patterns = SALCA_patterns_to_exclude,
                                            case_sensitive = True))

# Link flows
ecoinvent_db_simapro.apply_strategy(partial(link.link_biosphere_flows_externally,
                                            biosphere_db_name = biosphere_db_name_simapro,
                                            biosphere_db_name_unlinked = unlinked_biosphere_db_name,
                                            other_biosphere_databases = None,
                                            linking_order = None,
                                            relink = False,
                                            strip = True,
                                            case_insensitive = True,
                                            remove_special_characters = False,
                                            verbose = True), verbose = True)

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

# Make a new biosphere database for the flows which are currently not linked
# Add unlinked biosphere flows with a custom function
unlinked_biosphere_flows: dict = utils.add_unlinked_flows_to_biosphere_database(db = ecoinvent_db_simapro,
                                                                                biosphere_db_name_unlinked = unlinked_biosphere_db_name,
                                                                                biosphere_db_name = biosphere_db_name_simapro,
                                                                                add_to_existing_database = True,
                                                                                verbose = True)
print("\n------- Statistics")
ecoinvent_db_simapro.statistics()

# Delete ecoinvent database if already existing
if ecoinvent_db_name_simapro in bw2data.databases:
    print("\n------- Delete database: " + ecoinvent_db_name_simapro)
    del bw2data.databases[ecoinvent_db_name_simapro]

# Write database
print("\n------- Write database: " + ecoinvent_db_name_simapro)
ecoinvent_db_simapro.write_database()

# Free up memory
del unlinked_biosphere_flows


#%% Import SALCA LCI database from SimaPro

# Specific patterns that are used to identify the SALCA inventories
SALCA_patterns_to_include: list[str] = [
                          "SALCA", # abbreviation to identify SALCA inventories
                          "SLACA", # WOW... I mean come on...
                          # "WFLDB", # because why not finding WFLDB inventories in SALCA/ecoinvent?
                          "maize silage, conservation, sect.", # This inventory does not contain the SALCA abbreviation in the SimaPro name but we still have to include it.
                          "maize silage, horiz. silo, IP, conservation, sect", # This inventory does not contain the SALCA abbreviation in the SimaPro name but we still have to include it.
                          "maize silage, tow. silo, IP, conservation, sect", # This inventory does not contain the SALCA abbreviation in the SimaPro name but we still have to include it.
                          "EI3AS", # specific inventories from ecoinvent that were adapted and do not really belong to ecoinvent
                          "Phosphate rock, as P2O5, beneficiated, dry", # Well...
                          ]

salca_db_simapro.apply_strategy(partial(select_inventory_using_regex,
                                        exclude = False,
                                        include = True,
                                        patterns = SALCA_patterns_to_include,
                                        case_sensitive = True))

# Rename the database of the activities and the production exchanges
salca_db_simapro.apply_strategy(partial(change_database_name,
                                        new_db_name = salca_db_name_simapro,
                                        ))

# # !!!
# see_salca = [m for m in salca_db_simapro if m["name"] == "Phosphate rock, as P2O5, beneficiated, dry {{COUNTRY}}| phosphate rock beneficiation, dry | Cut-off, U"]
# see_eco = [m for m in ecoinvent_db_simapro if m["name"] == "Phosphate rock, as P2O5, beneficiated, dry {{COUNTRY}}| phosphate rock beneficiation, dry | Cut-off, U"]
# # !!!

salca_db_simapro.apply_strategy(partial(migrate_from_excel_file,
                                        excel_migration_filepath = LCI_salca_simapro_folderpath / "custom_migration_SALCA.xlsx",
                                        migrate_activities = False,
                                        migrate_exchanges = True),
                                verbose = True)

# Remove linking
salca_db_simapro.apply_strategy(partial(link.remove_linking,
                                        production_exchanges = True,
                                        substitution_exchanges = True,
                                        technosphere_exchanges = True,
                                        biosphere_exchanges = True))

# Link flows
salca_db_simapro.apply_strategy(partial(link.link_biosphere_flows_externally,
                                        biosphere_db_name = biosphere_db_name_simapro,
                                        biosphere_db_name_unlinked = unlinked_biosphere_db_name,
                                        other_biosphere_databases = None,
                                        linking_order = None,
                                        relink = False,
                                        strip = True,
                                        case_insensitive = True,
                                        remove_special_characters = False,
                                        verbose = True), verbose = True)

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
                                                link_to_databases = (wfldb_db_name_simapro,),
                                                link_production_exchanges = False,
                                                link_substitution_exchanges = False,
                                                link_technosphere_exchanges = True,
                                                relink = False,
                                                strip = True,
                                                case_insensitive = True,
                                                remove_special_characters = False,
                                                verbose = True), verbose = True)

salca_n_datasets, salca_n_exchanges, salca_n_unlinked = salca_db_simapro.statistics()
exchanges_that_are_salca_inventories: list[dict] = [exc for m in salca_db_simapro for exc in m["exchanges"] if "input" not in exc and exc["type"] in ["technosphere", "substitution"] and not exc.get("is_ecoinvent", False)]

salca_inventories_to_be_added: dict = {}
ecoinvent_db_from_background = {(m["name"], m["location"], m["unit"]): m for m in bw2data.Database(ecoinvent_db_name_simapro)}

for exc in exchanges_that_are_salca_inventories:
    
    ID: tuple[str, str, str] = (exc["name"], exc["location"], exc["unit"])
    
    if ID in salca_inventories_to_be_added:
        continue
    
    act: dict = {**ecoinvent_db_from_background[ID].as_dict(), **{"exchanges": [m.as_dict() for m in ecoinvent_db_from_background[ID].exchanges()]}}
    salca_inventories_to_be_added[ID]: dict = act

salca_db_simapro.data += list(salca_inventories_to_be_added.values())

# The new activities imported from ecoinvent need to be renamed
salca_db_simapro.apply_strategy(partial(change_database_name,
                                        new_db_name = salca_db_name_simapro,
                                        ))

salca_db_simapro.apply_strategy(partial(link.link_activities_externally,
                                        link_to_databases = (ecoinvent_db_name_simapro,),
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

# Make a new biosphere database for the flows which are currently not linked
# Add unlinked biosphere flows with a custom function
unlinked_biosphere_flows: dict = utils.add_unlinked_flows_to_biosphere_database(db = salca_db_simapro,
                                                                                biosphere_db_name_unlinked = unlinked_biosphere_db_name,
                                                                                biosphere_db_name = biosphere_db_name_simapro,
                                                                                add_to_existing_database = True,
                                                                                verbose = True)
print("\n------- Statistics")
salca_db_simapro.statistics()

# Delete salca database if already existing
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
del salca_db_simapro, unlinked_biosphere_flows


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
ecoinvent_db_xml: bw2io.importers.ecospold2.SingleOutputEcospold2Importer = import_XML_LCI_inventories(XML_LCI_filepath = LCI_ecoinvent_xml_datasets_folderpath,
                                                                                                       db_name = ecoinvent_db_name_xml,
                                                                                                       biosphere_db_name = biosphere_db_name_xml,
                                                                                                       db_model_type_name = "cutoff",
                                                                                                       db_process_type_name = "unit",
                                                                                                       verbose = True)

print("\n-----------Linking statistics of current database import")
ecoinvent_db_xml.statistics()
print()

# Create the biosphere from XML file containing all elementary exchanges
biosphere_flows_from_xml_elementary_exchanges: list[dict] = create_XML_biosphere_from_elmentary_exchanges_file(filepath_ElementaryExchanges = LCIA_XML_elementary_exchanges_filepath,
                                                                                                                biosphere_db_name = biosphere_db_name_xml)

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
xml_lcia_methods: list[dict] = import_XML_LCIA_methods(XML_LCIA_filepath = LCIA_XML_filepath,
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
def replace_XML_code_field_with_SimaPro_code(XML_db):
    
    # Create a mapping file from activity and reference product codes that are found in the ecoinvent SimaPro database
    mapping: dict = {m["activity_code"] + "_" + m["reference_product_code"]: m["code"] for m in ecoinvent_db_simapro if m["activity_code"] is not None and m["reference_product_code"] is not None}
    
    # Initialize counter
    counter: int = 0
    
    # Loop through each inventory from the XML database
    for ds in XML_db:
        
        # Try to find the corresponding SimaPro code using the mapping created beforehand
        new_code: (str | None) = mapping.get(ds["code"])
        
        # Go to next if no new code was found
        if new_code is None:
            continue
        
        # Update the code field
        ds["code"]: str = new_code
        
        # Increase counter
        counter += 1
        
        # Loop through the exchanges to update the production exchange as well
        for exc in ds["exchanges"]:
            
            # If the current exchange is not of type production, continue
            if exc["type"] != "production":
                continue
            
            # Update code and input fields
            exc["code"]: str = new_code
            exc["input"]: tuple = (ds["database"], new_code)
    
    # Print how many codes were changed
    print("{} from {} code(s) were updated with the respective SimaPro code".format(counter, len(XML_db)))
    
    return XML_db

# Apply strategy and update the code of XML inventories with the respecting code from SimaPro inventories
ecoinvent_db_xml_migrated.apply_strategy(replace_XML_code_field_with_SimaPro_code)

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
correspondence_mapping: list[dict] = create_correspondence_mapping(path_to_correspondence_files = folderpath_correspondence_files,
                                                                   model_type = "cutoff",
                                                                   map_to_version = (3, 10),
                                                                   output_path = output_path)

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
                                                                         manually_checked_SBERTs = manually_checked_SBERT_activity_names,
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

# Write database
print("\n-----------Write database: " + agribalyse_db_name_updated_simapro)
agribalyse_db_updated_simapro.write_database(overwrite = False)
print()

# Free up memory
del agribalyse_db_updated_simapro



#%% Create the WFDLB background activity migration --> all ecoinvent v3.5 found in the background from WFLDB should be updated to ecoinvent v3.10, if possible
wfldb_exchanges_to_migrate_to_ecoinvent: dict = {(exc["name"], exc["unit"], exc["location"]): exc for ds in list(wfldb_db_updated_simapro) for exc in ds["exchanges"] if exc["type"] not in ["production", "biosphere"] and exc.get("is_ecoinvent", False)}

# Load dataframe with manually checked activity flows
manually_checked_SBERT_activity_names: pd.DataFrame = pd.read_excel(LCI_ecoinvent_xml_folderpath / filename_SBERT_activity_names_validated)

WFLDB_background_ei_migration: dict = create_harmonized_activity_migration(flows_1 = list(wfldb_exchanges_to_migrate_to_ecoinvent.values()),
                                                                           flows_2 = list(bw2data.Database(ecoinvent_db_name_xml_migrated)),
                                                                           manually_checked_SBERTs = manually_checked_SBERT_activity_names,
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

# Write database
print("\n-----------Write database: " + wfldb_db_name_updated_simapro)
wfldb_db_updated_simapro.write_database(overwrite = False)
print()

# Free up memory
del wfldb_db_updated_simapro



#%% Create the SALCA background activity migration --> all ecoinvent v3.10 found in the background from SALCA should be updated to ecoinvent v3.10 from XML, if possible
salca_exchanges_to_migrate_to_ecoinvent: dict = {(exc["name"], exc["unit"], exc["location"]): exc for ds in list(salca_db_updated_simapro) for exc in ds["exchanges"] if exc["type"] not in ["production", "biosphere"] and exc.get("is_ecoinvent", False)}

# Load dataframe with manually checked activity flows
manually_checked_SBERT_activity_names: pd.DataFrame = pd.read_excel(LCI_ecoinvent_xml_folderpath / filename_SBERT_activity_names_validated)

SALCA_background_ei_migration: dict = create_harmonized_activity_migration(flows_1 = list(salca_exchanges_to_migrate_to_ecoinvent.values()),
                                                                           flows_2 = list(bw2data.Database(ecoinvent_db_name_xml_migrated)),
                                                                           manually_checked_SBERTs = manually_checked_SBERT_activity_names,
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
    
# Write database
print("\n-----------Write database: " + salca_db_name_updated_simapro)
salca_db_updated_simapro.write_database(overwrite = False)
print()

# Free up memory
del salca_db_updated_simapro



#%% Create the Agrifootprint activity migration --> all ecoinvent v3.8 found in the background from AgriFootprint should be updated to ecoinvent v3.10, if possible
agrifootprint_exchanges_to_migrate_to_ecoinvent: dict = {(exc["name"], exc["unit"], exc["location"]): exc for ds in list(agrifootprint_db_updated_simapro) for exc in ds["exchanges"] if exc["type"] not in ["production", "biosphere"] and exc.get("is_ecoinvent", False)}

# Load dataframe with manually checked activity flows
manually_checked_SBERT_activity_names: pd.DataFrame = pd.read_excel(LCI_ecoinvent_xml_folderpath / filename_SBERT_activity_names_validated)

AGF_background_ei_migration: dict = create_harmonized_activity_migration(flows_1 = list(agrifootprint_exchanges_to_migrate_to_ecoinvent.values()),
                                                                         flows_2 = list(bw2data.Database(ecoinvent_db_name_xml_migrated)),
                                                                         manually_checked_SBERTs = manually_checked_SBERT_activity_names,
                                                                         ecoinvent_correspondence_mapping = correspondence_mapping)

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

# Write database
print("\n-----------Write database: " + agrifootprint_db_name_updated_simapro)
agrifootprint_db_updated_simapro.write_database(overwrite = False)
print()

# Free up memory
del agrifootprint_db_updated_simapro


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
LCA_results_ecoinvent_simapro: dict[str, pd.DataFrame] = run_LCA(activities = ecoinvent_simapro_inventories,
                                                                 methods = simapro_methods,
                                                                 write_LCI_exchanges = False,
                                                                 write_LCI_exchanges_as_emissions = False,
                                                                 write_LCIA_impacts_of_activity_exchanges = False,
                                                                 write_LCIA_process_contribution = False,
                                                                 write_LCIA_emission_contribution = False,
                                                                 write_characterization_factors = False,
                                                                 cutoff_process = 0.001,
                                                                 cutoff_emission = 0.001,
                                                                 write_results_to_file = True,
                                                                 local_output_path = output_path,
                                                                 filename_without_ending = "ecoinvent_SimaPro",
                                                                 use_timestamp_in_filename = True,
                                                                 print_progress_bar = True)

# Run LCA calculation
LCA_results_ecoinvent_xml: dict[str, pd.DataFrame] = run_LCA(activities = ecoinvent_xml_inventories,
                                                             methods = ecoinvent_methods,
                                                             write_LCI_exchanges = False,
                                                             write_LCI_exchanges_as_emissions = False,
                                                             write_LCIA_impacts_of_activity_exchanges = False,
                                                             write_LCIA_process_contribution = False,
                                                             write_LCIA_emission_contribution = False,
                                                             write_characterization_factors = False,
                                                             cutoff_process = 0.001,
                                                             cutoff_emission = 0.001,
                                                             write_results_to_file = True,
                                                             local_output_path = output_path,
                                                             filename_without_ending = "ecoinvent_XML",
                                                             use_timestamp_in_filename = True,
                                                             print_progress_bar = True)

ecoinvent_xml_code_to_simapro_code_mapping: dict = {m["activity_code"] + "_" + m["reference_product_code"]: m["code"] for m in ecoinvent_simapro_inventories if m["activity_code"] is not None and m["reference_product_code"] is not None}

for m in LCA_results_ecoinvent_xml["LCIA_activity_scores"]:
    m["Activity_code"]: str = ecoinvent_xml_code_to_simapro_code_mapping[m["Activity_code"]]

df_1: pd.DataFrame = pd.DataFrame(LCA_results_ecoinvent_simapro["LCIA_activity_scores"] + LCA_results_ecoinvent_xml["LCIA_activity_scores"])
df_1.to_csv(output_path / "comparison_ecoinvent_SimaPro_XML.csv")

LCIA_mapping: dict = {('EF v3.1', 'climate change', 'global warming potential (GWP100)'): "EF v3.1 - Global warming potential (GWP100)",
                       ('Environmental Footprint v3.1', 'Climate change'): "EF v3.1 - Global warming potential (GWP100)",
                       ('EF v3.1', 'land use', 'soil quality index'): "EF v3.1 - Land use",
                       ('Environmental Footprint v3.1', 'Land use'): "EF v3.1 - Land use",
                       ('EF v3.1', 'acidification', 'accumulated exceedance (AE)'): "EF v3.1 - Acidification",
                       ('Environmental Footprint v3.1', 'Acidification'): "EF v3.1 - Acidification",
                       ('EF v3.1', 'eutrophication: freshwater', 'fraction of nutrients reaching freshwater end compartment (P)'): "EF v3.1 - Freshwater eutrophication",
                       ('Environmental Footprint v3.1', 'Eutrophication, freshwater'): "EF v3.1 - Freshwater eutrophication",
                       ('EF v3.1', 'ecotoxicity: freshwater', 'comparative toxic unit for ecosystems (CTUe)'): "EF v3.1 - Freshwater ecotoxicity",
                       ('Environmental Footprint v3.1', 'Ecotoxicity, freshwater - part 1'): "EF v3.1 - Freshwater ecotoxicity",
                       ('Environmental Footprint v3.1', 'Ecotoxicity, freshwater - part 2'): "EF v3.1 - Freshwater ecotoxicity",
                       ('EF v3.1', 'water use', 'user deprivation potential (deprivation-weighted water consumption)'): "EF v3.1 - Water use",
                       ('Environmental Footprint v3.1', 'Water use'): "EF v3.1 - Water use"}

df_1["Method_standardized"] = [LCIA_mapping.get(m, "") for m in list(df_1["Method"])]
df_1.query("Method_standardized != ''").to_excel(output_path / "comparison_ecoinvent_SimaPro_XML_filtered.xlsx")

# Free up memory
del LCA_results_ecoinvent_simapro, LCA_results_ecoinvent_xml


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
LCA_results_agribalyse_simapro: dict[str, pd.DataFrame] = run_LCA(activities = agribalyse_simapro_inventories,
                                                                  methods = simapro_methods,
                                                                  write_LCI_exchanges = False,
                                                                  write_LCI_exchanges_as_emissions = False,
                                                                  write_LCIA_impacts_of_activity_exchanges = False,
                                                                  write_LCIA_process_contribution = False,
                                                                  write_LCIA_emission_contribution = False,
                                                                  write_characterization_factors = False,
                                                                  cutoff_process = 0.001,
                                                                  cutoff_emission = 0.001,
                                                                  write_results_to_file = True,
                                                                  local_output_path = output_path,
                                                                  filename_without_ending = "agribalyse_SimaPro",
                                                                  use_timestamp_in_filename = True,
                                                                  print_progress_bar = True)

# Run LCA calculation
LCA_results_agribalyse_updated_simapro: dict[str, pd.DataFrame] = run_LCA(activities = agribalyse_updated_simapro_inventories,
                                                                          methods = ecoinvent_methods,
                                                                          write_LCI_exchanges = False,
                                                                          write_LCI_exchanges_as_emissions = False,
                                                                          write_LCIA_impacts_of_activity_exchanges = False,
                                                                          write_LCIA_process_contribution = False,
                                                                          write_LCIA_emission_contribution = False,
                                                                          write_characterization_factors = False,
                                                                          cutoff_process = 0.001,
                                                                          cutoff_emission = 0.001,
                                                                          write_results_to_file = True,
                                                                          local_output_path = output_path,
                                                                          filename_without_ending = "agribalyse_updated_SimaPro",
                                                                          use_timestamp_in_filename = True,
                                                                          print_progress_bar = True)

# Run LCA calculation
LCA_results_wfldb_simapro: dict[str, pd.DataFrame] = run_LCA(activities = wfldb_simapro_inventories,
                                                             methods = simapro_methods,
                                                             write_LCI_exchanges = False,
                                                             write_LCI_exchanges_as_emissions = False,
                                                             write_LCIA_impacts_of_activity_exchanges = False,
                                                             write_LCIA_process_contribution = False,
                                                             write_LCIA_emission_contribution = False,
                                                             write_characterization_factors = False,
                                                             cutoff_process = 0.001,
                                                             cutoff_emission = 0.001,
                                                             write_results_to_file = True,
                                                             local_output_path = output_path,
                                                             filename_without_ending = "wfldb_SimaPro",
                                                             use_timestamp_in_filename = True,
                                                             print_progress_bar = True)

# Run LCA calculation
LCA_results_wfldb_updated_simapro: dict[str, pd.DataFrame] = run_LCA(activities = wfldb_updated_simapro_inventories,
                                                                     methods = ecoinvent_methods,
                                                                     write_LCI_exchanges = False,
                                                                     write_LCI_exchanges_as_emissions = False,
                                                                     write_LCIA_impacts_of_activity_exchanges = False,
                                                                     write_LCIA_process_contribution = False,
                                                                     write_LCIA_emission_contribution = False,
                                                                     write_characterization_factors = False,
                                                                     cutoff_process = 0.001,
                                                                     cutoff_emission = 0.001,
                                                                     write_results_to_file = True,
                                                                     local_output_path = output_path,
                                                                     filename_without_ending = "wfldb_updated_SimaPro",
                                                                     use_timestamp_in_filename = True,
                                                                     print_progress_bar = True)

# Run LCA calculation
LCA_results_salca_simapro: dict[str, pd.DataFrame] = run_LCA(activities = salca_simapro_inventories,
                                                             methods = simapro_methods,
                                                             write_LCI_exchanges = False,
                                                             write_LCI_exchanges_as_emissions = False,
                                                             write_LCIA_impacts_of_activity_exchanges = False,
                                                             write_LCIA_process_contribution = False,
                                                             write_LCIA_emission_contribution = False,
                                                             write_characterization_factors = False,
                                                             cutoff_process = 0.001,
                                                             cutoff_emission = 0.001,
                                                             write_results_to_file = True,
                                                             local_output_path = output_path,
                                                             filename_without_ending = "salca_SimaPro",
                                                             use_timestamp_in_filename = True,
                                                             print_progress_bar = True)

# Run LCA calculation
LCA_results_salca_updated_simapro: dict[str, pd.DataFrame] = run_LCA(activities = salca_updated_simapro_inventories,
                                                                     methods = ecoinvent_methods,
                                                                     write_LCI_exchanges = False,
                                                                     write_LCI_exchanges_as_emissions = False,
                                                                     write_LCIA_impacts_of_activity_exchanges = False,
                                                                     write_LCIA_process_contribution = False,
                                                                     write_LCIA_emission_contribution = False,
                                                                     write_characterization_factors = False,
                                                                     cutoff_process = 0.001,
                                                                     cutoff_emission = 0.001,
                                                                     write_results_to_file = True,
                                                                     local_output_path = output_path,
                                                                     filename_without_ending = "salca_updated_SimaPro",
                                                                     use_timestamp_in_filename = True,
                                                                     print_progress_bar = True)

# Run LCA calculation
LCA_results_agrifootprint_simapro: dict[str, pd.DataFrame] = run_LCA(activities = agrifootprint_simapro_inventories,
                                                             methods = simapro_methods,
                                                             write_LCI_exchanges = False,
                                                             write_LCI_exchanges_as_emissions = False,
                                                             write_LCIA_impacts_of_activity_exchanges = False,
                                                             write_LCIA_process_contribution = False,
                                                             write_LCIA_emission_contribution = False,
                                                             write_characterization_factors = False,
                                                             cutoff_process = 0.001,
                                                             cutoff_emission = 0.001,
                                                             write_results_to_file = True,
                                                             local_output_path = output_path,
                                                             filename_without_ending = "agrifootprint_SimaPro",
                                                             use_timestamp_in_filename = True,
                                                             print_progress_bar = True)

# Run LCA calculation
LCA_results_agrifootprint_updated_simapro: dict[str, pd.DataFrame] = run_LCA(activities = agrifootprint_updated_simapro_inventories,
                                                                     methods = ecoinvent_methods,
                                                                     write_LCI_exchanges = False,
                                                                     write_LCI_exchanges_as_emissions = False,
                                                                     write_LCIA_impacts_of_activity_exchanges = False,
                                                                     write_LCIA_process_contribution = False,
                                                                     write_LCIA_emission_contribution = False,
                                                                     write_characterization_factors = False,
                                                                     cutoff_process = 0.001,
                                                                     cutoff_emission = 0.001,
                                                                     write_results_to_file = True,
                                                                     local_output_path = output_path,
                                                                     filename_without_ending = "agrifootprint_updated_SimaPro",
                                                                     use_timestamp_in_filename = True,
                                                                     print_progress_bar = True)

LCA_results_to_join: list[dict] = (LCA_results_agribalyse_simapro["LCIA_activity_scores"] +
                                   LCA_results_agribalyse_updated_simapro["LCIA_activity_scores"] +
                                   LCA_results_wfldb_simapro["LCIA_activity_scores"] +
                                   LCA_results_wfldb_updated_simapro["LCIA_activity_scores"] +
                                   LCA_results_salca_simapro["LCIA_activity_scores"] +
                                   LCA_results_salca_updated_simapro["LCIA_activity_scores"] +
                                   LCA_results_agrifootprint_simapro["LCIA_activity_scores"] +
                                   LCA_results_agrifootprint_updated_simapro["LCIA_activity_scores"]
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




