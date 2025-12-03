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
from  datetime import datetime
from functools import partial
from lcia import (import_SimaPro_LCIA_methods,
                  register_biosphere,
                  register_SimaPro_LCIA_methods,
                  write_biosphere_flows_and_method_names_to_XLSX)

from lci import (import_SimaPro_LCI_inventories,
                 migrate_from_excel_file,
                 select_inventory_using_regex)

from utils import (change_brightway_project_directory,
                   change_database_name)



#%% File- and folderpaths, key variables
start_time = datetime.now()

# LCI and LCIA data
LCI_ecoinvent_simapro_folderpath: pathlib.Path = here.parent / "data" / "lci" / "ECO_fromSimaPro"
LCI_agribalyse_simapro_folderpath: pathlib.Path = here.parent / "data" / "lci" / "AGB_fromSimaPro"
LCI_agrifootprint_simapro_folderpath: pathlib.Path = here.parent / "data" / "lci" / "AGF_fromSimaPro"
LCI_wfldb_simapro_folderpath: pathlib.Path = here.parent / "data" / "lci" / "WFLDB_fromSimaPro"
LCI_salca_simapro_folderpath: pathlib.Path = here.parent / "data" / "lci" / "SALCA_fromSimaPro"
LCI_fooddk_simapro_folderpath: pathlib.Path = here.parent / "data" / "lci" / "FoodDK_fromSimaPro"
LCIA_SimaPro_CSV_folderpath: pathlib.Path = here.parent / "data" / "lcia" / "fromSimaPro"

# Generic and Brightway
if bw2data.__version__[0] >= 4:
    project_path: pathlib.Path = here  / "Brightway2.5_projects"
else:
    project_path: pathlib.Path = here / "Brightway2_projects"
    
project_path.mkdir(exist_ok = True)

print("\nWrite the name of your project here and press Enter:")
print("my project name = ")

# Command to type in your desired project name in the console
project_name: str = input() # "LCI and LCIA from SimaPro"
print("\n")

#%% Change brightway project directory and setup project
change_brightway_project_directory(project_path)

# If project already exists, raise error
if project_name in bw2data.projects:
    raise ValueError("Project '{}' already exists and does not need any setup.".format(project_name))
    # bw2data.projects.delete_project(project_name)

# Set project
bw2data.projects.set_current(project_name)

#%% File- and folderpaths
# Setup output path
output_path: pathlib.Path = pathlib.Path(bw2data.projects.output_dir)

#%% Defaults for key variables
biosphere_db_name_simapro: str = "biosphere3 - from SimaPro"
unlinked_biosphere_db_name: str = biosphere_db_name_simapro + " - unlinked"
ecoinvent_db_name_simapro: str = "ecoinvent v3.11 Cut-off - SimaPro"
agribalyse_db_name_simapro: str = "Agribalyse v3.2 - SimaPro (background ecoinvent v3.9.1)"
agrifootprint_db_name_simapro: str = "AgriFootprint v6.3 - SimaPro (background ecoinvent v3.8)"
wfldb_db_name_simapro: str = "World Food LCA Database v3.5 - SimaPro (background ecoinvent v3.5)"
salca_db_name_simapro: str = "SALCA Database v3.11 (background ecoinvent v3.11)"
fooddk_db_name_simapro: str = "LCA Food DK"

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

write_biosphere_flows_and_method_names_to_XLSX(biosphere_db_name = biosphere_db_name_simapro,
                                               output_path = output_path,
                                               verbose = True)

#%% Create JSON files containing biosphere flow data

# Create the biosphere from registered biosphere database (here from SimaPro)
biosphere_flows_from_SimaPro: list[dict] = [dict(m) for m in bw2data.Database(biosphere_db_name_simapro)]

# Create the JSON object to be written
biosphere_flows_from_SimaPro_json: dict = json.dumps({idx: dict(m) for idx, m in enumerate(biosphere_flows_from_SimaPro)}, indent = 4)

# Write the unlinked biosphere data dictionary to a JSON file
with open(output_path / ("biosphere_flows_from_SimaPro.json"), "w") as outfile:
    outfile.write(biosphere_flows_from_SimaPro_json)


#%% Import LCA Food DK database from SimaPro
fooddk_db_simapro: bw2io.importers.base_lci.LCIImporter = import_SimaPro_LCI_inventories(SimaPro_CSV_LCI_filepaths = [LCI_fooddk_simapro_folderpath / "FoodDK.CSV"],
                                                                                         db_name = fooddk_db_name_simapro,
                                                                                         encoding = "latin-1",
                                                                                         delimiter = "\t",
                                                                                         verbose = True)

fooddk_db_simapro.apply_strategy(partial(link.link_biosphere_flows_externally,
                                         biosphere_db_name = biosphere_db_name_simapro,
                                         biosphere_db_name_unlinked = unlinked_biosphere_db_name,
                                         other_biosphere_databases = None,
                                         linking_order = None,
                                         relink = False,
                                         strip = True,
                                         case_insensitive = True,
                                         remove_special_characters = False,
                                         verbose = True), verbose = True)

fooddk_db_simapro.apply_strategy(partial(migrate_from_excel_file,
                                         excel_migration_filepath = LCI_fooddk_simapro_folderpath / "custom_migration_FoodDK.xlsx",
                                         migrate_activities = False,
                                         migrate_exchanges = True),
                                 verbose = True)

fooddk_db_simapro.apply_strategy(partial(link.link_activities_internally,
                                         production_exchanges = True,
                                         substitution_exchanges = True,
                                         technosphere_exchanges = True,
                                         relink = False,
                                         strip = True,
                                         case_insensitive = True,
                                         remove_special_characters = False,
                                         verbose = True), verbose = True)

print("\n------- Statistics")
fooddk_db_simapro.statistics()

# Make a new biosphere database for the flows which are currently not linked
# Add unlinked biosphere flows with a custom function
unlinked_biosphere_flows: dict = utils.add_unlinked_flows_to_biosphere_database(db = fooddk_db_simapro,
                                                                                biosphere_db_name_unlinked = unlinked_biosphere_db_name,
                                                                                biosphere_db_name = biosphere_db_name_simapro,
                                                                                add_to_existing_database = True,
                                                                                verbose = True)
print("\n------- Statistics")
fooddk_db_simapro.statistics()

# Delete lca food dk database if already existing
if fooddk_db_name_simapro in bw2data.databases:
    print("\n------- Delete database: " + fooddk_db_name_simapro)
    del bw2data.databases[fooddk_db_name_simapro]

# Write database
print("\n------- Write database: " + wfldb_db_name_simapro)
fooddk_db_simapro.write_database()

# Free up memory
del fooddk_db_simapro

#%% Import WFLDB LCI database from SimaPro
wfldb_db_simapro: bw2io.importers.base_lci.LCIImporter = import_SimaPro_LCI_inventories(SimaPro_CSV_LCI_filepaths = [LCI_wfldb_simapro_folderpath / "WFLDB.CSV"],
                                                                                                db_name = wfldb_db_name_simapro,
                                                                                                encoding = "latin-1",
                                                                                                delimiter = "\t",
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

# Free up memory
del wfldb_db_simapro

#%% Import ecoinvent LCI database from SimaPro
ecoinvent_db_simapro: bw2io.importers.base_lci.LCIImporter = import_SimaPro_LCI_inventories(SimaPro_CSV_LCI_filepaths = [LCI_ecoinvent_simapro_folderpath / "ECO.CSV"],
                                                                                            db_name = ecoinvent_db_name_simapro,
                                                                                            encoding = "latin-1",
                                                                                            delimiter = "\t",
                                                                                            verbose = True)

ecoinvent_db_simapro.apply_strategy(partial(migrate_from_excel_file,
                                            excel_migration_filepath = LCI_ecoinvent_simapro_folderpath / "custom_migration_ECO.xlsx",
                                            migrate_activities = False,
                                            migrate_exchanges = True),
                                    verbose = True)

# Remove linking
ecoinvent_db_simapro.apply_strategy(partial(link.remove_linking,
                                            production_exchanges = True,
                                            substitution_exchanges = True,
                                            technosphere_exchanges = True,
                                            biosphere_exchanges = True))

# Make a deepcopy to use for importing SALCA inventories
salca_db_simapro: bw2io.importers.base_lci.LCIImporter = bw2io.importers.base_lci.LCIImporter(salca_db_name_simapro)
salca_db_simapro.data: list[dict] = copy.deepcopy(ecoinvent_db_simapro.data)

# Specific patterns that are used to identify the SALCA inventories
SALCA_patterns_to_exclude: list[str] = [
                          "SALCA", # abbreviation to identify SALCA inventories
                          "SLACA", # WOW... I mean come on...
                          "at plant/CH mix", # some CH mixes that were created without the SALCA abbreviation
                          "WFLDB", # because why not finding WFLDB inventories in SALCA/ecoinvent?
                          "maize silage, conservation, sect.", # This inventory does not contain the SALCA abbreviation in the SimaPro name but we still have to exclude it.
                          "maize silage, horiz. silo, IP, conservation, sect", # This inventory does not contain the SALCA abbreviation in the SimaPro name but we still have to exclude it.
                          "maize silage, tow. silo, IP, conservation, sect", # This inventory does not contain the SALCA abbreviation in the SimaPro name but we still have to exclude it.
                          "EI3AS", # specific inventories from ecoinvent that were adapted and do not really belong to ecoinvent
                          # "Phosphate rock, as P2O5, beneficiated, dry", # Well...
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
del unlinked_biosphere_flows, ecoinvent_db_simapro

#%% Import SALCA LCI database from SimaPro

# Specific patterns that are used to identify the SALCA inventories
SALCA_patterns_to_include: list[str] = [
                          "SALCA", # abbreviation to identify SALCA inventories
                          "SLACA", # WOW... I mean come on...
                          # "WFLDB", # because why not finding WFLDB inventories in SALCA/ecoinvent?
                          "at plant/CH mix", # some CH mixes that were created without the SALCA abbreviation
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

salca_db_simapro.apply_strategy(partial(migrate_from_excel_file,
                                        excel_migration_filepath = LCI_salca_simapro_folderpath / "custom_migration_SALCA.xlsx",
                                        migrate_activities = False,
                                        migrate_exchanges = True),
                                verbose = True)

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

salca_db_simapro.apply_strategy(partial(link.remove_linking,
                                        production_exchanges = True,
                                        substitution_exchanges = True,
                                        technosphere_exchanges = True,
                                        biosphere_exchanges = False))

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
                                        link_to_databases = (wfldb_db_name_simapro, ecoinvent_db_name_simapro),
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

# Free up memory
del salca_db_simapro, unlinked_biosphere_flows


#%% Import Agribalyse LCI database from SimaPro
agribalyse_db_simapro: bw2io.importers.base_lci.LCIImporter = import_SimaPro_LCI_inventories(SimaPro_CSV_LCI_filepaths = [LCI_agribalyse_simapro_folderpath / "AGB.CSV"],
                                                                                             db_name = agribalyse_db_name_simapro,
                                                                                             encoding = "latin-1",
                                                                                             delimiter = "\t",
                                                                                             verbose = True)

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

# Free up memory
del agribalyse_db_simapro

#%% Import AgriFootprint LCI database from SimaPro
agrifootprint_db_simapro: bw2io.importers.base_lci.LCIImporter = import_SimaPro_LCI_inventories(SimaPro_CSV_LCI_filepaths = [LCI_agrifootprint_simapro_folderpath / "AGF.CSV"],
                                                                                                db_name = agrifootprint_db_name_simapro,
                                                                                                encoding = "latin-1",
                                                                                                delimiter = "\t",
                                                                                                verbose = True)

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

# Free up memory
del agrifootprint_db_simapro

#%%
print('Project created')
end_time = datetime.now()

print('Time elapsed' , str(round((end_time - start_time).seconds/60)), 'minutes')


#%% Extract metadata to excel
# import pandas as pd
# data: list[dict] = []

# # Loop through each database in the background
# for database_name in bw2data.databases:
#     print(database_name)
    
#     # Go on if it contains 'biosphere' in the name, since we are not interested in extracting biosphere flows
#     if "biosphere" in database_name:
#         continue
    
#     # Extract data and add to list
#     data += utils.extract_activity_list_with_metadata(database_name)
    
# # Convert to dataframe and write
# data_df: pd.DataFrame = pd.DataFrame(data)
# data_df.to_excel(output_path / "activity_metadata.xlsx", index = False)

# import pandas as pd
# from calculation import LCA_Calculation
# methods = [m for m in bw2data.methods if "SALCA" in m[0]]
# LCA_results: list[dict] = []

# for database_name in bw2data.databases:
#     print(database_name)
    
#     # Go on if it contains 'biosphere' in the name, since we will not calculate biosphere flows
#     if "biosphere" in database_name:
#         continue
    
#     activities: list = list(bw2data.Database(database_name))
#     lca_calculation: LCA_Calculation = LCA_Calculation(activities = activities, methods = methods)
#     lca_calculation.calculate(calculate_LCIA_scores = True)
#     LCIA_scores: list[dict] = lca_calculation.get_results(extended = True)["LCIA_scores"]
#     LCA_results += LCIA_scores
    
# LCA_results_df: pd.DataFrame = pd.DataFrame(LCA_results)
# LCA_results_df.to_excel(output_path / "activity_impacts.xlsx", index = False)

# #%% Example of how to run an LCA calculation

# # Methods to use for the LCA calculation
# simapro_EF_LCIA_name: str = "Environmental Footprint v3.1"
# simapro_methods: list[tuple[str]] = [m for m in bw2data.methods if m[0] == simapro_EF_LCIA_name]
# methods_all: list[tuple] = list(bw2data.methods)

# # Check if all specified methods are registered in the Brightway background
# for method in simapro_methods:
#     error: bool = False
#     if method not in bw2data.methods:
#         error: bool = True
#         print("Method not registered: '{}'".format(method))

# # If unregistered methods have been detected, raise error
# if error:
#     raise ValueError("Unregistered methods detected.")


# #%% LCA calculation

# # Extract all inventories
# # ... from ecoinvent v3.10 (SimaPro)
# ecoinvent_simapro_inventories: list = [m for m in bw2data.Database(ecoinvent_db_name_simapro)]

# # Run LCA calculation
# lca_calculation_ecoinvent: LCA_Calculation = LCA_Calculation(activities = ecoinvent_simapro_inventories,
#                                                              methods = simapro_methods,
#                                                              functional_amount = 1,
#                                                              cut_off_percentage = 0.001,
#                                                              exchange_level = 1,
#                                                              print_progress_bar = True)
# lca_calculation_ecoinvent.calculate_LCIA_scores()
# lca_calculation_ecoinvent.write_results(path = output_path,
#                                         filename = "ecoinvent_SimaPro",
#                                         use_timestamp_in_filename = False,
#                                         clean = True)

# # Extract all inventories
# # ... from Agribalyse v3.1 (SimaPro) with ecoinvent v3.8 background
# agribalyse_simapro_inventories: list = [m for m in bw2data.Database(agribalyse_db_name_simapro)]

# # Run LCA calculation
# lca_calculation_agribalyse: LCA_Calculation = LCA_Calculation(activities = agribalyse_simapro_inventories,
#                                                              methods = simapro_methods,
#                                                              functional_amount = 1,
#                                                              cut_off_percentage = 0.001,
#                                                              exchange_level = 1,
#                                                              print_progress_bar = True)
# lca_calculation_agribalyse.calculate_LCIA_scores()
# lca_calculation_agribalyse.write_results(path = output_path,
#                                          filename = "agribalyse_SimaPro",
#                                          use_timestamp_in_filename = False,
#                                          clean = True)

# # Extract all inventories
# # ... from SALCA v3.10 (SimaPro) with ecoinvent v3.10 background
# salca_simapro_inventories: list = [m for m in bw2data.Database(salca_db_name_simapro)]

# # Run LCA calculation
# lca_calculation_salca: LCA_Calculation = LCA_Calculation(activities = salca_simapro_inventories,
#                                                          methods = simapro_methods,
#                                                          functional_amount = 1,
#                                                          cut_off_percentage = 0.001,
#                                                          exchange_level = 1,
#                                                          print_progress_bar = True)
# lca_calculation_salca.calculate_LCIA_scores()
# lca_calculation_salca.write_results(path = output_path,
#                                     filename = "salca_SimaPro",
#                                     use_timestamp_in_filename = False,
#                                     clean = True)

# # Extract all inventories
# # ... from Agrifootprint v6.3 (SimaPro) with ecoinvent v3.8 background
# agrifootprint_simapro_inventories: list = [m for m in bw2data.Database(agrifootprint_db_name_simapro)]

# # Run LCA calculation
# lca_calculation_agrifootprint: LCA_Calculation = LCA_Calculation(activities = agrifootprint_simapro_inventories,
#                                                                  methods = simapro_methods,
#                                                                  functional_amount = 1,
#                                                                  cut_off_percentage = 0.001,
#                                                                  exchange_level = 1,
#                                                                  print_progress_bar = True)
# lca_calculation_agrifootprint.calculate_LCIA_scores()
# lca_calculation_agrifootprint.write_results(path = output_path,
#                                             filename = "agrifootprint_SimaPro",
#                                             use_timestamp_in_filename = False,
#                                             clean = True)

# # Extract all inventories
# # ... from WFLDB v3.5 (SimaPro) with ecoinvent v3.5 background
# wfldb_simapro_inventories: list = [m for m in bw2data.Database(wfldb_db_name_simapro)]

# # Run LCA calculation
# lca_calculation_wfldb: LCA_Calculation = LCA_Calculation(activities = wfldb_simapro_inventories,
#                                                          methods = simapro_methods,
#                                                          functional_amount = 1,
#                                                          cut_off_percentage = 0.001,
#                                                          exchange_level = 1,
#                                                          print_progress_bar = True)
# lca_calculation_wfldb.calculate_LCIA_scores()
# lca_calculation_wfldb.write_results(path = output_path,
#                                     filename = "wfldb_SimaPro",
#                                     use_timestamp_in_filename = False,
#                                     clean = True)


# #%% Export inventories to SimaPro CSV
# SimaPro_CSV_text_block: str = export_SimaPro_CSV(list_of_Brightway2_pewee_objects = ecoinvent_simapro_inventories[10:20],
#                                                  folder_path_SimaPro_CSV = output_path,
#                                                  file_name_SimaPro_CSV_without_ending = "10_exported_SimaPro_inventories",
#                                                  file_name_print_timestamp = True,
#                                                  separator = "\t",
#                                                  avoid_exporting_inventories_twice = True,
#                                                  csv_format_version = "7.0.0",
#                                                  decimal_separator = ".",
#                                                  date_separator = ".",
#                                                  short_date_format = "dd.MM.yyyy")


