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
import pandas as pd
from functools import partial
from lcia import (import_SimaPro_LCIA_methods,
                  import_XML_LCIA_methods,
                  register_biosphere,
                  register_SimaPro_LCIA_methods,
                  register_XML_LCIA_methods,
                  add_damage_normalization_weighting,
                  write_biosphere_flows_and_method_names_to_XLSX)

from lci import (unregionalize_biosphere,
                 import_SimaPro_LCI_inventories,
                 import_XML_LCI_inventories,
                 migrate_from_excel_file,
                 migrate_from_json_file,
                 create_XML_biosphere_from_elmentary_exchanges_file,
                 create_XML_biosphere_from_LCI
                 )

from notebook.setup import (biosphere_db_name_simapro,
                            unlinked_biosphere_db_name,
                            ecoinvent_db_name_simapro,
                            agribalyse_db_name_simapro,
                            agrifootprint_db_name_simapro,
                            wfldb_db_name_simapro,
                            salca_db_name_simapro)

from harmonization import (create_harmonized_biosphere_migration,
                           create_harmonized_activity_migration,
                           elementary_flows_that_are_not_used_in_XML_methods)

from correspondence.correspondence import (create_correspondence_mapping)

from utils import (change_brightway_project_directory)
from calculation import run_LCA

from builder import (create_base_inventory,
                     create_base_exchange)

from exporter import export_SimaPro_CSV

#%% File- and folderpaths, key variables

# LCI and LCIA data
LCI_ecoinvent_simapro_folderpath: pathlib.Path = here.parent / "data" / "lci" / "ECO_fromSimaPro"
LCI_ecoinvent_xml_folderpath: pathlib.Path = here.parent / "data" / "lci" / "ECO_fromXML"
LCI_ecoinvent_xml_data_folderpath: pathlib.Path = LCI_ecoinvent_xml_folderpath / "ecoinvent 3.10_cutoff_ecoSpold02"
LCI_ecoinvent_xml_datasets_folderpath: pathlib.Path = LCI_ecoinvent_xml_data_folderpath / "datasets"
LCI_agribalyse_simapro_folderpath: pathlib.Path = here.parent / "data" / "lci" / "AGB_fromSimaPro"
LCIA_SimaPro_CSV_folderpath: pathlib.Path = here.parent / "data" / "lcia" / "fromSimaPro"
LCIA_XML_folderpath: pathlib.Path = here.parent / "data" / "lcia" / "fromXML" / "ecoinvent 3.10_LCIA_implementation"
LCIA_XML_filename: str = "LCIA Implementation 3.10.xlsx"
LCIA_XML_filepath: pathlib.Path = LCIA_XML_folderpath / LCIA_XML_filename
LCIA_XML_sheetname: str = "CFs"
LCIA_XML_elementary_exchanges_filename: str = "ElementaryExchanges.xml"
LCIA_XML_elementary_exchanges_filepath: pathlib.Path = LCI_ecoinvent_xml_data_folderpath / "MasterData" / LCIA_XML_elementary_exchanges_filename

# Generic and Brightway
project_path: pathlib.Path = here / "notebook_data"
project_name: str = "notebook"

# Correspondence files
folderpath_correspondence_files: pathlib.Path = here.parent / "correspondence" / "data"

#%% Change brightway project directory and setup project

# Check if project path exists and raise error if not
if not project_path.exists():
    raise ValueError("Project path not found:\n '{}'".format(str(project_path)))

# Change project path
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
filename_activity_migration_data: str = "activity_migration.json"
filepath_activity_migration_data: pathlib.Path = output_path / filename_activity_migration_data

# Logs
filename_biosphere_flows_not_specified_in_XML_methods: str = "biosphere_flows_not_specified_in_LCIA_methods_from_XML.xlsx"

# Other
filename_SBERT_biosphere_names_validated: str = "manually_checked_SBERT_biosphere_names.xlsx"
filename_SBERT_activity_names_validated: str = "manually_checked_SBERT_activity_names.xlsx"

#%% Defaults for key variables
ecoinvent_db_name_simapro_unreg: str = ecoinvent_db_name_simapro + " - unregionalized"
agribalyse_db_name_simapro_unreg: str = agribalyse_db_name_simapro + " - unregionalized"
agrifootprint_db_name_simapro_unreg: str = agrifootprint_db_name_simapro + " - unregionalized"
wfldb_db_name_simapro_unreg: str = wfldb_db_name_simapro + " - unregionalized"
salca_db_name_simapro_unreg: str = salca_db_name_simapro + " - unregionalized"

biosphere_db_name_xml: str = "biosphere3 - from XML"
ecoinvent_db_name_xml: str = "ecoinvent v3.10 - XML"
ecoinvent_db_name_xml_migrated: str = ecoinvent_db_name_xml + " (migrated to SimaPro biosphere)"
agribalyse_db_name_updated_simapro: str = agribalyse_db_name_simapro + " (background updated to ecoinvent v3.10)" 


#%% Import SimaPro LCIA methods and create SimaPro biosphere database

# !!! Check if methods exist
# !!! Check if biosphere3 from SimaPro exists
# !!! Check if databases exist


#%% Import ecoinvent LCI database from SimaPro
# !!! Import ecoinvent as list of dict, with exchanges

#%% Unregionalize food databases
# !!! Load food databases from background, unregionalize and save again
ecoinvent_db_unreg: bw2io.importers.base_lci.LCIImporter = utils.copy_brightway_database(db_name = ecoinvent_db_name_simapro, new_db_name = ecoinvent_db_name_simapro_unreg)
agribalyse_db_unreg: bw2io.importers.base_lci.LCIImporter = utils.copy_brightway_database(db_name = agribalyse_db_name_simapro, new_db_name = agribalyse_db_name_simapro_unreg)
agrifootprint_db_unreg: bw2io.importers.base_lci.LCIImporter = utils.copy_brightway_database(db_name = agrifootprint_db_name_simapro, new_db_name = agrifootprint_db_name_simapro_unreg)
wfldb_db_unreg: bw2io.importers.base_lci.LCIImporter = utils.copy_brightway_database(db_name = wfldb_db_name_simapro, new_db_name = wfldb_db_name_simapro_unreg)
salca_db_unreg: bw2io.importers.base_lci.LCIImporter = utils.copy_brightway_database(db_name = salca_db_name_simapro, new_db_name = salca_db_name_simapro_unreg)


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

# Delete ecoinvent database if already existing
if agribalyse_db_name_simapro in bw2data.databases:
    print("\n------- Delete database: " + agribalyse_db_name_simapro)
    del bw2data.databases[agribalyse_db_name_simapro]

# Write database
print("\n------- Write database: " + agribalyse_db_name_simapro)
agribalyse_db_simapro.write_database()


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

print("\n-----------Write database: " + ecoinvent_db_name_xml)
ecoinvent_db_xml.write_database(overwrite = False)
print()

# Import ecoinvent LCIA methods from Excel file
xml_lcia_methods: list[dict] = import_XML_LCIA_methods(XML_LCIA_filepath = LCIA_XML_filepath,
                                                       biosphere_db_name = biosphere_db_name_xml,
                                                       ecoinvent_version = None)

print("\n-----------Write methods from ecoinvent Excel")
register_XML_LCIA_methods(methods = xml_lcia_methods)


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
    
# Write database
print("\n-----------Write database: " + ecoinvent_db_name_xml)
ecoinvent_db_xml_migrated.write_database(overwrite = False)
print()



#%% Create correspondence mapping
correspondence_mapping: list[dict] = create_correspondence_mapping(path_to_correspondence_files = folderpath_correspondence_files,
                                                                   model_type = "cutoff",
                                                                   map_to_version = (3, 10),
                                                                   output_path = output_path)

#%% Read Agribalyse again from SimaPro CSV files where we will then update the background
agribalyse_db_updated_simapro: bw2io.importers.base_lci.LCIImporter = import_SimaPro_LCI_inventories(SimaPro_CSV_LCI_filepaths = [LCI_agribalyse_simapro_folderpath / "AGB.CSV"],
                                                                                                     db_name = agribalyse_db_name_updated_simapro,
                                                                                                     encoding = "latin-1",
                                                                                                     delimiter = "\t",
                                                                                                     verbose = True)
agribalyse_db_updated_simapro.apply_strategy(unregionalize_biosphere)

agribalyse_db_updated_simapro.apply_strategy(partial(link.link_biosphere_flows_externally,
                                                     biosphere_db_name = biosphere_db_name_simapro,
                                                     biosphere_db_name_unlinked = unlinked_biosphere_db_name,
                                                     other_biosphere_databases = None,
                                                     linking_order = None,
                                                     relink = False,
                                                     strip = True,
                                                     case_insensitive = True,
                                                     remove_special_characters = False,
                                                     verbose = True), verbose = True)

agribalyse_db_updated_simapro.apply_strategy(partial(migrate_from_excel_file,
                                                     excel_migration_filepath = LCI_agribalyse_simapro_folderpath / "custom_migration_AGB.xlsx",
                                                     migrate_activities = False,
                                                     migrate_exchanges = True),
                                             verbose = True)

agribalyse_db_updated_simapro.apply_strategy(partial(link.remove_linking,
                                                     production_exchanges = False,
                                                     substitution_exchanges = True,
                                                     technosphere_exchanges = True,
                                                     biosphere_exchanges = False), verbose = True)

#%% Create the activity migration --> all ecoinvent v3.8 found in the background from Agribalyse should be updated to ecoinvent v3.10, if possible
agribalyse_exchanges_to_migrate_to_ecoinvent: dict = {(exc["name"], exc["unit"], exc["location"]): exc for ds in list(agribalyse_db_updated_simapro) for exc in ds["exchanges"] if exc["type"] not in ["production", "biosphere"] and exc.get("is_ecoinvent", False)}

# Load dataframe with manually checked activity flows
manually_checked_SBERT_activity_names: pd.DataFrame = pd.read_excel(LCI_ecoinvent_xml_folderpath / filename_SBERT_activity_names_validated)

activity_harmonization: dict = create_harmonized_activity_migration(flows_1 = list(agribalyse_exchanges_to_migrate_to_ecoinvent.values()),
                                                                     flows_2 = list(bw2data.Database(ecoinvent_db_name_xml_migrated)),
                                                                     manually_checked_SBERTs = manually_checked_SBERT_activity_names,
                                                                     ecoinvent_correspondence_mapping = correspondence_mapping)

# SBERT to map for activities
pd.DataFrame(activity_harmonization["successfully_migrated_activity_flows"]).to_excel(output_path / "activity_flows_successfully_migrated.xlsx")
pd.DataFrame(activity_harmonization["unsuccessfully_migrated_activity_flows"]).to_excel(output_path / "activity-flows_unuccessfully_migrated.xlsx")
pd.DataFrame(activity_harmonization["SBERT_to_map"]).to_excel(output_path / "activity_flows_SBERT_to_map.xlsx")

# Create the JSON object to be written
activity_migration_data_in_json_format = json.dumps(activity_harmonization["activity_migration"], indent = 3)

# Write the biosphere dictionary to a JSON file
with open(filepath_activity_migration_data, "w") as outfile:
   outfile.write(activity_migration_data_in_json_format)

#%% Update the ecoinvent background activities in the Agribalyse database from v3.8 to v3.10 and register database
# Apply activity migration
agribalyse_db_updated_simapro.apply_strategy(partial(migrate_from_json_file,
                                                     json_migration_filepath = filepath_activity_migration_data,
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

# Write unlinked biosphere flows to XLSX
print("\n-----------Write unlinked flows to excel file")
agribalyse_db_updated_simapro.write_excel(only_unlinked = True)

# Show statistic of current linking of database import
print("\n-----------Linking statistics of current database import")
agribalyse_db_updated_simapro.statistics()
print()
    
# Write database
print("\n-----------Write database: " + agribalyse_db_updated_simapro)
agribalyse_db_updated_simapro.write_database(overwrite = False)
print()


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
df_1.query("Method_standardized != ''").to_excel(output_path / "comparison_ecoinvent_SimaPro_filtered.xlsx")

#%% Comparison 2

# Extract all inventories
# ... from Agribalyse (SimaPro) with ecoinvent v3.8 background
agribalyse_simapro_inventories: list = [m for m in bw2data.Database(agribalyse_db_name_simapro)]

# ... from Agribalyse (SimaPro) with ecoinvent v3.10 background
agribalyse_updated_simapro_inventories: list = [m for m in bw2data.Database(agribalyse_db_name_updated_simapro)]

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

df_2: pd.DataFrame = pd.DataFrame(LCA_results_agribalyse_simapro["LCIA_activity_scores"] + LCA_results_agribalyse_updated_simapro["LCIA_activity_scores"])
df_2.to_csv(output_path / "comparison_agribalyse_updated_background.csv")

df_2["Method_standardized"] = [LCIA_mapping.get(m, "") for m in list(df_2["Method"])]
df_2.query("Method_standardized != ''").to_excel(output_path / "comparison_agribalyse_updated_background_filtered.xlsx")

#%% Build a random inventory with the builder
tech_exchange: dict = create_base_exchange(exc_name = "test exchange",
                                           exc_SimaPro_name = "text exchange {GLO}",
                                           exc_cat = ("energy",),
                                           exc_SimaPro_cat = ("Materials/fuels",),
                                           exc_unit = "kilogram",
                                           exc_SimaPro_unit = "kg",
                                           exc_location = "GLO",
                                           exc_amount = 3,
                                           exc_type = "technosphere",
                                           exc_database = "test_db",
                                           exc_code = None,
                                           exc_uncertainty_type = "undefined",
                                           exc_uncertainty_taken_from_existing_inventory = False,
                                           exc_scale = None,
                                           exc_shape = None,
                                           exc_minimum = None,
                                           exc_maximum = None,
                                           overwrite_existing_key_value_pairs_with_kwargs = False,
                                           some_additional_info = "additional_inventory_information"
                                           )

inv: dict = create_base_inventory(inv_name = "test inventory",
                                  inv_SimaPro_name = "test inventory {GLO}",
                                  inv_cat = ("Energy",),
                                  inv_SimaPro_category_type = "material",
                                  inv_unit = "kilogram",
                                  inv_SimaPro_unit = "kg",
                                  inv_location = "GLO",
                                  inv_SimaPro_classification = ("test_db", "category"),
                                  inv_amount = 2,
                                  inv_database = "test_db",
                                  inv_allocation = 0.55,
                                  inv_type = "process",
                                  inv_code = None,
                                  inv_exchanges = [tech_exchange],
                                  overwrite_existing_key_value_pairs_with_kwargs = False
                                  )


#%% Export inventories to SimaPro CSV
SimaPro_CSV_text_block: str = export_SimaPro_CSV(list_of_Brightway2_pewee_objects = ecoinvent_simapro_inventories[10:20],
                                                 folder_path_SimaPro_CSV = output_path,
                                                 file_name_SimaPro_CSV_without_ending = "10_exported_SimaPro_inventories",
                                                 file_name_print_timestamp = True,
                                                 separator = "\t",
                                                 avoid_exporting_inventories_twice = True,
                                                 csv_format_version = "7.0.0",
                                                 decimal_separator = ".",
                                                 date_separator = ".",
                                                 short_date_format = "dd.MM.yyyy")

#%%
for method_name, _ in LCIA_mapping.items():
    method = bw2data.Method(method_name).load()
    print(str(len(method)) + " char. factors, " + str(method_name))
    print()




