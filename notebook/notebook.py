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
                  register_biosphere,
                  register_SimaPro_LCIA_methods,
                  add_damage_normalization_weighting,
                  write_biosphere_flows_and_method_names_to_XLSX)

from lci import (# unregionalize_biosphere,
                 import_SimaPro_LCI_inventories,
                 import_XML_LCI_inventories,
                 migrate_from_excel_file,
                 migrate_from_json_file,
                 # create_XML_biosphere_from_elmentary_exchanges_file,
                 create_XML_biosphere_from_LCI
                 )

from harmonization import (# extract_ecoinvent_UUID_from_SimaPro_comment_field,
                           # identify_and_detoxify_SimaPro_name_of_ecoinvent_inventories,
                           create_harmonized_biosphere_migration,
                           create_harmonized_activity_migration,
                           elementary_flows_that_are_not_used_in_XML_methods)

from correspondence.correspondence import (create_correspondence_mapping)

from utils import (change_brightway_project_directory)
from calculation import (run_LCA)
# from calculation import (LCA_Calculation)

#%% File- and folderpaths, key variables

# LCI and LCIA data
LCIA_SimaPro_CSV_folderpath: pathlib.Path = here.parent / "data" / "lcia" / "fromSimaPro"
LCIA_XML_folderpath: pathlib.Path = here.parent / "data" / "lcia" / "fromXML" / "ecoinvent 3.10_LCIA_implementation"
LCIA_XML_filename: str = "LCIA Implementation 3.10.xlsx"
LCIA_XML_sheetname: str = "CFs"
LCI_ecoinvent_simapro_folderpath: pathlib.Path = here.parent / "data" / "lci" / "ECO_fromSimaPro"
LCI_ecoinvent_xml_folderpath: pathlib.Path = here.parent / "data" / "lci" / "ECO_fromXML"
LCI_agribalyse_simapro_folderpath: pathlib.Path = here.parent / "data" / "lci" / "AGB_fromSimaPro"

# Generic and Brightway
project_path: pathlib.Path = here / "notebook_data"
project_path.mkdir(exist_ok = True)
project_name: str = "notebook"

# Correspondence files
folderpath_correspondence_files: pathlib.Path = here.parent / "correspondence" / "data"

#%% Change brightway project directory and setup project
change_brightway_project_directory(project_path)
bw2data.projects.set_current(project_name)

#%% File- and folderpaths

# Setup output path
output_path: pathlib.Path = pathlib.Path(bw2data.projects.output_dir)

# Migration
filename_biosphere_migration_data: str = "biosphere_migration.json"
filepath_biosphere_migration_data: pathlib.Path = output_path / filename_biosphere_migration_data
filename_activity_migration_data: str = "activity_migration.json" # !!! TODO
filepath_activity_migration_data: pathlib.Path = output_path / filename_activity_migration_data # !!! TODO

# Logs
filename_biosphere_flows_not_specified_in_XML_methods: str = "biosphere_flows_not_specified_in_LCIA_methods_from_XML.xlsx"

# Other
filename_SBERT_names_validated: str = "manually_checked_SBERT_names.xlsx"

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
                   BRIGHTWAY2_DIR = project_path,
                   biosphere_db_name = biosphere_db_name,
                   imported_methods = methods,
                   verbose = True)

register_SimaPro_LCIA_methods(imported_methods = methods,
                              biosphere_db_name = biosphere_db_name,
                              Brightway_project_name = project_name,
                              BRIGHTWAY2_DIR = project_path,
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
ecoinvent_db_simapro: bw2io.importers.base_lci.LCIImporter = import_SimaPro_LCI_inventories(SimaPro_CSV_LCI_filepaths = [LCI_ecoinvent_simapro_folderpath / "ECO.CSV"],
                                                                                            db_name = ecoinvent_db_name_simapro,
                                                                                            encoding = "latin-1",
                                                                                            delimiter = "\t",
                                                                                            verbose = True)
# ecoinvent_db_simapro.apply_strategy(unregionalize_biosphere) # !!! remove?
# ecoinvent_db_simapro.apply_strategy(extract_ecoinvent_UUID_from_SimaPro_comment_field)
# ecoinvent_db_simapro.apply_strategy(identify_and_detoxify_SimaPro_name_of_ecoinvent_inventories)
# df: pd.DataFrame = pd.DataFrame([{k: v for k, v in m.items() if k.lower() not in ["exchanges", "simapro metadata"]} for m in ecoinvent_db_simapro]) # !!! remove

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
                                            excel_migration_filepath = LCI_ecoinvent_simapro_folderpath / "custom_migration_ECO.xlsx"),
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
print("\n------- Statistics")
ecoinvent_db_simapro.statistics()

# Make a new biosphere database for the flows which are currently not linked
# Add unlinked biosphere flows with a custom function
unlinked_biosphere_flows: dict = utils.add_unlinked_flows_to_biosphere_database(db = ecoinvent_db_simapro,
                                                                                biosphere_db_name_unlinked = unlinked_biosphere_db_name,
                                                                                biosphere_db_name = biosphere_db_name,
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


#%% Import Agribalyse LCI database 
agribalyse_db_simapro: bw2io.importers.base_lci.LCIImporter = import_SimaPro_LCI_inventories(SimaPro_CSV_LCI_filepaths = [LCI_agribalyse_simapro_folderpath / "AGB.CSV"],
                                                                                             db_name = agribalyse_db_name_simapro,
                                                                                             encoding = "latin-1",
                                                                                             delimiter = "\t",
                                                                                             verbose = True)
# agribalyse_db_simapro.apply_strategy(unregionalize_biosphere) # !!! remove?
# agribalyse_db_simapro.apply_strategy(extract_ecoinvent_UUID_from_SimaPro_comment_field)
# agribalyse_db_simapro.apply_strategy(identify_and_detoxify_SimaPro_name_of_ecoinvent_inventories)

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
                                             excel_migration_filepath = LCI_agribalyse_simapro_folderpath / "custom_migration_AGB.xlsx"),
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
                                                                                biosphere_db_name = biosphere_db_name,
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



#%% Import ecoinvent LCI database (XML)
# Import ecoinvent database from XML LCI data
ecoinvent_db_xml: bw2io.importers.ecospold2.SingleOutputEcospold2Importer = import_XML_LCI_inventories(XML_LCI_filepath = LCI_ecoinvent_xml_folderpath / "ecoinvent 3.10_cutoff_ecoSpold02" / "datasets",
                                                                                                       db_name = ecoinvent_db_name_xml,
                                                                                                       biosphere_db_name = biosphere_db_name,
                                                                                                       db_model_type_name = "cutoff",
                                                                                                       db_process_type_name = "unit",
                                                                                                       verbose = True)

#%% Create JSON files containing biosphere flow data
# Create biosphere from XML LCI data
biosphere_flows_from_XML_LCI_data: list[dict] = create_XML_biosphere_from_LCI(db = ecoinvent_db_xml,
                                                                              biosphere_db_name = biosphere_db_name)
    
# # Create the biosphere from XML file containing all elementary exchanges
# biosphere_flows_from_XML_elementary_exchanges: list[dict] = create_XML_biosphere_from_elmentary_exchanges_file(filepath_ElementaryExchanges = LCI_ecoinvent_xml_folderpath / "ecoinvent 3.10_cutoff_ecoSpold02" / "MasterData" / "ElementaryExchanges.xml",
#                                                                                                                biosphere_db_name = biosphere_db_name)

# Read excel file containing the LCIA methods from ecoinvent
df_LCIA_methods_ecoinvent: pd.DataFrame = pd.read_excel(LCIA_XML_folderpath / LCIA_XML_filename, sheet_name = LCIA_XML_sheetname)

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
biosphere_flows_from_SimaPro: list[dict] = [dict(m) for m in bw2data.Database(biosphere_db_name)]

# Create the JSON object to be written
biosphere_flows_from_SimaPro_json: dict = json.dumps({idx: dict(m) for idx, m in enumerate(biosphere_flows_from_SimaPro)}, indent = 4)

# Write the unlinked biosphere data dictionary to a JSON file
with open(output_path / ("biosphere_flows_from_SimaPro.json"), "w") as outfile:
    outfile.write(biosphere_flows_from_SimaPro_json)


#%% Harmonize biospheres and create migration JSON

# Load dataframe with manually checked biosphere flows
manually_checked_SBERTs: pd.DataFrame = pd.read_excel(LCI_ecoinvent_xml_folderpath / filename_SBERT_names_validated)

# Harmonize the two biospheres
biosphere_migration_data: dict = create_harmonized_biosphere_migration(biosphere_flows_1 = biosphere_flows_from_XML_used,
                                                                       biosphere_flows_2 = biosphere_flows_from_SimaPro,
                                                                       manually_checked_SBERTs = manually_checked_SBERTs,
                                                                       output_path = output_path)

# Create the JSON object to be written
biosphere_migration_data_in_json_format = json.dumps(biosphere_migration_data, indent = 3)

# Write the biosphere dictionary to a JSON file
with open(filepath_biosphere_migration_data, "w") as outfile:
   outfile.write(biosphere_migration_data_in_json_format)


#%% Remove unused flows
def remove_unused_biosphere_flows(db):
    for ds in db:
        ds["exchanges"]: list = [m for m in ds["exchanges"] if not (m["type"] == "biosphere" and (biosphere_db_name, m["code"]) in not_used_flows)]
    return db

# Apply strategy and remove unused flows
ecoinvent_db_xml.apply_strategy(remove_unused_biosphere_flows)

            
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
ecoinvent_db_xml.apply_strategy(replace_XML_code_field_with_SimaPro_code)

#%% Migrate biosphere flows and register ecoinvent XML database

# Apply biosphere migration
ecoinvent_db_xml.apply_strategy(partial(migrate_from_json_file,
                                        json_migration_filepath = filepath_biosphere_migration_data),
                                verbose = True)

ecoinvent_db_xml.apply_strategy(partial(link.remove_linking,
                                        production_exchanges = True,
                                        substitution_exchanges = True,
                                        technosphere_exchanges = True,
                                        biosphere_exchanges = True), verbose = True)

ecoinvent_db_xml.apply_strategy(partial(link.link_activities_internally,
                                        production_exchanges = True,
                                        substitution_exchanges = True,
                                        technosphere_exchanges = True,
                                        relink = False,
                                        strip = True,
                                        case_insensitive = True,
                                        remove_special_characters = False,
                                        verbose = True), verbose = True)

# Apply external linking of biosphere flows
ecoinvent_db_xml.apply_strategy(partial(link.link_biosphere_flows_externally,
                                        biosphere_db_name = biosphere_db_name,
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



#%% Creat correspondence mapping
correspondence_mapping: list[dict] = create_correspondence_mapping(path_to_correspondence_files = folderpath_correspondence_files,
                                                                   model_type = "cutoff",
                                                                   map_to_version = (3, 10),
                                                                   output_path = output_path)

#%% Update the ecoinvent background activities in Agribalyse to the latest version
# !!! TODO
activity_migration_data: dict = create_harmonized_activity_migration(activity_flows_1 = list(agribalyse_db_simapro),
                                                                     activity_flows_2 = list(ecoinvent_db_simapro),
                                                                     correspondence_mapping = correspondence_mapping,
                                                                     output_path = output_path)


#%% Run LCA calculation

# Methods to use for the LCA calculation
methods: list[tuple[str]] = [("AWARE", "Water use"),
                             ("IPCC 2021", "GWP100 - fossil"),
                             ("USEtox v2 (recommended + interim)", "Freshwater ecotoxicity"),
                             ("ReCiPe 2016 Midpoint (H)", "Freshwater eutrophication"),
                             ("SALCA v2.01", "Land occupation - Total"),
                             ("SALCA v2.01", "Land transformation - Deforestation")]

# Check if all specified methods are registered in the Brightway background
for method in methods:
    error: bool = False
    if method not in bw2data.methods:
        error: bool = True
        print("Method not registered: '{}'".format(method))

# If unregistered methods have been detected, raise error
if error:
    raise ValueError("Unregistered methods detected.")

# Extract all inventories
# ... from ecoinvent (SimaPro)
ecoinvent_simapro_inventories: list = [m for m in bw2data.Database(ecoinvent_db_name_simapro)]

# ... from ecoinvent (XML)
ecoinvent_xml_inventories: list = [m for m in bw2data.Database(ecoinvent_db_name_xml)]

# Run LCA calculation
LCA_results_ecoinvent_simapro: dict[str, pd.DataFrame] = run_LCA(activities = ecoinvent_simapro_inventories,
                                                                 methods = methods,
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
LCA_results_ecoinvent_simapro: dict[str, pd.DataFrame] = run_LCA(activities = ecoinvent_xml_inventories,
                                                                 methods = methods,
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

see = list(agribalyse_db_simapro)[1500:1550]
# mets: list[tuple] = [m for m in bw2data.methods][1:5]
# invs: list[bw2data.backends.proxies.Activity] = [m for m in bw2data.Database(ecoinvent_db_name_simapro)][1:5]

# calculation: LCA_Calculation = LCA_Calculation(activities = invs,
#                                                methods = mets,
#                                                functional_amount = 1.5,
#                                                cut_off_percentage = None,
#                                                exchange_level = 1,
#                                                print_progress_bar = True)

# calculation.calculate_all()
# calculation.calculate_LCIA_scores()
# calculation.extract_LCI_exchanges(exchange_level = 3)
# calculation.calculate_LCIA_scores_of_exchanges(exchange_level = 2)
# calculation.extract_LCI_emission_contribution()
# calculation.extract_LCI_process_contribution()
# calculation.calculate_LCIA_emission_contribution()
# calculation.calculate_LCIA_process_contribution()
# calculation.extract_characterization_factors()
# results_dict_cleaned = calculation.get_result_dictionaries(True)
# results_dict_not_cleaned = calculation.get_result_dictionaries(False)
# results_df_cleaned = calculation.get_result_dataframes(True)
# results_df_not_cleaned = calculation.get_result_dataframes(False)

# calculation.write_results(path = output_path,
#                           filename = "LCA_calculation_esults",
#                           use_timestamp_in_filename = True)

