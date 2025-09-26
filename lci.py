import pathlib

if __name__ == "__main__":
    import os
    os.chdir(pathlib.Path(__file__).parent)

import re
import ast
import copy
import json
import bw2io
import pathlib
import hashlib
import datetime
import pandas as pd
import helper as hp
import linking
from lcia import (ensure_categories_are_tuples,
                  create_SimaPro_fields,
                  normalize_simapro_biosphere_categories,
                  transformation_units,
                  add_location_to_biosphere_exchanges,
                  add_top_and_subcategory_fields_for_biosphere_flows)

starting: str = "------------"


# Exact names of compartments in CSV files from SimaPro
SIMAPRO_PRODUCT_COMPARTMENTS = {"products": "Products"}

SIMAPRO_SUBSTITUTION_COMPARTMENTS = {"avoided_products": "Avoided products"}

SIMAPRO_TECHNOSPHERE_COMPARTMENTS = {"materials_fuels": "Materials/fuels",
                                     "electricity_heat": "Electricity/heat",
                                     "final_waste_flows": "Final waste flows",
                                     "waste_to_treatment": "Waste to treatment"}

SIMAPRO_BIOSPHERE_COMPARTMENTS = {"resources": "Resources",
                                  "emissions_air": "Emissions to air",
                                  "emissions_water": "Emissions to water",
                                  "emissions_soil": "Emissions to soil",
                                  "non_material_emissions": "Non material emissions",
                                  "social_issues": "Social issues",
                                  "economic_issues": "Economic issues"}

SIMAPRO_COMPARTMENTS = dict(**SIMAPRO_PRODUCT_COMPARTMENTS,
                            **SIMAPRO_SUBSTITUTION_COMPARTMENTS,
                            **SIMAPRO_TECHNOSPHERE_COMPARTMENTS,
                            **SIMAPRO_BIOSPHERE_COMPARTMENTS)

SIMAPRO_CATEGORY_TYPE_COMPARTMENT_MAPPING = {"material": SIMAPRO_COMPARTMENTS["materials_fuels"],
                                             "waste treatment": SIMAPRO_COMPARTMENTS["waste_to_treatment"],
                                             "energy": SIMAPRO_COMPARTMENTS["electricity_heat"],
                                             "processing": SIMAPRO_COMPARTMENTS["materials_fuels"],
                                             "transport": SIMAPRO_COMPARTMENTS["materials_fuels"],
                                             "use": SIMAPRO_COMPARTMENTS["materials_fuels"]}


#%% LCI strategies
def change_database_name_and_remove_code(db_var,
                                         new_database_name: str,
                                         list_of_types: list = ["production"]):
    
    # Make variable check
    hp.check_function_input_type(change_database_name_and_remove_code, locals())
    
    # Loop through each inventory
    for ds in db_var:
        
        # Rename
        ds["database"] = new_database_name
        
        # Delete existing code
        try: del ds["code"]
        except: pass
        
        # Loop through each exchange of that inventory
        for exc in ds["exchanges"]:
            
            # Rename
            if exc["type"] in list_of_types:
                exc["database"] = new_database_name
                
    return db_var



def extract_geography_from_SimaPro_name(db_var,
                                        placeholder_for_not_identified_locations: str = "not identified"):
    
    """ Extract the geography from the SimaPro inventory names and write it to a key 'location'.
    The original SimaPro name is set to ``SimaPro_name``."""
    
    # Check function input type
    hp.check_function_input_type(extract_geography_from_SimaPro_name, locals())
    
    # Define the patterns needed
    # Extract everything in between curly brackets
    pat_curly_brackets = "^(?P<name_I>.*\{)(?P<country>[A-Za-z0-9\s\-\&\/\+\,]{2,})(?P<name_II>\}.*)$"
    
    # A general SimaPro pattern. Characters after '/' are identified as locations
    pat_simapro_general = "^(?P<name_I>.*\/)(?P<country>[A-Za-z\-]{2,})(?P<name_II>\s([A-Za-z0-9\s]{2,})?(S|U))$"
    
    # Specific pattern that is used in the SALCA database
    pat_SALCA = "^(?P<name_I>.*\/(?P<unit>[A-Za-z0-9]+)\/)(?P<country>[A-Z]{2,})(?P<name_II>(\/I)?\s(.*)(S|U|$))"
    
    # Set up the function to extract the patterns from the inventory names
    def apply_regex(name_string, pat1, pat2, pat3):
        
        # Apply the patterns to the function input variable 'name_string'
        pat1 = re.match(pat_curly_brackets, name_string)
        pat2 = re.match(pat_simapro_general, name_string)
        pat3 = re.match(pat_SALCA, name_string)
        
        # Check if the patterns are found (if None is returned, no pattern is found)
        if pat1 is not None:
            x = pat1
        elif pat2 is not None:
            x = pat2
        elif pat3 is not None:
            x = pat3
        else:
            # If no pattern has been found, simply return None
            return None
        
        # Otherwise return the grouped dictionary
        return x.groupdict()
    
    # Loop through each inventory in the database
    for ds in db_var:
        
        # Apply the function to extract the location and the new inventory name
        extracted_from_ds = apply_regex(name_string = ds["name"],
                                        pat1 = pat_curly_brackets,
                                        pat2 = pat_simapro_general,
                                        pat3 = pat_SALCA)
        
        # Check if location has already been extracted and a new location was found
        if "location" not in ds and extracted_from_ds is not None:
                
                # Write the old inventory name to 'SimaPro_name'
                ds["SimaPro_name"] = str(ds["name"])
            
                # Extract the new inventory name and overwrite the existing key 'name'
                ds["name"] = str(extracted_from_ds["name_I"]) + "{COUNTRY}" + str(extracted_from_ds["name_II"])
            
                # Add the location
                ds["location"] = str(extracted_from_ds["country"])
        
        # Check if location has already been tried to extract and was not sucessfully found. In that case, we do it again and try again to extract something.
        elif ds.get("location", "") == placeholder_for_not_identified_locations and extracted_from_ds is not None:
                
            # Check if the new country is again COUNTRY. If yes, we don't want to add it.
            if not bool(re.search("COUNTRY", extracted_from_ds["country"])):
                
                # Write the old inventory name to 'SimaPro_name'
                ds["SimaPro_name"] = str(ds["name"])
            
                # Extract the new inventory name and overwrite the existing key 'name'
                ds["name"] = str(extracted_from_ds["name_I"]) + "{COUNTRY}" + str(extracted_from_ds["name_II"])
            
                # Add the location
                ds["location"] = str(extracted_from_ds["country"])
           
        elif "location" in ds:
            "nothing to do"
            
        else:
            # Write the old inventory name to 'SimaPro_name'
            ds["SimaPro_name"] = str(ds["name"])
                
            # Add 'not identified' for the location
            ds["location"] = placeholder_for_not_identified_locations
        
        
        
        # Loop through each exchange in the inventory 'ds'
        for exc in ds.get("exchanges", []):
            
            # Only go on with extracting the location if the exchange is not of type 'biosphere'
            if exc["type"] == "biosphere":
                continue
            
            else:
                
                # Again apply the function to extract the location and the new name of the current exchange
                extracted_from_exc = apply_regex(name_string = exc["name"],
                                                 pat1 = pat_curly_brackets,
                                                 pat2 = pat_simapro_general,
                                                 pat3 = pat_SALCA)
                
                # Check if location has already been extracted
                if "location" not in exc and extracted_from_exc is not None:
                    
                    # Write the old exchange name to 'SimaPro_name'
                    exc["SimaPro_name"] = str(exc["name"])
                
                    # Extract the new exchange name and overwrite the existing key 'name'
                    exc["name"] = str(extracted_from_exc["name_I"]) + "{COUNTRY}" + str(extracted_from_exc["name_II"])
            
                    # Add the location
                    exc["location"] = str(extracted_from_exc["country"])
                
                
                # Check if location has already been tried to extract and was not sucessfully found. In that case, we do it again and try again to extract something.
                elif exc.get("location", "") == placeholder_for_not_identified_locations and extracted_from_exc is not None:
                    
                    # Check if the new country is again COUNTRY. If yes, we don't want to add it.
                    if not bool(re.search("COUNTRY", extracted_from_exc["country"])):
                    
                        # Write the old exchange name to 'SimaPro_name'
                        exc["SimaPro_name"] = str(exc["name"])
                    
                        # Extract the new exchange name and overwrite the existing key 'name'
                        exc["name"] = str(extracted_from_exc["name_I"]) + "{COUNTRY}" + str(extracted_from_exc["name_II"])
                
                        # Add the location
                        exc["location"] = str(extracted_from_exc["country"])
                
                elif "location" in exc:
                    "nothing to do"
                    
                else:
                    # Write the old exchange name to 'orig_name'
                    exc["SimaPro_name"] = str(exc["name"])
                    
                    # Add 'not identified' for the location
                    exc["location"] = placeholder_for_not_identified_locations
            
    return db_var



def set_code(db_var,
             fields: (tuple | list),
             overwrite: bool,
             strip: bool,
             case_insensitive: bool,
             remove_special_characters: bool):

    # Path to input file where UUIDs are stored
    file_path = pathlib.Path(__file__).parent / "UUIDs.xlsx"
    
    # Current date formatted as a string
    current_date_string = datetime.datetime.now().strftime("%Y-%m-%d")
    
    # We exclude 'NA' because it refers to country abbreviation and should be read as 'NA' string instead of float("NaN")
    na_values = ["", 
                 "#N/A", 
                 "#N/A N/A", 
                 "#NA", 
                 "-1.#IND", 
                 "-1.#QNAN", 
                 "-NaN", 
                 "-nan", 
                 "1.#IND", 
                 "1.#QNAN", 
                 "<NA>", 
                 "N/A", 
                 # "NA", 
                 "NULL", 
                 "NaN", 
                 "n/a", 
                 "nan", 
                 "null"]
    
    # Initialize variable to store all mapping UUIDs
    UUID_mapping: dict = {}
    
    # Create empty excel file if not yet existing
    if "UUIDs.xlsx" not in [m.name for m in file_path.parent.iterdir()]:
        pd.DataFrame(columns = ("UUID",) + tuple(fields) + ("date_created",)).to_excel(file_path, sheet_name = "UUIDs")
    
    # Import existing UUIDs from Excel file
    UUIDs_orig = pd.read_excel(file_path, sheet_name = "UUIDs", na_values = na_values, keep_default_na = False) 
    
    # Check if all fields provided can be found in the Excel file
    not_specified_in_excel = [m for m in fields if m not in UUIDs_orig.columns]
    
    # If not, raise error indicating the fields hwich are not provided
    if not_specified_in_excel != []:
        raise ValueError("The field(s) '" + ", ".join(not_specified_in_excel) + "' is/are not specified as column in the excel file '" + str(file_path) + "'")
    
    # Extract all rows which contain at least one missing value
    rows_with_nan = UUIDs_orig[UUIDs_orig.isnull().any(axis = 1)]
    
    # Raise error if rows are found that do not contain all information according to the fields specified
    if len(rows_with_nan) > 0:
        raise ValueError("Some rows in file 'data_UUIDs.xlsx' contain missing fields. Check indexes:\n" + "\n".join([" - " + str(m + 1) for m in list(rows_with_nan.index)]))
    
    # Loop through each line in the excel file
    for item in UUIDs_orig.to_dict("records"):
        
        # Create the unique key (= ID) by using the parsing function
        key = hp.format_values(tuple([item[m] for m in fields]),
                               case_insensitive = case_insensitive,
                               strip = strip,
                               remove_special_characters = remove_special_characters)
        
        # The value is simply retrieved from the UUID column
        value = item["UUID"]
        
        # We don't add to the mapping file, if the exact same key already exists
        if key in UUID_mapping:
            continue
        
        # Add information to mapping dictionary
        UUID_mapping[key] = value
    
    # Initialize variable to store the amount of missing fields from the activities
    missing_fields = {m: 0 for m in fields}
    missing_fields_error = False
    
    # Initialize a list where newly created UUIDs can be stored, in order to be written to the excel afterwards
    new_UUIDs: list = []
    
    # Loop through all activities
    for ds in db_var:
        
        # If a code field is already existing and we do not want to overwrite the code, go to next activity
        if "code" in ds and not overwrite:
            continue

        # Loop through each relevant field (previously specified)
        for field in fields:
            
            # If field is missing, write to variable -> will raise error in the end
            # Reason: we can only assign an UUID correctly, if all fields specified can be found for all activities
            if ds.get(field) is None:
                missing_fields[field] += 1
                missing_fields_error = True
        
        # Use the format values function to check for the uniqueness of the flow
        ID = hp.format_values(tuple([ds.get(m) for m in fields]),
                              case_insensitive = True,
                              strip = True,
                              remove_special_characters = False)
        
        # Either use existing UUID from Excel file or create a new one
        if ID in UUID_mapping:
            
            # If the ID is already existing in the Excel file (= in mapping), use it
            UUID = UUID_mapping[ID]
            
        else:
            # Otherwise, create a new UUID
            UUID = str(hashlib.md5("".join([m for m in ID if m is not None]).encode("utf-8")).hexdigest())
            
            # Write the new UUID to the list in order to be added to the Excel afterwards
            new_UUIDs += [dict({field: ds.get(field) for field in fields},
                               **{"UUID": UUID, "date_created": current_date_string})]
        
        # Add code field with the UUID found/created
        ds["code"]: str = UUID
        
        # Raise error if None or more than one production exchange was found
        prod_exchanges = [m for m in ds["exchanges"] if m["type"] == "production"]
        assert len(prod_exchanges) == 1, str(len(prod_exchanges)) + " production exchange(s) was/were found. Allowed is only one. Allocate products/inventories first and make sure to only provide one production exchange. Error occured in " + str(ds)
        
        # We also need to adapt the production exchange
        for exc in ds["exchanges"]:
            
            # Only modify if the current exchange is a production exchange
            if exc["type"] == "production":
            
                # Delete output field
                try: del exc["output"]
                except: pass
                
                # Overwrite or create 'code' field with the new code of the inventory
                exc["code"]: str = UUID
                
                # Overwrite or create 'input' field with the new code of the inventory and the database
                exc["input"]: tuple = (ds["database"], UUID)
                

    # Raise error if fields were not found in activities
    if missing_fields_error:
        formatted_error = "The following fields were found to be missing (of a total of " + str(len(db_var)) + " inventories)\n" + "\n".join([" - '" + k + "' --> missing in " + str(v) + " activity/ies" for k, v in missing_fields.items() if v > 0])
        raise ValueError(formatted_error)
    
    # Write dataframe with new UUIDs
    pd.DataFrame(UUIDs_orig.to_dict("records") + new_UUIDs).to_excel(file_path, sheet_name = "UUIDs", index = False)
 
    return db_var


# Created by Chris Mutel
def drop_final_waste_flows(db_var):
    
    # Loop through each inventory
    for ds in db_var:
        
        # Loop through each exchange and only keep the exchange if it is not of type 'Final waste flows'
        # Otherwise, exclude the exchange
        ds["exchanges"] = [exc for exc in ds["exchanges"] if (exc.get("input") or exc["categories"] != ("Final waste flows",))]
    
    return db_var


def add_SimaPro_classification(db_var):
    
    # Initialize list to gather the inventory names where classification was not available
    inventory_classification_not_available: list = []
    
    # Loop through each inventory
    for ds in db_var:
        
        # Go on if all fields are already available
        if "SimaPro_classification" in ds and "SimaPro_categories" in ds and "categories" in ds:
            continue
        
        # Initialize list
        inventory_classification: list = []
        
        # Loop through each exchange
        for exc in ds["exchanges"]:
            
            if exc["type"] == "production":
                
                # The inventory classification is extracted from 'categories' parameter from the production exchange
                inventory_classification += [exc["categories"]]
                
                # Adapt the production exchange
                exc["SimaPro_classification"] = exc["categories"]
                exc["SimaPro_categories"] = (SIMAPRO_PRODUCT_COMPARTMENTS["products"],)
                exc["categories"] = exc["SimaPro_categories"]
                            
        # Raise error if more than one or no production exchange was found
        assert len(inventory_classification) == 1, str(len(inventory_classification)) + " production exchange(s) found. Only one allowed. Check the following inventory:\n\n" + str(ds["name"])

        # If an inventory classification was found, add to the inventory
        if inventory_classification != [] and "SimaPro_classification" not in ds.keys() and not all([x is None for x in inventory_classification]):
            ds["SimaPro_classification"] = inventory_classification[0]
        
        else:
            # Otherwise, add a None
            ds["SimaPro_classification"] = None
            
            # Add to list of errors
            inventory_classification_not_available += [ds["name"]]
    
    # Raise error if inventory classification could not be specified for some inventories
    if inventory_classification_not_available != []:
        raise ValueError("For some inventories, the inventory classification of the output could not be extracted. Error occured in the following inventories:\n\n - " + "\n - ".join(set(inventory_classification_not_available)))
    
    
    return db_var



def add_SimaPro_categories_and_category_type(db_var):

    # Loop through each inventory
    for ds in db_var:
        
        # Go on if all fields are already available
        if "SimaPro_category_type" in ds and "SimaPro_categories" in ds and "categories" in ds:
            continue
        
        # Extract the category type from the SimaPro meta data
        category_type: (str | None) = ds.get("simapro metadata", {}).get("Category type")
        
        # If it is None (not available), add to list
        if category_type is None:
            category_type: str = "material" # We specify here that category type is 'material' if none is provided
        
        # Otherwise, add respective fields
        ds["SimaPro_category_type"]: str = category_type
        ds["categories"]: tuple = (SIMAPRO_CATEGORY_TYPE_COMPARTMENT_MAPPING[category_type],)
        ds["SimaPro_categories"]: tuple = (SIMAPRO_CATEGORY_TYPE_COMPARTMENT_MAPPING[category_type],)
    
    return db_var


def add_allocation_field(db_var):
    
    # Loop through each inventory
    for ds in db_var:
        
        # For waste treatments, unfortunately no allocation is specified
        # We assume that there is always only one product and therefore the allocation is 1.
        if ds["SimaPro_categories"] == (SIMAPRO_TECHNOSPHERE_COMPARTMENTS["waste_to_treatment"],):
            ds["allocation"]: float = float(1)
            continue
        
        # Initialize a list to store all allocation factors
        allocations: list = []
        
        # Loop through each exchange
        for exc in ds["exchanges"]:
            
            # Check if current exchange is of type production
            if exc["type"] == "production":
                
                # Extract allocation
                allocation: (float | None) = exc.get("allocation")
                
                # Raise error if not found
                if allocation is None:
                    raise ValueError("Allocation field not found in production exchange.")
                
                # The inventory classification is extracted from 'categories' parameter from the production exchange
                allocations += [exc["allocation"]]
        
        # Raise error if more than one or no production exchange was found
        assert len(allocations) == 1, str(len(allocations)) + " production exchange(s) found. Only one allowed. Check the following inventory:\n\n" + str(ds["name"])
        
        # Add allocation field to inventory
        ds["allocation"] = float(allocations[0] / 100)
        
    return db_var



def add_output_amount_field(db_var):
    
    # Loop through each inventory
    for ds in db_var:
        
        allocation: (float | None) = ds.get("allocation")
        prod_amount: (float | None) = ds.get("production amount")
        
        if allocation is None:
            raise ValueError("Field 'allocation' not provided in current inventory dictionary.")
        
        if prod_amount is None:
            raise ValueError("Field 'production amount' not provided in current inventory dictionary.")
        
        # Add output amount field to inventory
        ds["output amount"]: float = float(prod_amount * allocation)
        
    return db_var



# Erase all exchanges that have a zero amount
def remove_exchanges_with_zero_amount(db_var):
    
    # Loop through each inventory
    for ds in db_var:
        
        # Initialize an empty list to store all exchanges that have an amount other than 0
        exchanges: list = []
        
        # Loop through all exchanges
        for exc in ds["exchanges"]:
            
            # Check if the amount of the current exchange is 0
            # If yes, do not add it (= erase it)
            if exc["amount"] == 0:
                
                # However, we do not add it only if it is not of type production
                if exc["type"] != "production":
                    continue
            
            # Otherwise, if amount is other than 0, add
            exchanges += [exc]
        
        # Overwrite the old exchanges with the list of new exchanges
        ds["exchanges"]: list = copy.deepcopy(exchanges)

    return db_var



def remove_duplicates(db_var,
                      fields: (tuple | list),
                      strip: bool,
                      case_insensitive: bool,
                      remove_special_characters: bool):
    
    # Make variable check
    hp.check_function_input_type(remove_duplicates, locals())
    
    # Initialize a variable to store all elements already found
    container: dict = {}
    
    # Initialize a variable to store the new list without the duplicates
    new_db_var: list = []
    
    # Initialize variable to store the amount of missing fields from the inventories
    missing_fields: dict = {m: 0 for m in fields}
    missing_fields_error: bool = False
    
    # Loop through each inventory
    for ds in db_var:
        
        # Loop through each relevant field (previously specified)
        for field in fields:
            
            # If field is missing, write to variable -> will raise error in the end
            # Reason: we can only remove duplicates correctly, if all fields specified can be found for all inventories
            if ds.get(field) is None:
                missing_fields[field] += 1
                missing_fields_error: bool = True
        
        # Create the ID of the current inventory to check if it can be a duplicate            
        ID = hp.format_values(tuple([ds[m] for m in fields]),
                              case_insensitive = case_insensitive,
                              strip = strip,
                              remove_special_characters = remove_special_characters)
        
        # Check if ID has already been found (... and was stored in container)
        # If yes, the current ID is a duplicate and we erase it --> we just don't add it to the new list
        if container.get(ID, False):
            continue
        
        # Add the inventory to the new list
        new_db_var += [ds]
        
        # Update the container dictionary, in case we would find the same ID again further in the loop
        container[ID]: bool = True
        
    
    # Raise error if fields were not found in inventories
    if missing_fields_error:
        formatted_error = "The following fields were found to be missing (of a total of " + str(len(db_var)) + " inventories)\n" + "\n".join([" - '" + k + "' --> missing in " + str(v) + " inventory/ies" for k, v in missing_fields.items() if v > 0])
        raise ValueError(formatted_error)
            
    return new_db_var


def unregionalize(db_var):
    
    # Loop through each inventory
    for ds in db_var:
        
        # Loop through all exchanges
        for exc in ds["exchanges"]:
            
            # Check if current exchange is of type biosphere
            if exc["type"] == "biosphere":
                
                # Unregionalize, overwrite with GLO
                exc["location"]: str = "GLO"
                
    return db_var
            

def create_structured_migration_dictionary_from_excel(excel_dataframe: pd.DataFrame):
    
    # Check function input type
    hp.check_function_input_type(create_structured_migration_dictionary_from_excel, locals())
    
    # Fill NaN values with empty string
    df: pd.DataFrame = excel_dataframe.fillna("")
    
    # Extract columns
    cols: list[str] = list(df.columns)
    
    # Separate FROM and TO columns
    FROM_cols: list[str] = [m for m in cols if "FROM_" in m]
    TO_cols: list[str] = [m for m in cols if "TO_" in m or m == "multiplier"]
    
    # We need information for all FROM cols. Otherwise, the excel is invalid
    if any([[n for n in list(df[m]) if n == ""] != [] for m in FROM_cols]):
        raise ValueError("Invalid migration Excel. Some of the FROM columns contain empty values.")
    
    # Create fields --> all names from the FROM columns
    fields: list[str] = [m.replace("FROM_", "") for m in FROM_cols]
    
    # Initialize a list for the 'data'
    data: list = []
    
    # Loop through each row in the dataframe
    for idx, row in df.iterrows():
        
        # The first element is the identifier (uses the FROM cols)
        element_1: tuple = tuple([row[m] for m in FROM_cols])
        
        # The second element specifies which fields should be mapped to which values (uses TO cols)
        element_2 = {(m.replace("TO_", "")): row[m] for m in TO_cols if row[m] != ""}
        
        # Append to the data list
        data += [(element_1, element_2)]
        
    return {"fields": fields, "data": data} 
    

def create_migration_mapping(json_dict: dict):
    
    # Check function input type
    hp.check_function_input_type(create_migration_mapping, locals())
    
    # Raise error if format is wrong
    if "data" not in json_dict or "fields" not in json_dict:
        raise ValueError("Invalid migration dictionary. 'data' and 'fields' need to be provided.")
    
    # 'data' needs to be a list
    if not isinstance(json_dict["data"], list):
        raise ValueError("Invalid migration dictionary. 'data' needs to be a list.")
    
    # More specifically, 'data' needs to be a list of lists which contains a list as a first element and a dictionary as a second element
    if not all([True if isinstance(m[0], list | tuple) and isinstance(m[1], dict) else False for m in json_dict["data"]]):
        raise ValueError("Invalid migration dictionary. 'data' needs to be a list of list which contains a list as a first element and a dictionary as a second element.")
    
    # 'fields' needs to be a list
    if not isinstance(json_dict["fields"], list | tuple):
        raise ValueError("Invalid migration dictionary. 'fields' needs to be a list.")
    
    # 'fields' needs to be a list of strings
    if not all([isinstance(m, str) for m in json_dict["fields"]]):
        raise ValueError("Invalid migration dictionary. 'fields' needs to be a list of strings.")
         
    # Initialize mapping dictionary
    mapping: dict = {}

    # Construct mapping dictionary. Looping through all elements
    for FROM_orig, TO_orig in json_dict["data"]:
        
        # First, we need to adapt the FROMs and the TOs
        # We use ast literal to evaluate the elements and merge them to the corresponding Python type
        # We also check for lists and replace them with the immutable tuple types
        
        # Initialize new variables
        FROM_tuple: tuple = ()
        TO_dict: dict = {}
        
        # Loop through each element from the FROM list
        for x in FROM_orig:
            
            # Evaluate, if possible. Otherwise, use the value as it is right now
            try: xx = ast.literal_eval(x)
            except: xx = x
            
            # Convert list to tuples
            if isinstance(xx, list):
                xx = tuple(xx)
            
            # Append to variable
            FROM_tuple += (xx,)
           
        # Loop through each key/value pair of the TO element
        for y, z in TO_orig.items():
            
            # Evaluate, if possible. Otherwise, use the value as it is right now
            try: yy = ast.literal_eval(y)
            except: yy = y
            
            # Evaluate, if possible. Otherwise, use the value as it is right now
            try: zz = ast.literal_eval(z)
            except: zz = z
            
            # Convert list to tuples
            if isinstance(yy, list):
                yy = tuple(yy)
                
            # Convert list to tuples
            if isinstance(zz, list):
                zz = tuple(zz)
            
            # Append to variable
            TO_dict[yy] = zz
        
        # Append to mapping variable
        mapping[FROM_tuple] = TO_dict
        
    return tuple(json_dict["fields"]), mapping


def apply_migration_mapping(db_var, fields: tuple, migration_mapping: dict):
    
    # Check function input type
    hp.check_function_input_type(apply_migration_mapping, locals())
    
    # Amount fields to which the 'multiplier' should be applied to
    amount_fields_ds = ["production amount", "output amount"]
    amount_fields_exc = ["amount", "loc", "scale", "shape", "minimum", "maximum"]

    # Loop through each element in the current database data (inventory)
    for ds in db_var:
        
        # We try to find a suitable entry of the migration mapping for the current inventory
        # If yes (= no key errors), we replace the values of the current inventory with the ones from the migration mapping
        try:
            # We first extract the respective information of the fields of the current inventory with which we search in the migration mapping
            ds_ID = tuple([ds[m] for m in fields])
            
            # We check if we find a corresponding item in the migration mapping
            map_to = migration_mapping[ds_ID]
            
            # If yes, we loop through each key/value pair
            for ds_k_new, ds_v_new in map_to.items():
                
                # If we find the 'multiplier', it is a bit special
                # It is not a simple replacement, but we rather apply the multiplier to any 'amount' field
                if ds_k_new == "multiplier":
                    
                    # We loop through all the amount fields (specified above) individually
                    for amount_field_ds in amount_fields_ds:
                        
                        # If it exists in the current dictionary, we update the field
                        # We multiply the current value with the multiplier value
                        if amount_field_ds in ds:
                            ds[amount_field_ds] *= ds_v_new
                            
                    continue
                    
                # We replace (or add if not existing) the values of the keys in the inventory
                ds[ds_k_new] = ds_v_new
                
        except:
            # If we encounter a key error, we go on.
            # A key error means, there is either no corresponding item for the current inventory in the migration mapping
            # or some of the fields are not present in the inventory
            pass
        
        # We do the same as above also for the exchanges
        # Loop through each exchange of the current inventory
        for exc in ds["exchanges"]:
            
            # We try to find a suitable entry of the migration mapping for the current exchange
            # If yes (= no key errors), we replace the values of the current exchange with the ones from the migration mapping
            try:
                # We first extract the respective information of the fields of the current exchange with which we search in the migration mapping
                exc_ID = tuple([exc[m] for m in fields])
                
                # We check if we find a corresponding item in the migration mapping
                map_to = migration_mapping[exc_ID]
                
                # If yes, we loop through each key/value pair
                for exc_k_new, exc_v_new in map_to.items():
                    
                    # If we find the 'multiplier', it is a bit special
                    # It is not a simple replacement, but we rather apply the multiplier to any 'amount' field
                    if exc_k_new == "multiplier":
                        
                        # We loop through all the amount fields (specified above) individually
                        for amount_field_exc in amount_fields_exc:
                            
                            # If it exists in the current dictionary, we update the field
                            # We multiply the current value with the multiplier value
                            if amount_field_exc in exc:
                                exc[amount_field_exc] *= exc_v_new
                                
                        # We need to update the negative key
                        if "negative" in exc:
                            exc["negative"] = exc["amount"] < 0
                                
                        continue
                        
                    # We replace (or add if not existing) the values of the keys in the exchange
                    exc[exc_k_new] = exc_v_new
                    
            except:
                # If we encounter a key error, we go on.
                # A key error means, there is either no corresponding item for the current exchange in the migration mapping
                # or some of the fields are not present in the exchange
                pass
            
    return db_var


def migrate_from_json_file(db_var, json_migration_filepath: pathlib.Path | None = None):
    
    # Check function input type
    hp.check_function_input_type(migrate_from_json_file, locals())
    
    # Return the original database if no migration is specified
    if json_migration_filepath is None:
        return db_var
    
    # Opening JSON file
    f = open(json_migration_filepath)
     
    # returns JSON object as 
    # a dictionary
    json_dict: dict = json.load(f)
     
    # Closing file
    f.close()
    
    # Create mapping of the JSON custom migration file
    fields, migration_mapping = create_migration_mapping(json_dict = json_dict)
    
    # Apply migration
    new_db_var = apply_migration_mapping(db_var = db_var,
                                         fields = fields,
                                         migration_mapping = migration_mapping)
            
    return new_db_var


def migrate_from_excel_file(db_var, excel_migration_filepath: pathlib.Path | None = None):
    
    # Check function input type
    hp.check_function_input_type(migrate_from_excel_file, locals())
    
    # Return the original database if no migration is specified
    if excel_migration_filepath is None:
        return db_var
    
    # Read excel file
    excel: pd.DataFrame = pd.read_excel(excel_migration_filepath)
    
    # Convert Excel file to dictionary with the migration structure
    json_dict: dict = create_structured_migration_dictionary_from_excel(excel_dataframe = excel)
    
    # Create mapping of the JSON custom migration file
    fields, migration_mapping = create_migration_mapping(json_dict = json_dict)
    
    # Apply migration
    new_db_var = apply_migration_mapping(db_var = db_var,
                                         fields = fields,
                                         migration_mapping = migration_mapping)
            
    return new_db_var


#%% Import functions

def import_SimaPro_LCI_inventories(SimaPro_CSV_LCI_filepaths: list,
                                   db_name: str,
                                   encoding: str = "latin-1",
                                   delimiter: str = "\t",
                                   verbose: bool = True,
                                   ) -> bw2io.importers.base_lci.LCIImporter:
    
    # Make variable check
    hp.check_function_input_type(import_SimaPro_LCI_inventories, locals())
    
    # Check if all list elements are of type pathlib.Path
    not_pathlib_object: list = [m for m in SimaPro_CSV_LCI_filepaths if not isinstance(m, pathlib.Path)]
    if not_pathlib_object != []:
        raise ValueError("Function input variable 'paths_to_SimaPro_CSV_LCI_files' only accepts a list with elements of type pathlib.Path.")
    
    # # ... filenames of CSV's which should be imported
    # list_of_SimaPro_inventory_CSV_filepaths: list[pathlib.Path] = [m for m in paths_to_SimaPro_CSV_LCI_files.iterdir() if m.suffix.lower() == ".csv"]
    
    # if list_of_SimaPro_inventory_CSV_filepaths == []:
    #     raise ValueError("No SimaPro LCI files found in path:\n{}".format(list_of_SimaPro_inventory_CSV_filepaths))
    
    # Import data from SimaPro CSV file using Brightway function
    if verbose:
        print(starting + "Import inventories from SimaPro CSV:")
    
    # Create new list to store all inventory dicts
    db: list[dict] = []
    
    # Loop through each file individually and use Brightway importer to read data
    for filepath in SimaPro_CSV_LCI_filepaths:
        db += copy.deepcopy(bw2io.importers.simapro_csv.SimaProCSVImporter(str(filepath), db_name, delimiter))
    
    # ... make sure that all categories fields of all biosphere flows are of type tuple    
    db: list[dict] = ensure_categories_are_tuples(db)
    
    # ... we remove the second category (= sub_category) of the categories field if it is unspecified
    db: list[dict] = bw2io.strategies.biosphere.drop_unspecified_subcategories(db)
    
    # ... SimaPro contains multioutput inventories. Brightway can only handle one output per inventory.
    # if an inventory contains multiple outputs, we copy the inventory and create duplicated ones for each output. We adjust the output and allocation factors accordingly.
    db: list[dict] = bw2io.strategies.simapro.sp_allocate_products(db)
    
    # ... Output products which have an allocation factor of 0 have no impact. 
    # it is no harm to keep them. However, it is more convenient to exclude them because they are of no relevance for us either. So we remove all which have 0 allocation.
    db: list[dict] = bw2io.strategies.simapro.fix_zero_allocation_products(db)
    
    # ... add more fields to each inventory dictionary --> we want to keep the SimaPro standard
    db: list[dict] = create_SimaPro_fields(db, for_ds = True, for_exchanges = True)
    
    # ecoinvent per se uses different names for categories and units
    # units are also transformed
    # if specified beforehand, certain import strategies are applied which normalize the current information of the exchange flows to ecoinvent standard
    db: list[dict] = normalize_simapro_biosphere_categories(db)
        
    # ... we do the same as for the normalization of the categories also for the normalization of units
    # this means, we use the brightway mapping to normalize 'kg' to 'kilogram' for instance
    db: list[dict] = bw2io.strategies.generic.normalize_units(db)
        
    # ... at the end, we try to transform all units to a common standard, if possible
    # 'kilowatt hour' and 'kilojoule' are for example both transformed to 'megajoule' using a respective factor for transformation
    db: list[dict] = transformation_units(db)
    
    # ... SimaPro contains regionalized flows. But: the country is only specified within the name of a flow. That is inconvenient
    # we extract the country/region of a flow (if there is any) from the name and write this information to a separate field 'location'
    # flows where no region is specified obtain the location 'GLO'
    select_GLO_in_name_valid_for_method_import = False
    db: list[dict] = add_location_to_biosphere_exchanges(db,
                                                         select_GLO_in_name_valid_for_method_import = select_GLO_in_name_valid_for_method_import)
    
    # ... with SimaPro, we need to link by top and sub category individually for biosphere flows
    # this is not possible, if we have only one field which combines both categories
    # Therefore, we write the categories field into two separate fields, one for top category and one for sub category
    db: list[dict] = add_top_and_subcategory_fields_for_biosphere_flows(db)
    
    # ... SimaPro has no specific field to specify the location of an inventory. It is usually included in the name of an inventory. This is inconvenient.
    # for all inventories and exchange flows, names are checked with regex patterns if they contain a region. If yes, it is extracted as best as possible.
    # if it can not be extracted, it is specified as 'not identified'
    # the name is updated: a placeholder '{COUNTRY}' is put where the location originally has been placed.
    db: list[dict] = extract_geography_from_SimaPro_name(db)
    
    # ... set a UUID for each inventory based on the Brightway strategy
    db: list[dict] = set_code(db,
                              fields = ("name", "unit", "location"),
                              overwrite = True,
                              strip = True,
                              case_insensitive = True,
                              remove_special_characters = False)
    
    # ... the flows of the SimaPro category 'Final waste flows' is of no use for us.
    # we can therefore remove it
    db: list[dict] = drop_final_waste_flows(db)
    
    # ... SimaPro has an own classification for inventories. This classification is helpful to group inventories, search and filter them.
    # however, this category is hidden in the production exchange, where it is not really accessible for us.
    # we therefore extract it from the production exchange and write it to the inventory.
    db: list[dict] = add_SimaPro_classification(db)
    
    # ... SimaPro assigns each technosphere inventory into a category (category type). This information is especially needed, when we want to export inventories back again into SimaPro.
    # we therefore write the SimaPro categories to a specific field in the inventory, so that we can easily access it.
    db: list[dict] = add_SimaPro_categories_and_category_type(db)
    
    # ... the allocation amount is only found in the production exchange
    # this is inconvenient, especially if we want to use it later. We therefore write it as an additional field to the inventory dictionary
    db: list[dict] = add_allocation_field(db)
    
    # ... the output amount is only found in the production exchange
    # this is inconvenient, especially if we want to use it later. We therefore write it as an additional field to the inventory dictionary
    # NOTE: to get to the original amount, we need to multiply the production amount with the allocation factor. We also need to do that after having transformed units. Otherwise, the unit of other fields is adapted but not the output amount, which leads to a wronge value for the output amount.
    db: list[dict] = add_output_amount_field(db)
    
    # ... we can not write duplicates and therefore need to remove them before
    # we can specify the fields which should be used to identify the duplicates
    db: list[dict] = remove_duplicates(db,
                                       fields = ("name", "unit", "location"),
                                       strip = True,
                                       case_insensitive = True,
                                       remove_special_characters = False)
        
    # ... exchanges that have an amount of 0 will not contribute to the environmental impacts
    # we can therefore remove them
    db: list[dict] = remove_exchanges_with_zero_amount(db)
    
    # Apply internal linking of activities
    db: list[dict] = linking.link_activities_internally(db,
                                                        production_exchanges = True,
                                                        substitution_exchanges = True,
                                                        technosphere_exchanges = True,
                                                        relink = False,
                                                        strip = True,
                                                        case_insensitive = True,
                                                        remove_special_characters = False,
                                                        verbose = verbose)
    
    # As brightway importer object    
    db_as_obj: bw2io.importers.base_lci.LCIImporter = bw2io.importers.base_lci.LCIImporter(db_name)
    db_as_obj.data: list[dict] = db
    
    return db_as_obj



# # !!! TODO
# #%% Function to import the ecoinvent database from XML files
# def ecoinvent_XML_database_import_from_raw_data(Brightway_project_name: str,
#                                                 ecoinvent_XML_filepath: pathlib.Path,
#                                                 ecoinvent_database_name: str,
#                                                 ecoinvent_database_name_abbreviated: str,
#                                                 JSON_LCI_filepath: pathlib.Path,
#                                                 JSON_migration_biosphere_filepath: (None | pathlib.Path),
#                                                 import_regionalized_database_version: bool = True,
#                                                 BRIGHTWAY2_DIR: pathlib.Path = PATH_VAR["BRIGHTWAY2_DIR"],
#                                                 verbose: bool = True):
    
#     # Make variable check
#     hp.check_function_input_type(ecoinvent_XML_database_import_from_raw_data, locals())
    
#     # Function to add 'GLO' to biosphere exchanges
#     # We need to do that in order to be consistent with SimaPro flows. As a default, ecoinvent only uses 'GLO' flows
#     def add_GLO_to_biosphere_exchanges(db_var):
#         for ds in db_var:
#             for exc in ds["exchanges"]:
#                 if exc["type"] == "biosphere":
#                     exc["location"] = "GLO"
#         return db_var
    
#     # Change project path
#     utils.change_brightway_project_directory(BRIGHTWAY2_DIR)
    
#     # Open Brightway2 project
#     bw2data.projects.set_current(Brightway_project_name)
    
#     # Print statement
#     if verbose:
#         print(starting + "Data is imported into project:\n" + Brightway_project_name + "\n")
    
#     # Delete databases, if they exist
#     # ... 'ecoinvent_database_name'
#     if ecoinvent_database_name in bw2data.databases:
        
#         # Print message to console which database is deleted
#         if verbose:
#             print(starting + "Delete database: " + ecoinvent_database_name)
        
#         # Delete database
#         del bw2data.databases[ecoinvent_database_name]
        
#         # Add line to console
#         if verbose:
#             print("")
            
#     # Use Brightway importer to import XML files
#     if verbose:
#         print(starting + "Import database from XML: " + ecoinvent_database_name)
#     ecoinvent_db = bw2io.SingleOutputEcospold2Importer(str(ecoinvent_XML_filepath), ecoinvent_database_name, use_mp = False)

#     # Apply all Brightway strategies
#     ecoinvent_db.apply_strategies(verbose = verbose)

#     # Apply custom strategies
#     ecoinvent_db.apply_strategy(strategies.XML_strategies.assign_flow_field_as_code, verbose = verbose)
#     ecoinvent_db.apply_strategy(strategies.XML_strategies.assign_categories_from_XML_to_biosphere_flows, verbose = verbose)
#     ecoinvent_db.apply_strategy(strategies.XML_strategies.modify_fields_to_SimaPro_standard, verbose = verbose)
#     ecoinvent_db.apply_strategy(add_GLO_to_biosphere_exchanges, verbose = verbose)
    
#     # Apply biosphere migration
#     ecoinvent_db.apply_strategy(partial(strategies.migration_strategies.migrate_from_JSON_file,
#                                         JSON_migration_filepath = JSON_migration_biosphere_filepath), verbose = verbose)
    
#     ecoinvent_db.apply_strategy(partial(linking.remove_linking,
#                                         production_exchanges = True,
#                                         substitution_exchanges = True,
#                                         technosphere_exchanges = True,
#                                         biosphere_exchanges = True), verbose = verbose)
    
#     # Apply internal linking of activities
#     ecoinvent_db.apply_strategy(partial(linking.link_activities_internally,
#                                         production_exchanges = True,
#                                         substitution_exchanges = True,
#                                         technosphere_exchanges = True,
#                                         relink = False,
#                                         verbose = verbose), verbose = verbose)
    
#     # Apply external linking of biosphere flows
#     ecoinvent_db.apply_strategy(partial(linking.link_biosphere_flows_externally,
#                                         biosphere_connected_to_methods = True,
#                                         biosphere_NOT_connected_to_methods = False,
#                                         relink = False,
#                                         verbose = verbose), verbose = verbose)
    
#     # Write unlinked biosphere flows to XLSX
#     if verbose:
#         print("\n" + starting + "Write unlinked flows")
#     ecoinvent_db.write_excel(only_unlinked = True)
    
#     # Show statistic of current linking of database import
#     if verbose:
#         print("\n" + starting + "Linking statistics of current database import")
#     ecoinvent_db.statistics()
    
#     # Add line to console
#     if verbose:
#         print("")    
    
#     # Make a new biosphere database for the flows which are currently not linked
#     # Add unlinked biosphere flows with a custom function
#     unlinked_biosphere_flows_final = utils.add_unlinked_flows_to_biosphere_database(ecoinvent_db,
#                                                                                     verbose = verbose)
    
#     # Show statistic of current linking of database import
#     if verbose:
#         print("\n" + starting + "Linking statistics of current database import")
#     ecoinvent_db.statistics()
    
#     # Write database
#     if verbose:
#         print(starting + "Write database: " + ecoinvent_database_name)
#     ecoinvent_db.write_database()
    
#     # Print empty row
#     if verbose:
#         print("")
        
#     # If regionalized database version should be imported, do so
#     if import_regionalized_database_version:
        
#         # Print statements
#         if verbose:
#             print(starting + "Import regionalized database version")
#             print(starting + "Apply strategies")
        
#         # Create a new LCI importer object for the regionalized database
#         ecoinvent_db_reg = bw2io.importers.base_lci.LCIImporter(copy.deepcopy(ecoinvent_database_name + REG_ADDITIONAL_NAME_FRAGMENT))
        
#         # Add the original inventory data from before to the LCI importer object
#         ecoinvent_db_reg.data = [m for m in copy.deepcopy(ecoinvent_db)]
        
#         # Apply strategy to remove certain parameters
#         ecoinvent_db_reg.apply_strategy(partial(strategies.mixed_strategies.eliminate_fields,
#                                                 ds_fields = ["database", "input", "output"],
#                                                 exc_fields = ["database", "input", "output"]), verbose = verbose)
        
#         # Apply strategy to change the name of the database parameter and to remove the linking (input fields)
#         ecoinvent_db_reg.apply_strategy(partial(change_database_name_and_remove_code,
#                                                 new_database_name = ecoinvent_database_name + REG_ADDITIONAL_NAME_FRAGMENT),
#                                         verbose = verbose)
        
#         # Set new codes
#         ecoinvent_db_reg.apply_strategy(partial(strategies.inventory_strategies.set_code,
#                                                 fields = TECHNOSPHERE_FIELDS,
#                                                 overwrite = True,
#                                                 strip = STRIP,
#                                                 case_insensitive = CASE_INSENSITIVE,
#                                                 remove_special_characters = REMOVE_SPECIAL_CHARACTERS), verbose = verbose)
        
#         # Apply internal linking of activities
#         ecoinvent_db_reg.apply_strategy(partial(linking.link_activities_internally,
#                                                 production_exchanges = True,
#                                                 substitution_exchanges = True,
#                                                 technosphere_exchanges = True,
#                                                 relink = False,
#                                                 verbose = verbose), verbose = verbose)
        
#         # Apply external linking of biosphere flows
#         ecoinvent_db_reg.apply_strategy(partial(linking.link_biosphere_flows_externally,
#                                                 biosphere_connected_to_methods = True,
#                                                 biosphere_NOT_connected_to_methods = True,
#                                                 relink = False,
#                                                 verbose = verbose), verbose = verbose)
        
#         # Show statistic of current linking of regionalized database import
#         if verbose:
#             print("\n" + starting + "Linking statistics of current regionalized database import")
#         ecoinvent_db_reg.statistics()
        
#         # Write database
#         print(starting + "Write database: " + ecoinvent_database_name + REG_ADDITIONAL_NAME_FRAGMENT)
#         ecoinvent_db_reg.write_database()
        
#         # Print empty row
#         if verbose:
#             print("")
            
#     # Specify, whether a JSON object of the Brightway database should be written
#     if write_JSON_file:
        
#         # Make variable of the whole database as dictionary where keys are the number of an inventory
#         database_as_dict = {str(idx): m for idx, m in enumerate([m for m in ecoinvent_db])}
        
#         # Create the JSON object to be written
#         json_database_object = json.dumps(database_as_dict, indent = 4)
        
#         # Write the database dictionary to a JSON file
#         if verbose:
#             print(starting + "Write JSON file: '" + str(ecoinvent_database_name_abbreviated + "_" + XML_FILENAME_FRAGMENT) + "'.json")
#         with open(JSON_LCI_filepath / (ecoinvent_database_name_abbreviated + "_" + XML_FILENAME_FRAGMENT + ".json"), "w") as outfile:
#             outfile.write(json_database_object)
        
#         # The JSON variable can be very big. Once the file has been written, we now remove the variable again
#         del database_as_dict, json_database_object
        
#         # Extract the unlinked biosphere data used in the database to JSON
#         # Rename keys
#         database_as_dict_bio = {str(idx): m[1] for idx, m in enumerate(unlinked_biosphere_flows_final.items())}

#         # Create the JSON object to be written
#         json_database_object_bio = json.dumps(database_as_dict_bio, indent = 4)
        
#         # Write the unlinked biosphere data dictionary to a JSON file
#         if verbose:
#             print(starting + "Write JSON file: '" + BIOSPHERE_NAME_UNLINKED_ABB + "'.json")
#         with open(JSON_LCI_filepath / (BIOSPHERE_NAME_UNLINKED_ABB + ".json"), "w") as outfile:
#             outfile.write(json_database_object_bio)
        
#         # Write only if regionalized database version was imported
#         if import_regionalized_database_version:
            
#             # Make variable of the whole database as dictionary where keys are the number of an inventory
#             database_as_dict_reg = {str(idx): m for idx, m in enumerate([m for m in ecoinvent_db_reg])}

#             # Create the JSON object to be written
#             json_database_object_reg = json.dumps(database_as_dict_reg, indent = 4)
            
#             # Write the regionalized database dictionary to a JSON file
#             if verbose:
#                 print(starting + "Write JSON file: '" + str(ecoinvent_database_name_abbreviated + REG_ADDITIONAL_NAME_FRAGMENT_ABB + "_" + XML_FILENAME_FRAGMENT) + "'.json")
#             with open(JSON_LCI_filepath / (ecoinvent_database_name_abbreviated + REG_ADDITIONAL_NAME_FRAGMENT_ABB + "_" + XML_FILENAME_FRAGMENT + ".json"), "w") as outfile:
#                 outfile.write(json_database_object_reg)
            
#             # The JSON variable can be very big. Once the file has been written, we now remove the variable again
#             del database_as_dict_reg, json_database_object_reg
            
#             # Print empty row
#             if verbose:
#                 print("")
    




