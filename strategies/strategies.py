
import pathlib

if __name__ == "__main__":
    import os
    os.chdir(pathlib.Path(__file__).parent.parent)
    
import os
import re
import ast
import json
import copy
import bw2io
import bw2data
import hashlib
import pathlib
import datetime
import itertools
import pandas as pd

here = pathlib.Path(__file__).parent
os.chdir(here)
from variables import (use_ecoinvent_standard,
                       BIOSPHERE_NAME,
                       STRIP,
                       CASE_INSENSITIVE,
                       REMOVE_SPECIAL_CHARACTERS,
                       TECHNOSPHERE_FIELDS,
                       PATH_VAR,
                       FILE_VAR,
                       SIMAPRO_BIO_TOPCATEGORIES_MAPPING,
                       SIMAPRO_BIO_TOPCATEGORIES_LCI_MAPPING,
                       SIMAPRO_BIO_SUBCATEGORIES_MAPPING,
                       SIMAPRO_CATEGORY_TYPE_COMPARTMENT_MAPPING,
                       SIMAPRO_TECHNOSPHERE_COMPARTMENTS,
                       SIMAPRO_PRODUCT_COMPARTMENTS,
                       XML_FILEFOLDER_PATH,
                       XML_MODEL_TYPE,
                       XML_PROCESS_TYPE,
                       XML_TO_SIMAPRO_MODEL_TYPE_MAPPING,
                       XML_TO_SIMAPRO_PROCESS_TYPE_MAPPING)

import helper as hp

starting: str = "------------"

# Create backward unit mappings
# ... import from Brightway2
unit_transformation_mapping = {m[0]: {"unit_transformed": m[1], "multiplier": m[2]} for m in bw2io.units.DEFAULT_UNITS_CONVERSION}
backward_unit_normalization_mapping_orig = {v: k for k, v in bw2io.units.UNITS_NORMALIZATION.items()}

# Add custom normalizations
additional_normalization_mapping = {"guest night": "day"}

# Merge
backward_unit_normalization_mapping = dict(**backward_unit_normalization_mapping_orig, **additional_normalization_mapping)

# Create a backward category mapping
BACKWARD_SIMAPRO_BIO_SUBCATEGORIES_MAPPING = {v.lower(): k for k, v in SIMAPRO_BIO_SUBCATEGORIES_MAPPING.items()}
BACKWARD_SIMAPRO_BIO_TOPCATEGORIES_MAPPING = {v.lower(): k for k, v in SIMAPRO_BIO_TOPCATEGORIES_LCI_MAPPING.items()}


#%%

# Master strategies combine other strategies and summarize them to be easily applicable
class MASTER():
    
    # Summary of all relevant strategies that need to be applied for the import of inventory data from SimaPro CSV files
    def SimaPro_LCI_import_strategies(db_var,
                                      use_ecoinvent_standard: bool = use_ecoinvent_standard,
                                      strategy_ensure_categories_are_tuples: bool = True,
                                      strategy_drop_unspecified_subcategories: bool = True,
                                      strategy_sp_allocate_products: bool = True,
                                      strategy_fix_zero_allocation_products: bool = True,
                                      strategy_create_SimaPro_fields: bool = True,
                                      strategy_normalize_simapro_biosphere_categories: bool = True,
                                      strategy_normalize_units: bool = True,
                                      strategy_transformation_units: bool = True,
                                      strategy_cas_number_addition: bool = True,
                                      strategy_add_location_to_biosphere_exchanges: bool = True,
                                      strategy_add_top_and_subcategory_fields_for_biosphere_flows: bool = True,
                                      strategy_extract_geography_from_SimaPro_name: bool = True,
                                      strategy_set_code: bool = True,
                                      strategy_drop_final_waste_flows: bool = True,
                                      strategy_add_SimaPro_classification: bool = True,
                                      strategy_add_SimaPro_categories_and_category_type: bool = True,
                                      strategy_add_allocation_field: bool = True,
                                      strategy_add_output_amount: bool = True,
                                      strategy_remove_exchanges_with_zero_amount: bool = True,
                                      strategy_remove_duplicates: bool = True,
                                      verbose: bool = True,
                                      **kwargs: dict):

        # Make variable check
        hp.check_function_input_type(MASTER.SimaPro_LCI_import_strategies, locals())
        
        # Print statement
        if verbose:
            print("Applying strategy: SimaPro_LCI_import_strategies")
        
        # ... make sure that all categories fields of all biosphere flows are of type tuple    
        if strategy_ensure_categories_are_tuples:
            
            # Print statement
            if verbose:
                print("  - ensure_categories_are_tuples")
                
            db_var = mixed_strategies.ensure_categories_are_tuples(db_var)
        
        # ... we remove the second category (= sub_category) of the categories field if it is unspecified
        if strategy_drop_unspecified_subcategories:
            
            # Print statement
            if verbose:
                print("  - drop_unspecified_subcategories")
                
            db_var = bw2io.strategies.biosphere.drop_unspecified_subcategories(db_var)
        
        # ... SimaPro contains multioutput inventories. Brightway can only handle one output per inventory.
        # if an inventory contains multiple outputs, we copy the inventory and create duplicated ones for each output. We adjust the output and allocation factors accordingly.
        if strategy_sp_allocate_products:
            
            # Print statement
            if verbose:
                print("  - sp_allocate_products")
                
            db_var = bw2io.strategies.simapro.sp_allocate_products(db_var)
        
        # ... Output products which have an allocation factor of 0 have no impact. 
        # it is no harm to keep them. However, it is more convenient to exclude them because they are of no relevance for us either. So we remove all which have 0 allocation.
        if strategy_fix_zero_allocation_products:
            
            # Print statement
            if verbose:
                print("  - fix_zero_allocation_products")
            
            db_var = bw2io.strategies.simapro.fix_zero_allocation_products(db_var)
        
        # ... add more fields to each inventory dictionary --> we want to keep the SimaPro standard
        if strategy_create_SimaPro_fields:
            
            # Print statement
            if verbose:
                print("  - create_SimaPro_fields")
                
            db_var = mixed_strategies.create_SimaPro_fields(db_var, for_ds = True, for_exchanges = True)
        
        # ecoinvent per se uses different names for categories and units
        # units are also transformed
        # if specified beforehand, certain import strategies are applied which normalize the current information of the exchange flows to ecoinvent standard
        if use_ecoinvent_standard:
            
            # ... we use the new ecoinvent names for the categories. Brightway specifies a mapping for that
            if strategy_normalize_simapro_biosphere_categories:
                
                # Print statement
                if verbose:
                    print("  - normalize_simapro_biosphere_categories")
                    
                db_var = biosphere_strategies.normalize_simapro_biosphere_categories(db_var)
            
            # ... we do the same as for the normalization of the categories also for the normalization of units
            # this means, we use the brightway mapping to normalize 'kg' to 'kilogram' for instance
            if strategy_normalize_units:
                
                # Print statement
                if verbose:
                    print("  - normalize_units")
                    
                db_var = bw2io.strategies.generic.normalize_units(db_var)
            
            # ... at the end, we try to transform all units to a common standard, if possible
            # 'kilowatt hour' and 'kilojoule' are for example both transformed to 'megajoule' using a respective factor for transformation
            if strategy_transformation_units:
                
                # Print statement
                if verbose:
                    print("  - transformation_units")
                    
                db_var = mixed_strategies.transformation_units(db_var)
        
        # ... for some biosphere flows, there is no CAS-Nr. specified. If possible, try to add the respective CAS-Nr. of that flow via a mapping file
        if strategy_cas_number_addition:
            
            # Print statement
            if verbose:
                print("  - cas_number_addition")
                
            db_var = biosphere_strategies.cas_number_addition(db_var)
        
        # ... SimaPro contains regionalized flows. But: the country is only specified within the name of a flow. That is inconvenient
        # we extract the country/region of a flow (if there is any) from the name and write this information to a separate field 'location'
        # flows where no region is specified obtain the location 'GLO'
        if strategy_add_location_to_biosphere_exchanges:
            
            # Print statement
            if verbose:
                print("  - add_location_to_bioshpere_exchanges")
                
            select_GLO_in_name_valid_for_method_import = False
            db_var = biosphere_strategies.add_location_to_biosphere_exchanges(db_var,
                                                                              select_GLO_in_name_valid_for_method_import = select_GLO_in_name_valid_for_method_import)
        
        # ... with SimaPro, we need to link by top and sub category individually for biosphere flows
        # this is not possible, if we have only one field which combines both categories
        # Therefore, we write the categories field into two separate fields, one for top category and one for sub category
        if strategy_add_top_and_subcategory_fields_for_biosphere_flows:
            
            # Print statement
            if verbose:
                print("  - add_top_and_subcategory_fields_for_biosphere_flows")
                
            db_var = biosphere_strategies.add_top_and_subcategory_fields_for_biosphere_flows(db_var)
        
        # ... SimaPro has no specific field to specify the location of an inventory. It is usually included in the name of an inventory. This is inconvenient.
        # for all inventories and exchange flows, names are checked with regex patterns if they contain a region. If yes, it is extracted as best as possible.
        # if it can not be extracted, it is specified as 'not identified'
        # the name is updated: a placeholder '{COUNTRY}' is put where the location originally has been placed.
        if strategy_extract_geography_from_SimaPro_name:
            
            # Print statement
            if verbose:
                print("  - extract_geography_from_SimaPro_name")
                
            db_var = technosphere_strategies.extract_geography_from_SimaPro_name(db_var)
        
        # ... set a UUID for each inventory based on the Brightway strategy
        if strategy_set_code:
            
            # Print statement
            if verbose:
                print("  - set_code")
                
            db_var = inventory_strategies.set_code(db_var,
                                                   fields = TECHNOSPHERE_FIELDS,
                                                   overwrite = True,
                                                   strip = STRIP,
                                                   case_insensitive = CASE_INSENSITIVE,
                                                   remove_special_characters = REMOVE_SPECIAL_CHARACTERS)
        
        # ... the flows of the SimaPro category 'Final waste flows' is of no use for us.
        # we can therefore remove it
        if strategy_drop_final_waste_flows:
            
            # Print statement
            if verbose:
                print("  - drop_final_waste_flows")
                
            db_var = technosphere_strategies.drop_final_waste_flows(db_var)
        
        # ... SimaPro has an own classification for inventories. This classification is helpful to group inventories, search and filter them.
        # however, this category is hidden in the production exchange, where it is not really accessible for us.
        # we therefore extract it from the production exchange and write it to the inventory.
        if strategy_add_SimaPro_classification:
            
            # Print statement
            if verbose:
                print("  - add_SimaPro_classification")
                
            db_var = inventory_strategies.add_SimaPro_classification(db_var)
        
        # ... SimaPro assigns each technosphere inventory into a category (category type). This information is especially needed, when we want to export inventories back again into SimaPro.
        # we therefore write the SimaPro categories to a specific field in the inventory, so that we can easily access it.
        if strategy_add_SimaPro_categories_and_category_type:
            
            # Print statement
            if verbose:
                print("  - add_SimaPro_categories_and_category_type")
                
            db_var = inventory_strategies.add_SimaPro_categories_and_category_type(db_var)
        
        # ... the allocation amount is only found in the production exchange
        # this is inconvenient, especially if we want to use it later. We therefore write it as an additional field to the inventory dictionary
        if strategy_add_allocation_field:
            
            # Print statement
            if verbose:
                print("  - add_allocation_field")
                
            db_var = inventory_strategies.add_allocation_field(db_var)
        
        # ... the output amount is only found in the production exchange
        # this is inconvenient, especially if we want to use it later. We therefore write it as an additional field to the inventory dictionary
        # NOTE: to get to the original amount, we need to multiply the production amount with the allocation factor. We also need to do that after having transformed units. Otherwise, the unit of other fields is adapted but not the output amount, which leads to a wronge value for the output amount.
        if strategy_add_output_amount:
            
            # Print statement
            if verbose:
                print("  - add_output_amount")
                
            db_var = inventory_strategies.add_output_amount_field(db_var)
        
        # ... we can not write duplicates and therefore need to remove them before
        # we can specify the fields which should be used to identify the duplicates
        if strategy_remove_duplicates:
            
            # Print statement
            if verbose:
                print("  - remove_duplicates")
                
            db_var = inventory_strategies.remove_duplicates(db_var,
                                                            fields = TECHNOSPHERE_FIELDS,
                                                            strip = STRIP,
                                                            case_insensitive = CASE_INSENSITIVE,
                                                            remove_special_characters = REMOVE_SPECIAL_CHARACTERS)
            
        # ... exchanges that have an amount of 0 will not contribute to the environmental impacts
        # we can therefore remove them
        if strategy_remove_exchanges_with_zero_amount:
            
            # Print statement
            if verbose:
                print("  - remove_exchanges_with_zero_amount")
                
            db_var = exchange_strategies.remove_exchanges_with_zero_amount(db_var)
        
        return db_var

# Mixed in the sense that strategies modify both, inventory and exchange dictionaries
class mixed_strategies():
    
    # Convert categories value to a tuple
    def ensure_categories_are_tuples(db_var):
        
        # Make variable check
        hp.check_function_input_type(mixed_strategies.ensure_categories_are_tuples, locals())
        
        # Loop through each inventory
        for ds in db_var:
            
            # Transform the value of the key 'categories' from the current inventory into a tuple
            if "categories" in ds and not isinstance(ds["categories"], tuple):
                ds["categories"] = tuple(ds["categories"])
                
            # Loop through each exchange
            for exc in ds["exchanges"]:
                
                # Transform the value of the key 'categories' from the current exchange into a tuple
                if "categories" in exc and not isinstance(exc["categories"], tuple):
                    exc["categories"] = tuple(exc["categories"])
                    
        return db_var
    
    # Specify which dictionary key/value pairs should be removed.
    def eliminate_fields(db_var,
                         ds_fields: list | None,
                         exc_fields: list | None):
        
        # Make variable check
        hp.check_function_input_type(mixed_strategies.eliminate_fields, locals())
        
        # Loop through each inventory
        for ds in db_var:
            
            # Check if ds_fields was specified
            if ds_fields is not None:
                
                # Loop through each inventory parameter listed
                for ds_field in ds_fields:
                
                    # Try to remove the database parameter in the inventory, if possible
                    try: del ds[ds_field]
                    except: pass
            
            # Check if exc_fields was specified
            if exc_fields is not None:
                
                # Loop through each exchange of the inventory
                for exc in ds["exchanges"]:
                
                    # Loop through each exchange parameter listed
                    for exc_field in exc_fields:
                    
                        # Try to remove the exchange parameters, if possible
                        try: del exc[exc_field]
                        except: pass

        return db_var
    
    
    def create_SimaPro_fields(db_var, for_ds: bool, for_exchanges: bool):
        
        for ds in db_var:
            
            if for_ds:
                
                if "SimaPro_name" not in ds:
                    if "name" in ds: # !!! Added but does it make sense?
                        ds["SimaPro_name"] = ds["name"]
                    
                if "SimaPro_categories" not in ds:
                    ds["SimaPro_categories"] = None

                if "SimaPro_unit" not in ds:
                    if "unit" in ds: # !!! Added but does it make sense?
                        ds["SimaPro_unit"] = ds["unit"]
            
            for exc in ds["exchanges"]:
                
                if for_exchanges:
                    
                    if "SimaPro_name" not in exc:
                        if "name" in exc: # !!! Added but does it make sense?
                            exc["SimaPro_name"] = exc["name"]
                        
                    if "SimaPro_categories" not in exc:
                        if "categories" in exc: # !!! Added but does it make sense?
                            exc["SimaPro_categories"] = exc["categories"]
                        
                    if "SimaPro_unit" not in exc:
                        if "unit" in exc: # !!! Added but does it make sense?
                            exc["SimaPro_unit"] = exc["unit"]
                
        return db_var
    
    
    
    
    
    def transformation_units(db_var):
         
        # Loop through each inventory
        for ds in db_var:
            
            # If unit and production amount are provided as parameters in the current inventory, we can go on and try to convert the unit, if needed
            if "unit" in ds and "production amount" in ds:
                
                # Lookup the new unit based on the information of the old unit
                unit_transformed_ds = unit_transformation_mapping.get(ds["unit"])
                
                # If a unit transformation has been found, write new data
                if unit_transformed_ds is not None:
                    
                    # Replace the current with the new unit
                    ds["unit"] = unit_transformed_ds["unit_transformed"]
                    
                    # Adapt the current value to a new value based on the multiplication factor provided
                    ds["production amount"] *= unit_transformed_ds["multiplier"] 
                    
                    # If we transform the current unit, and there is also the field 'SimaPro_unit' present
                    # we must also transform that field
                    if "SimaPro_unit" in ds:
                        
                        # We try to backward normalize the newly transformed name
                        try:
                            ds["SimaPro_unit"] = backward_unit_normalization_mapping[ds["unit"]]
                            
                        except:
                            # If we fail, we need to raise an error
                            raise ValueError("Inventory unit was transformed using the 'transformation_units' strategy BUT the field 'SimaPro_unit' (" + str(ds["SimaPro_unit"]) + ") could not be adjusted properly for the current inventory (backward mapped).")
            
            # Loop through each exchange and do the same as for the inventories
            for exc in ds.get("exchanges", []):
                
                if "unit" in exc and "amount" in exc:
                    
                    unit_transformed_exc = unit_transformation_mapping.get(exc["unit"])
                    
                    if unit_transformed_exc is not None:
                        exc["unit"] = unit_transformed_exc["unit_transformed"]
                        exc["amount"] *= unit_transformed_exc["multiplier"] 
                        
                        # Update negative field
                        if "negative" in exc:
                            exc["negative"] = exc["amount"] < 0 # !!! NEWLY ADDED !!!
                        
                        # Transform all relevant uncertainty fields
                        exc |= {n: exc[n] * unit_transformed_exc["multiplier"]  for n in ["loc", "shape", "minimum", "maximum"] if n in exc.copy()} # !!! NEWLY ADDED !!!
                        
                        # If we transform the current unit, and there is also the field 'SimaPro_unit' present
                        # we must also transform that field
                        if "SimaPro_unit" in exc:
                            
                            # We try to backward normalize the newly transformed name
                            try:
                                exc["SimaPro_unit"] = backward_unit_normalization_mapping[exc["unit"]]
                                
                            except:
                                # If we fail, we need to raise an error
                                raise ValueError("Exchange unit was transformed using the 'transformation_units' strategy BUT the field 'SimaPro_unit' (" + str(exc["SimaPro_unit"]) + ") could not be adjusted properly for the current exchange (backward mapped).")
                
                        
        return db_var
    

#%%
class inventory_strategies():
    
    
    def select_inventory_using_regex(db_var, exclude: bool, include: bool, patterns: list, case_sensitive: bool = True):
        
        # Check function input type
        hp.check_function_input_type(inventory_strategies.select_inventory_using_regex, locals())
        
        # Raise error if exlude and include was both specified. This is not possible
        if exclude and include:
            raise ValueError("Parameters 'include' and 'exclude' can not be True at the same time. It is only possible to either exlude or include.")
        
        # If exlude nor include was specified, just return the database in its original state
        if not exclude and not include:
            return db_var
        
        # We also return the database if no patterns were specified, because selecting does not make sense without a pattern
        if patterns == []:
            return db_var
        
        # Merge the different patterns to a string
        pattern_string = "|".join([str(m) for m in patterns]).lower()
        
        # Initialize a new list to store the new inventories to
        new_db_var = []
        
        # Loop through each inventory and select whether to in- or exlude it based on patterns
        for ds in db_var:
            
            # Check if the pattern is contained in the SimaPro name
            contained = bool(re.search(pattern_string, ds["SimaPro_name"].lower()))
            
            # Remove, if specified and the pattern is contained
            if contained and exclude:
                continue
            
            # Otherwise, if the pattern is not contained, we keep the inventory (by adding it to the new list)
            elif not contained and exclude:
                new_db_var += [ds]
                continue
            
            # If the pattern is contained and we want to include the pattern, write the dataset to the new list
            elif contained and include:
                new_db_var += [ds]
                continue
                
        return new_db_var
    
    
    
    def remove_duplicates(db_var,
                          fields: tuple | list,
                          strip: bool = True,
                          case_insensitive: bool = True,
                          remove_special_characters: bool = False):
        
        # Make variable check
        hp.check_function_input_type(inventory_strategies.remove_duplicates, locals())
        
        # Initialize a variable to store all elements already found
        container = {}
        
        # Initialize a variable to store the new list without the duplicates
        new_db_var = []
        
        # Initialize variable to store the amount of missing fields from the inventories
        missing_fields = {m: 0 for m in fields}
        missing_fields_error = False
        
        # Loop through each inventory
        for ds in db_var:
            
            # Loop through each relevant field (previously specified)
            for field in fields:
                
                # If field is missing, write to variable -> will raise error in the end
                # Reason: we can only remove duplicates correctly, if all fields specified can be found for all inventories
                if ds.get(field) is None:
                    missing_fields[field] += 1
                    missing_fields_error = True
            
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
            container[ID] = True
            
        
        # Raise error if fields were not found in inventories
        if missing_fields_error:
            formatted_error = "The following fields were found to be missing (of a total of " + str(len(db_var)) + " inventories)\n" + "\n".join([" - '" + k + "' --> missing in " + str(v) + " inventory/ies" for k, v in missing_fields.items() if v > 0])
            raise ValueError(formatted_error)
                
        return new_db_var
    
    
    
    def set_code(db_var,
                 fields: tuple | list,
                 overwrite: bool = True,
                 strip: bool = True,
                 case_insensitive: bool = True,
                 remove_special_characters: bool = False):

        # Path to input file where UUIDs are stored
        file_path = pathlib.Path(__file__).parent / "Input" / "data_UUIDs.xlsx"
        
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
        UUID_mapping = {}
        
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
        new_UUIDs = []
        
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
                                  case_insensitive = case_insensitive,
                                  strip = strip,
                                  remove_special_characters = remove_special_characters)
            
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
            ds["code"] = UUID
            
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
                    exc["code"] = UUID
                    
                    # Overwrite or create 'input' field with the new code of the inventory and the database
                    exc["input"] = (ds["database"], UUID)
                    

        # Raise error if fields were not found in activities
        if missing_fields_error:
            formatted_error = "The following fields were found to be missing (of a total of " + str(len(db_var)) + " inventories)\n" + "\n".join([" - '" + k + "' --> missing in " + str(v) + " activity/ies" for k, v in missing_fields.items() if v > 0])
            raise ValueError(formatted_error)
        
        # Write dataframe with new UUIDs
        pd.DataFrame(UUIDs_orig.to_dict("records") + new_UUIDs).to_excel(file_path, sheet_name = "UUIDs", index = False)
     
        return db_var
    
    
    
    def add_SimaPro_classification(db_var):
        
        # Initialize list to gather the inventory names where classification was not available
        inventory_classification_not_available = []
        
        # Loop through each inventory
        for ds in db_var:
            
            # Go on if all fields are already available
            if "SimaPro_classification" in ds and "SimaPro_categories" in ds and "categories" in ds:
                continue
            
            # Initialize list
            inventory_classification = []
            
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
        
        # # Initialize list to gather the inventory names where category types were not available
        # category_type_not_available = []
        
        # Loop through each inventory
        for ds in db_var:
            
            # Go on if all fields are already available
            if "SimaPro_category_type" in ds and "SimaPro_categories" in ds and "categories" in ds:
                continue
            
            # Extract the category type from the SimaPro meta data
            category_type = ds.get("simapro metadata", {}).get("Category type")
            
            # If it is None (not available), add to list
            if category_type is None:
                category_type = "material" # !!! We specify here that category type is 'material' if none is provided
                # category_type_not_available += [ds["name"]]
                # continue
            
            # Otherwise, add respective fields
            ds["SimaPro_category_type"] = category_type
            ds["categories"] = (SIMAPRO_CATEGORY_TYPE_COMPARTMENT_MAPPING[category_type],)
            ds["SimaPro_categories"] = (SIMAPRO_CATEGORY_TYPE_COMPARTMENT_MAPPING[category_type],)
        
        # # Raise error if category type could not be specified for some inventories
        # if category_type_not_available != []:
        #     raise ValueError("For some inventories, 'Category type' could not be extracted. Error occured in the following inventories:\n\n - " + "\n - ".join(set(category_type_not_available)))
                
        return db_var
    
    
    def add_allocation_field(db_var):
        
        # Loop through each inventory
        for ds in db_var:
            
            # For waste treatments, unfortunately no allocation is specified
            # We assume that there is always only one product and therefore the allocation is 1.
            if ds["SimaPro_categories"] == (SIMAPRO_TECHNOSPHERE_COMPARTMENTS["waste_to_treatment"],):
                ds["allocation"] = float(1)
                continue
            
            # Initialize a list to store all allocation factors
            allocations = []
            
            # Loop through each exchange
            for exc in ds["exchanges"]:
                
                # Check if current exchange is of type production
                if exc["type"] == "production":
                    
                    # Extract allocation
                    allocation = exc.get("allocation")
                    
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
            
            allocation = ds.get("allocation")
            prod_amount = ds.get("production amount")
            
            if allocation is None:
                raise ValueError("Field 'allocation' not provided in current inventory dictionary.")
            
            if prod_amount is None:
                raise ValueError("Field 'production amount' not provided in current inventory dictionary.")
            
            # Add output amount field to inventory
            ds["output amount"] = float(prod_amount * allocation)
            
        return db_var

    
# All strategies that only affect exchange and not inventory dictionaries
class exchange_strategies():
    
    # Erase all exchanges that have a zero amount
    def remove_exchanges_with_zero_amount(db_var):
        
        # Loop through each inventory
        for ds in db_var:
            
            # Initialize an empty list to store all exchanges that have an amount other than 0
            exchanges = []
            
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
            ds["exchanges"] = copy.deepcopy(exchanges)

        return db_var
    
    
    
    def regionalize(db_var,
                    regionalize_biosphere: bool,
                    regionalize_technosphere: bool,
                    loaded_biosphere_databases: (list | None) = None,
                    loaded_technosphere_databases: (list | None) = None,
                    local_output_path: (pathlib.Path | None) = None,
                    write_mapping_xlsx: bool = True,
                    tag_regionalized_flow: bool = True,
                    ):
        
        """
        Brightway2 strategy that can be used to find and replace exchanges of type 'biosphere' and/or 'technosphere' with respective regionalized flows.
        Replacing inventories/exchanges need to be provided by already existing databases via 'db_name'.load()

        Parameters
        ----------
        db_var : Brightway2 Database Importer object
            Database importer object with attribute (.data) containing a list of dictionaries (= inventories) to be imported
        
        regionalize_biosphere : bool
            Define whether to regionalize 'biosphere' flows from exchanges (dict.key 'exchanges') of an inventory or not.
        
        regionalize_technosphere : bool
            Define whether to regionalize 'technosphere' flows from exchanges (dict.key 'exchanges') of an inventory or not.
        
        loaded_biosphere_databases : (list | None)
            If 'regionalize_biosphere' is True, needs to be provided as list of existing and loaded Brightway2 biosphere databases (dict). Otherwise it is None.
        
        loaded_technosphere_databases : (list | None)
            If 'regionalize_technosphere' is True, needs to be provided as list of existing and loaded Brightway2 technosphere databases (dict). Otherwise it is None.
        
        local_output_path : (pathlib.Path | None), optional
            Path where mapping XLSX file will be written to. The default is None.
        
        write_mapping_xlsx : bool, optional
            Specify whether to write the mapping of regionalized exchanges from the strategy to XLSX. The default is True.
        
        tag_regionalized_flow : bool, optional
            Specify whether to state if exchange has been regionalized. If yes, a key ('exchange was regionalized') / value ('True' or 'False') will be added to the dictionary of the exchanges. The default is True.
        
        Returns
        -------
        db_var : Brightway2 Importer object
            Returns an updated Brightway2 database

        """
        # Check function input variable type
        hp.check_function_input_type(exchange_strategies.regionalize, locals())
        
        # Import packages
        from location_dependencies.create_location_dependencies_and_mapping import location_mapping
        
        # Import the location dependency 'within' which was created by the module 'create_location_dependencies_and_mapping.py' from XLSX
        location_within_orig = pd.read_excel(PATH_VAR["folder_location"] / "Output" / FILE_VAR["location_within_XLSX"], keep_default_na = False).fillna("") 

        # Create dictionary with within dependencies
        location_within = {m: {row["TO"]: {"region_shortname": row["TO"], "region_name": location_mapping[row["TO"]], "region_size": row["AREA_TO [km2]"]} for idx_row, row in location_within_orig.query("FROM == @m").iterrows()} for m in set(location_within_orig["FROM"])}

        # Create a dictionary with the area sizes of the countries
        location_sizes = {k2: v2["region_size"] for k1, v1 in location_within.items() for k2, v2 in v1.items()}

        # Change working directory
        os.chdir(PATH_VAR["folder_Input"])

        # Read the mapping file
        sim_inv_orig = pd.read_excel("data_similar_inventories.xlsx").fillna("")

        # Again switch back
        os.chdir(here.parent)

        # Extract all unique indexes
        indexes = set(list(sim_inv_orig["index"]))

        # Initialise an empty dictionary for the mapping
        similar_name_mapping = {}

        # Loop through each index
        for index in indexes:
            
            # Extract the relevant exchange/flow names and their respective database for the current index
            data_index = sim_inv_orig.query("index == @index")
            
            # Take only unique elements
            unique_items = list(set([m["similar_inventory_names"] for idx, m in data_index.iterrows()]))
            
            # Make the permutations
            unique_combinations = [m for m in itertools.permutations(unique_items, 2)]
            
            # Loop through each permutation
            for orig_name, new_name in unique_combinations:
                
                # Write to the 'similar_name_mapping' dicitonary, if not yet available
                if orig_name.lower() not in similar_name_mapping:
                    similar_name_mapping[orig_name.lower()] = [new_name.lower()]
                else:
                    similar_name_mapping[orig_name.lower()] += [new_name.lower()]

        def extract_new_exc(exc: dict, location: str, mapping_dict: dict, type_str: str, tag_regionalized_flow: bool):
            
            # Try to find the same flow but with the other region 
            # Extract the new flow from the mapping
            if type_str == "technosphere":
                try: new_exc = mapping_dict[exc["name"].lower()][exc["unit"].lower()][location.lower()]
                except: return None
            
            elif type_str == "biosphere":
                try: new_exc = mapping_dict[exc["name"].lower()][exc["top_category"].lower()][exc["sub_category"].lower()][exc["unit"].lower()][location.lower()]
                except:
                    try: new_exc = mapping_dict[exc["name"].lower()][exc["top_category"].lower()][""][exc["unit"].lower()][location.lower()]
                    except: return None
            
            # Merge old and new together
            exc_to_return_orig = dict(exc, **new_exc)
            exc_to_return = dict(exc_to_return_orig, **{"type": type_str,
                                                        "location": location,
                                                        "exchange was regionalized": True})
                
            # Eliminate certain parameters which should not be transferred
            for key in ["input", "output", "database", "code"]:
                try: del exc_to_return[key]
                except: pass
                
            # Decide whether to mark the current exchange that it has been regionalized or not
            # If it should not be tagged, eliminate the tag key again
            if not tag_regionalized_flow:
                del exc_to_return["exchange was regionalized"]
                
            # Return the new exchange
            return exc_to_return
        
        
        if regionalize_biosphere and loaded_biosphere_databases is None:
            raise ValueError("No biosphere databases provided for regionalization of biosphere. Either: provide variable 'loaded_biosphere_databases' (list) or switch 'regionalize_biosphere' to False.")
        
        if regionalize_technosphere and loaded_technosphere_databases is None:
            raise ValueError("No technosphere databases provided for regionalization of technosphere. Either: provide variable 'loaded_technosphere_databases' (list) or switch 'regionalize_technosphere' to False.")
        
        # Check if filepath is specified
        if write_mapping_xlsx and local_output_path is None:
            raise ValueError("Specify output path 'local_output_path' or change 'write_mapping_xlsx' to False.")
        
        # Data which should not be added as exchanges
        exclude_keys = ["allocation", "output amount", "exchanges", "simapro metadata", "filename", "production amount", "reference product", "comment", "SimaPro_category_type"]
        exclude_keys_lower = [m.lower() for m in exclude_keys]
        
        # Initialise a variable to which mapped flows are written
        mapping_file = []
        
        # Create a technosphere dictionary to check for regionalized flows
        # Only create the dictionary, if technosphere should be regionalized
        if regionalize_technosphere:
            
            # Initialise an empty dictionary
            tech_dict = {}
            
            # # Loop through each loaded database
            # for database in loaded_technosphere_databases:
                
            #     # Loop through each inventory item
            #     for key, value in database.items():
                    
            #         # Write the name of the inventory as key to the new 'tech_dict'
            #         if value["name"].lower() not in tech_dict:
            #             tech_dict[value["name"].lower()] = {}
                    
            #         # Add the unit as dict to the existing dict
            #         if value["unit"].lower() not in tech_dict[value["name"].lower()]:
            #             tech_dict[value["name"].lower()][value["unit"].lower()] = {}
                        
            #         # Add the location as dict to the existing dict
            #         if value["location"].lower() not in tech_dict[value["name"].lower()][value["unit"].lower()]:
            #             tech_dict[value["name"].lower()][value["unit"].lower()][value["location"].lower()] = dict({k: v for k, v in value.items() if k.lower() not in exclude_keys_lower}, **{"database": key[0]})

            
            # Loop through each inventory item
            for value in loaded_technosphere_databases:
                
                # Write the name of the inventory as key to the new 'tech_dict'
                if value["name"].lower() not in tech_dict:
                    tech_dict[value["name"].lower()] = {}
                
                # Add the unit as dict to the existing dict
                if value["unit"].lower() not in tech_dict[value["name"].lower()]:
                    tech_dict[value["name"].lower()][value["unit"].lower()] = {}
                    
                # Add the location as dict to the existing dict
                if value["location"].lower() not in tech_dict[value["name"].lower()][value["unit"].lower()]:
                    tech_dict[value["name"].lower()][value["unit"].lower()][value["location"].lower()] = {k: v for k, v in value.items() if k.lower() not in exclude_keys_lower}
    
        
        
        # Create a biosphere dictionary to check for regionalized flows
        # Only create the dictionary, if biosphere should be regionalized
        if regionalize_biosphere:
            
            # Initialise an empty dictionary
            bio_dict = {}
            
            # # Loop through each loaded database
            # for database in loaded_biosphere_databases:
                
            #     # Loop through each biosphere flow item
            #     for key, value in database.items():
                    
            #         # Write the name of the biosphere flow as key to the new 'bio_dict'
            #         if value["name"].lower() not in bio_dict:
            #             bio_dict[value["name"].lower()] = {}
                    
            #         # Add the categories as dict to the existing dict
            #         if value["top_category"].lower() not in bio_dict[value["name"].lower()]:
            #             bio_dict[value["name"].lower()][value["top_category"].lower()] = {}
                    
            #         # Add the categories as dict to the existing dict
            #         if value["sub_category"].lower() not in bio_dict[value["name"].lower()][value["top_category"].lower()]:
            #             bio_dict[value["name"].lower()][value["top_category"].lower()][value["sub_category"].lower()] = {}
                    
            #         # Add the unit as dict to the existing dict
            #         if value["unit"].lower() not in bio_dict[value["name"].lower()][value["top_category"].lower()][value["sub_category"].lower()]:
            #             bio_dict[value["name"].lower()][value["top_category"].lower()][value["sub_category"].lower()][value["unit"].lower()] = {}            
                    
            #         # Add the location as dict to the existing dict
            #         if value["location"].lower() not in bio_dict[value["name"].lower()][value["top_category"].lower()][value["sub_category"].lower()][value["unit"].lower()]:
            #             bio_dict[value["name"].lower()][value["top_category"].lower()][value["sub_category"].lower()][value["unit"].lower()][value["location"].lower()] = dict({k: v for k, v in value.items() if k.lower() not in exclude_keys_lower}, **{"database": key[0]})


            # Loop through each biosphere flow item
            for value in loaded_biosphere_databases:
                
                # Write the name of the biosphere flow as key to the new 'bio_dict'
                if value["name"].lower() not in bio_dict:
                    bio_dict[value["name"].lower()] = {}
                
                # Add the categories as dict to the existing dict
                if value["top_category"].lower() not in bio_dict[value["name"].lower()]:
                    bio_dict[value["name"].lower()][value["top_category"].lower()] = {}
                
                # Add the categories as dict to the existing dict
                if value["sub_category"].lower() not in bio_dict[value["name"].lower()][value["top_category"].lower()]:
                    bio_dict[value["name"].lower()][value["top_category"].lower()][value["sub_category"].lower()] = {}
                
                # Add the unit as dict to the existing dict
                if value["unit"].lower() not in bio_dict[value["name"].lower()][value["top_category"].lower()][value["sub_category"].lower()]:
                    bio_dict[value["name"].lower()][value["top_category"].lower()][value["sub_category"].lower()][value["unit"].lower()] = {}            
                
                # Add the location as dict to the existing dict
                if value["location"].lower() not in bio_dict[value["name"].lower()][value["top_category"].lower()][value["sub_category"].lower()][value["unit"].lower()]:
                    bio_dict[value["name"].lower()][value["top_category"].lower()][value["sub_category"].lower()][value["unit"].lower()][value["location"].lower()] = {k: v for k, v in value.items() if k.lower() not in exclude_keys_lower}

    
        
        
        # Loop through each inventory in the database
        for ds in db_var:
            
            # Extract the original location of the inventory
            orig_location = ds.get("location")
            
            # If the current location is None or is not specified, go to next inventory
            if orig_location is None:
                continue
            
            # If the current 'orig_location' can not be mapped to another region, use the already existing region
            if location_within.get(orig_location) is None:
                
                # Write list with the original location
                regionalized_ds = [(orig_location, 1)]
            else:        
                # Otherwise..
                # Extract the broader regions for the current inventory region which could be used for regionalization
                # regionalized_ds = sorted([(v["region_shortname"], v["region_size"]) for k, v in location_mapping[orig_location].items() if v["region_shortname"] != orig_location] + [(orig_location, 1)], key = lambda x: x[1])
                regionalized_ds = sorted([(v["region_shortname"], v["region_size"]) for k, v in location_within[orig_location].items() if v["region_shortname"] != orig_location] + [(orig_location, 1)], key = lambda x: x[1])
                
            
            # Initialise an empty list for the new exchanges
            exc_list_new = []
            
            # Loop through exchanges of the inventory
            for exc in ds["exchanges"]:
                
                # Write exchange location to separate variable
                exc_location = exc["location"]
                
                # Define the cutoff value for the location
                # If the new location size is above the current location size, do not change
                exc_location_size = int(location_sizes.get(exc["location"], 0))
                
                # # Check if the current exchange location is part of the inventory location
                # exchange_location_is_within_ds_location = exc_location in [m[0] for m in regionalized_ds]
                
                # Add the current location of the exchange
                if exc_location_size != 0:
                    regionalized_ds_curr = sorted(regionalized_ds.copy() + [(exc_location, exc_location_size)], key = lambda x: x[1])
                else:
                    regionalized_ds_curr = regionalized_ds.copy()
                
                # Deepcopy the current exchange
                new_exc_curr = copy.deepcopy(exc)
                
                # If the type is 'technosphere' and the flow should be regionalized and the current location of the exchange is not the same as the inventory location, go on
                if exc["type"] == "technosphere" and regionalize_technosphere:
                
                    # Extract the new exchanges, if possible. If not possible, None will be returned
                    new_excs_orig = [(extract_new_exc(exc, region, tech_dict, "technosphere", tag_regionalized_flow), size) for region, size in regionalized_ds_curr]

                    # Remove the regions where no exchanges have been found
                    new_excs = [m for m in new_excs_orig if m[0] is not None]
                                       
                    # Try to find a similar exchange name and then try again to regionalize that exchange with the new name
                    # First, extract all possible new names
                    available_new_names = similar_name_mapping.get(exc.get("name", "").lower())
                        
                    # Only go on, if something has been found
                    if available_new_names is not None:
                            
                        # For each new name found, make a copy of the current exchange and rename the original name with the new name
                        additional_exc = [dict(exc, **{"name": name}) for name in available_new_names]
                    
                        # Check whether a new exchange is found with the new name and the new region
                        additional_new_exc_orig = [(extract_new_exc(m, region, tech_dict, "technosphere", tag_regionalized_flow), size) for m in additional_exc for region, size in regionalized_ds_curr]
                           
                        # Remove all None's (None's indicate that nothing has been found)
                        additional_new_exc = [m for m in additional_new_exc_orig if m[0] is not None]
                        
                        # Add the exchanges found with the similar name
                        new_excs += additional_new_exc

                    # If the list is not empty, that means, that a new exchange has been found. Write it to the list of exchanges
                    if new_excs != []:
                                
                        # Sort the list, the smallest region size coming first
                        new_excs_sorted = [m for m in sorted(new_excs, key = lambda x: x[1])]
                        
                        # Extract the new inventory and the location size of it
                        new_excs_inv = new_excs_sorted[0][0]
                        # new_excs_size = new_excs_sorted[0][1]
                                      
                        # We add the new exchange to the list of exchanges
                        new_exc_curr = copy.deepcopy(new_excs_inv)
                        
                        # Change regionalize bool variable
                        regionalize_bool = True
                                
                    else:
                        # Otherwise, keep the already existing exchange
                        new_exc_curr = copy.deepcopy(exc)
                        
                        # Change regionalize bool variable
                        regionalize_bool = False



                # If the type is 'technosphere' and the flow should be regionalized and the current location of the exchange is not the same as the inventory location, go on
                elif exc["type"] == "biosphere" and regionalize_biosphere:
                
                    # Extract the new exchanges, if possible. If not possible, None will be returned
                    new_excs_orig = [(extract_new_exc(exc, region, bio_dict, "biosphere", tag_regionalized_flow), size) for region, size in regionalized_ds_curr]

                    # Remove the regions where no exchanges have been found
                    new_excs = [m for m in new_excs_orig if m[0] is not None]
                                        
                    # Try to find a similar exchange name and then try again to regionalize that exchange with the new name
                    # First, extract all possible new names
                    available_new_names = similar_name_mapping.get(exc.get("name", "").lower())
                        
                    # Only go on, if something has been found
                    if available_new_names is not None:
                            
                        # For each new name found, make a copy of the current exchange and rename the original name with the new name
                        additional_exc = [dict(exc, **{"name": name}) for name in available_new_names]
                    
                        # Check whether a new exchange is found with the new name and the new region
                        additional_new_exc_orig = [(extract_new_exc(m, region, bio_dict, "biosphere", tag_regionalized_flow), size) for m in additional_exc for region, size in regionalized_ds_curr]
                           
                        # Remove all None's (None's indicate that nothing has been found)
                        additional_new_exc = [m for m in additional_new_exc_orig if m[0] is not None]
                        
                        # Add the exchanges found with the similar name
                        new_excs += additional_new_exc

                    # If the list is not empty, that means, that a new exchange has been found. Write it to the list of exchanges
                    if new_excs != []:

                        # Sort the list, the smallest region size coming first
                        new_excs_sorted = [m for m in sorted(new_excs, key = lambda x: x[1])]
                        
                        # Extract the new inventory and the location size of it
                        new_excs_inv = new_excs_sorted[0][0]
                        # new_excs_size = new_excs_sorted[0][1]
                                      
                        # We add the new exchange to the list of exchanges
                        new_exc_curr = copy.deepcopy(new_excs_inv)
                                
                        # Change regionalize bool variable
                        regionalize_bool = True
                                
                    else:
                        # Otherwise, keep the already existing exchange
                        new_exc_curr = copy.deepcopy(exc)
                        
                        # Change regionalize bool variable
                        regionalize_bool = False  
                        
                else:
                    regionalize_bool = False
                
                
                # The variable 'new_exc_curr' needs to be of type dict
                if not isinstance(new_exc_curr, dict):

                    # Raise an error if we arrive at this point here
                    raise ValueError("The variable 'new_exc_curr' is of type " + str(type(new_exc_curr)) + ", but should be of type <dict>." + "\nInventory name: " + str(ds.get("name")) + ", Inventory location: " + str(ds.get("location")) + "\nExchange name: " + str(exc.get("name")) + ", Exchange location: " + str(exc.get("location")) + ", Exchange unit: " + str(exc.get("unit")))

                
                # Write (new) exchange to list
                # Decide whether to mark the current exchange that it has been regionalized or not
                if tag_regionalized_flow:
                    
                    # Check if the new location is the same as the old location
                    # if exc_location == new_exc_curr["location"]:
                    if exc_location == new_exc_curr["location"] or exc_location == orig_location:
                        
                        # Write the new exchange to the list of new exchanges
                        exc_list_new += [dict(copy.deepcopy(exc), **{"exchange was regionalized": False})]
                    
                    else:
                        # Write the new exchange to the list of new exchanges
                        exc_list_new += [dict(copy.deepcopy(new_exc_curr), **{"exchange was regionalized": regionalize_bool})]
                        
                else:
                    # Write the new exchange to the list of new exchanges
                    exc_list_new += [copy.deepcopy(new_exc_curr)]
                
                
                # We don't have to write to the mapping file if the exchange is not of type biosphere or technosphere
                if exc["type"] not in ["biosphere", "technosphere"]:
                    continue
                
                # Write mapping file
                if not regionalize_bool or exc_location == new_exc_curr["location"] or exc_location == orig_location:
                        
                    # Write to list
                    mapping_file += [{"original_location": orig_location,
                                      "type": exc["type"],
                                      "old_name": exc["name"],
                                      "old_top_category": exc.get("top_category"),
                                      "old_sub_category": exc.get("sub_category"),
                                      "old_location": exc["location"],
                                      "old_unit": exc["unit"],
                                      "new_name": "",
                                      "new_top_category": "",
                                      "new_sub_category": "",
                                      "new_location": "",
                                      "new_unit": ""}]
                    
                else:
                    # Write to list
                    mapping_file += [{"original_location": orig_location,
                                      "type": exc["type"],
                                      "old_name": exc["name"],
                                      "old_top_category": exc.get("top_category"),
                                      "old_sub_category": exc.get("sub_category"),
                                      "old_location": exc["location"],
                                      "old_unit": exc["unit"],
                                      "new_name": new_exc_curr["name"],
                                      "new_top_category": new_exc_curr.get("top_category"),
                                      "new_sub_category": new_exc_curr.get("sub_category"),
                                      "new_location": new_exc_curr["location"],
                                      "new_unit": new_exc_curr["unit"]}]
                
            
            # Overwrite the existing with the new list of exchanges
            ds["exchanges"] = copy.deepcopy(exc_list_new)
        
        # Write mapping file if specified
        if write_mapping_xlsx:
            
            # Extract current time
            current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            
            # Define the name of the mapping file
            xlsx_filename_orig = "mapping_file.xlsx"
            xlsx_filename = current_time + "_" + xlsx_filename_orig
            
            # Write mapping file
            mapping_file_XLSX_orig = pd.DataFrame(mapping_file)
            
            # Drop duplicates
            mapping_file_XLSX = mapping_file_XLSX_orig.drop_duplicates(subset = ["original_location",
                                                                                 "type",
                                                                                 "old_name",
                                                                                 "old_top_category",
                                                                                 "old_sub_category",
                                                                                 "old_location",
                                                                                 "old_unit",
                                                                                 "new_name",
                                                                                 "new_top_category",
                                                                                 "new_sub_category",
                                                                                 "new_location",
                                                                                 "new_unit"])
            
            # Print statement
            print(starting + "\nSaving xlsx")
            
            # Write data to XLSX
            with pd.ExcelWriter(os.path.join(local_output_path, xlsx_filename)) as writer:
                mapping_file_XLSX.to_excel(writer, sheet_name = "mapped") 
                
            # Print statement saying where xlsx file was stored
            print(starting + "\nMapping file saved to:\n" + os.path.join(str(local_output_path), str(xlsx_filename)))
            print("\n")
        
        return db_var
    
    
    
    
class biosphere_strategies():
    
    def add_top_and_subcategory_fields_for_biosphere_flows(db_var, remove_initial_category_field: bool = False):
        
        for ds in db_var:
            for exc in ds["exchanges"]:
                
                if exc["type"] == "biosphere":
                    
                    exc["top_category"] = exc["categories"][0]
                    exc["sub_category"] = exc["categories"][1] if len(exc["categories"]) > 1 else ""
                    
                    if remove_initial_category_field:
                        del exc["categories"]
                        
        return db_var


    def cas_number_addition(db_var):
        
        """ Brightway2 strategy, which adds CAS number from a mapping file to biosphere elementary flows if no CAS number is yet defined. 

        Parameters
        ----------
        db_var : Brightway2 Backends Pewee Database
            A Brightway2 Backends Pewee Database which should be modified.
            
        mapping_data : dict
            A dictionary where keys are biosphere elementary flow names and values are CAS numbers.

        Returns
        -------
        db_var : Modified Brightway2 Backends Pewee Database
            A modified Brightway2 Backends Pewee Database is returned, where CAS numbers are added to biosphere elementary flows, if mapping was successful.

        """
        
        # Make variable check
        hp.check_function_input_type(biosphere_strategies.cas_number_addition, locals())
        
        # -------------------
        # Add CAS number to where no CAS number is currently identified

        # Get paths
        local_input_path = PATH_VAR["folder_Input"]

        # Import the CAS-Nr. & substance mapping file
        CAS_mapping_orig = pd.read_excel(local_input_path / FILE_VAR["cas_numbers_mapping_XLSX"])
           
        # Drop duplicated rows. All entries are removed which have a common value in columns 'CAS_Number' and 'lc_Wirkstoffname'
        CAS_mapping = CAS_mapping_orig.drop_duplicates(["CAS_Number", "lc_Wirkstoffname"])[["CAS_Number", "lc_Wirkstoffname"]]
           
        # Convert pandas series to list
        substance_names = list(CAS_mapping["lc_Wirkstoffname"])
        cas_number = list(CAS_mapping["CAS_Number"])

        # Create a dictionary with key/value pairs, where key is a substance name and value is the respecting CAS-Nr.
        CAS_mapping_dict = {a : hp.give_back_correct_cas(b) for a, b in zip(substance_names, cas_number) if hp.give_back_correct_cas(b) is not None}
        
        # Loop through each inventory
        for ds in db_var:
            
            # Loop through each exchange
            for exc in ds.get("exchanges", []):
                
                # Only add to biosphere flows
                if exc.get("type") == "biosphere":
                    
                    # Only add if no CAS number is available
                    if exc.get("CAS number") is None or exc.get("CAS number") == "":
                        
                        # Lookup if a CAS number can be found based on the elementary flow name given
                        CAS_number = CAS_mapping_dict.get(exc.get("name", ""))
                        
                        # If a CAS number has been found, add it to the current biosphere flow
                        if CAS_number is not None:
                            exc["CAS number"] = CAS_number
                        
                        else:
                            exc["CAS number"] = ""
                            
                    else:
                        # Otherwise, make sure that the existing CAS number is in the correct format and delete if not successfully transformed
                        # Check if transformation yields something
                        if hp.give_back_correct_cas(exc["CAS number"]) is None:
                            
                            # If transformation was unsuccessful and yielded None, add empty string
                            exc["CAS number"] = ""
                            
                        else:
                            # Otherwise, transform and update the current key/value pair
                            exc["CAS number"] = hp.give_back_correct_cas(exc["CAS number"])
                        
        return db_var    




    # A copy of the Brightway strategy is made here with the adaptations that we consider our own biosphere mapping
    # ... which is slightly different from Brightway but still holds to the ecoinvent standard
    def normalize_simapro_biosphere_categories(db_var):
        
        """Normalize biosphere categories to own and ecoinvent standard."""
        
        for ds in db_var:
            for exc in (
                exc for exc in ds.get("exchanges", []) if exc["type"] == "biosphere"
            ):
                cat = SIMAPRO_BIO_TOPCATEGORIES_MAPPING.get(exc["categories"][0], exc["categories"][0])
                if len(exc["categories"]) > 1:
                    subcat = SIMAPRO_BIO_SUBCATEGORIES_MAPPING.get(
                        exc["categories"][1], exc["categories"][1]
                    )
                    exc["categories"] = (cat, subcat)
                else:
                    exc["categories"] = (cat,)
        
        return db_var
    
    
    
    
    
    def add_location_to_biosphere_exchanges(db_var,
                                            select_GLO_in_name_valid_for_method_import: bool = False):
        
        """ Add a location parameter to biosphere elementary exchanges in inventories of database 'db_var'.

        Parameters
        ----------
        db_var : Brightway2 Backends Pewee Database
            A Brightway2 Backends Pewee Database where elementary flows should be modified.
            
        select_GLO_in_name_valid_for_method_import : bool
            Should be specified as True, if the strategy is used for the import of impact asssessment methods. Otherwise, set this parameter to False, if e.g., inventories are imported.
            
            Explanation: there might appear flows in impact assessment methods which have 'GLO' specified in the flow name AND at the same time the same flow name without 'GLO' in the name also appears.
            In this case, we can only keep one flow, otherwise we would have duplicated flows. The strategy eliminates the flow where 'GLO' was not specified in the name and keeps the flow where 'GLO' was specified in the name.

        Returns
        -------
        db_var : Modified Brightway2 Backends Pewee Database
            A modified Brightway2 Backends Pewee Database is returned, where the parameter 'location' is added to each biosphere elementary exchange.

        """
        
        # Check function input type
        hp.check_function_input_type(biosphere_strategies.add_location_to_biosphere_exchanges, locals())
        
        # Set path
        os.chdir(here)
        
        # Import location dependencies
        from location_dependencies.create_location_dependencies_and_mapping import location_mapping
        
        # Additional location mappings which might appear in biosphere flows
        # and which might be outdated and need to be replaced with other locations
        additional_location_mappings = {"ASCC": "US-ASCC",
                                        # "FRCC": "",
                                        "Europe, without Russia and Turkey": "Europe, without Russia and Türkiye",
                                        "HICC": "US-HICC",
                                        "MRO, US only": "US-MRO",
                                        "NPCC, US only": "US-NPCC",
                                        "OCE": "UN-OCEANIA",
                                        # "OECD": "",
                                        "RFC": "US-RFC",
                                        "SERC": "US-SERC",
                                        "TRE": "US-TRE",
                                        "WECC, US only": "US-WECC",
                                        # "ZR": "",
                                        }

        # Check with the current location mapping, if the mapped locations appear
        # If not, raise an error. In that case, adjust the mapping above
        mapped_locations_not_available = [v for k, v in additional_location_mappings.items() if location_mapping.get(v) is None]
        if mapped_locations_not_available != []:
            raise ValueError("Additional location mapping (variable 'additional_location_mappings') contains key/value pairs where the value (= mapped location) does not appear in the 'location_mapping'. Make sure that the mapping is valid = that the value appears in the 'location_mapping' --> replace the following values:\n- " + "\n- ".join(mapped_locations_not_available))
            
        # Valid ecoinvent geographies
        locations = list(location_mapping.keys()) + list(additional_location_mappings.keys())
        
        # Make the own location mapping keys lowercase
        additional_location_mappings_small = {k.lower(): v for k, v in additional_location_mappings.items()}
        
        # Create dictionary for better search performance
        locations_dict = {m: m for m in locations}
        
        
        
        # Function to split exchange name into name and location based on fragment specified
        def split_exchange_name(name: str, pattern: str, loc_mapping_dict: dict):
            
            # Check function input type
            hp.check_function_input_type(split_exchange_name, locals())
            
            # Split the name with the pattern specified into fragments
            splitted = name.split(pattern)
            
            # Initialise variables
            initial = splitted[len(splitted) - 1]
            appended = (initial,)
            
            # Go through each fragment which was split by the pattern
            # ... and extract potential locations
            for mm in reversed(splitted[:-1]):
                appended += (mm + pattern + initial,)
                initial = mm + pattern + initial
            
            # Check if the fragment which could be locations are actually locations or not by comparing the fragment with the location mapping dictionary
            locs_extracted = [loc_mapping_dict[mmm] for mmm in reversed(appended) if loc_mapping_dict.get(mmm) is not None]
            
            # If something was found, return the result
            if len(locs_extracted) > 0:
                
                # Return a successful boolean, the new name and the new location
                return True, name.replace(pattern + locs_extracted[0], ""), locs_extracted[0]
            
            else:
                # Return an unsuccessful boolean, the old name and a global location
                return False, name, "GLO"



        # Loop through each inventory
        for ds in db_var:
            
            # Initalize list to store elementary flows, which have the pattern 'GLO' in their name
            GLO_priorized_orig = []
            
            # Loop through all exchanges of an inventory.
            for exc in ds["exchanges"]:
                
                # Only go on if the exchange is a biosphere elementary flow
                if exc.get("type") == "biosphere":
                    
                    # Check if location key is already available
                    if exc.get("location") is not None:
                        
                        # Add SimaPro name field
                        # Either with the comma and the location abbreviation in the name
                        if "SimaPro_name" not in exc and exc["location"] != "GLO":
                            exc["SimaPro_name"] = exc["name"] + ", " + str(exc["location"])
                        
                        # Or without the location abbreviation and only the name, if the location is GLO
                        elif "SimaPro_name" not in exc and exc["location"] == "GLO":
                            exc["SimaPro_name"] = exc["name"]
                            
                        continue
                        
                        
                    # Apply the first pattern and extract new name and new location
                    successful_1, name_1, location_1 = split_exchange_name(exc["name"], ", ", locations_dict)
                    
                    # If the first pattern did not yield a result, try to apply the second pattern
                    if not successful_1:
                        
                        # Apply the second pattern and extract new name and new location
                        successful_2, name_2, location_2 = split_exchange_name(exc["name"], ",", locations_dict)
                        
                    else:
                        # Otherwise just specify a false boolean
                        successful_2 = False
                    
                    # Add new name and new location extracted with the first pattern to the exchange
                    if successful_1:
                        
                        # Add parameters to exchange dictionary
                        exc["SimaPro_name"] = exc["name"]
                        exc["location"] = additional_location_mappings_small.get(location_1.lower(), location_1)
                        exc["name"] = name_1
                        
                        # Check, if the pattern 'GLO' has appeared in the elementary flow name
                        # If yes, store the flow in a list. In case the same flow without specifying 'GLO' in the name appears in the same method,
                        # it will be overwritten with that elementary flow at the end
                        if exc["location"] == "GLO" and select_GLO_in_name_valid_for_method_import:
                            
                            # Add the current elementary flow to the list
                            GLO_priorized_orig += [exc].copy()
                    
                    # Add new name and new location extracted with the second pattern to the exchange
                    elif successful_2:
                        
                        # Add parameters to exchange dictionary
                        exc["SimaPro_name"] = exc["name"]
                        exc["location"] = additional_location_mappings_small.get(location_2.lower(), location_2)
                        exc["name"] = name_2
                        
                        # Check, if the pattern 'GLO' has appeared in the elementary flow name
                        # If yes, store the flow in a list. In case the same flow without specifying 'GLO' in the name appears in the same method,
                        # it will be overwritten with that elementary flow at the end
                        if exc["location"] == "GLO" and select_GLO_in_name_valid_for_method_import:
                            
                            # Add the current elementary flow to the list
                            GLO_priorized_orig += [exc].copy()
                        
                    else:
                        # Add parameters to exchange dictionary
                        exc["SimaPro_name"] = exc["name"]
                        exc["location"] = additional_location_mappings_small.get(location_1.lower(), location_1)
                        exc["name"] = name_1

            
            # Check if flows have appeared, where 'GLO' has been specified in the elementary flow name
            if select_GLO_in_name_valid_for_method_import and GLO_priorized_orig != []:
                
                # Check if 'location' is specified as separate key in exchanges
                if None in [m if "location" in m else None for m in GLO_priorized_orig]:
                    
                    # Make a dictionary of the list with unique values as keys
                    # ... using 'name', 'categories' and 'unit'
                    GLO_priorized = {(m["name"], m["categories"], m["unit"]): m for m in GLO_priorized_orig}
                    
                    # Delete all unique and same exchanges as in the list 'GLO_priorized'
                    ds["exchanges"] = [m for m in ds["exchanges"].copy() if not any([element is None for element in [m.get("name"), m.get("categories"), m.get("unit")]]) and (m.get("name"), m.get("categories"), m.get("unit")) not in GLO_priorized.keys()].copy()
                
                else:
                    # Make a dictionary of the list with unique values as keys
                    # ... using 'name', 'categories', 'unit' and 'location'
                    GLO_priorized = {(m["name"], m["categories"], m["unit"], m["location"]): m for m in GLO_priorized_orig}
                    
                    # Delete all unique and same exchanges as in the list 'GLO_priorized'
                    ds["exchanges"] = [m for m in ds["exchanges"].copy() if not any([element is None for element in [m.get("name"), m.get("categories"), m.get("unit"), m.get("location")]]) and (m.get("name"), m.get("categories"), m.get("unit"), m.get("location")) not in GLO_priorized.keys()].copy()
                
                # Add only the flows again, which have been specified by 'GLO' in the name
                ds["exchanges"] += [n for n in GLO_priorized.values()].copy()
            
        return db_var



class technosphere_strategies():
    
    # Created by Chris Mutel

    # Somehow, the technosphere flows from SimaPro category 'Final waste flows' are not relevant.
    # They do not contribute to an LCA in Brightway (?).
    # During import, they are also not linked and only cause problems. Therefore, we exclude them.
    def drop_final_waste_flows(db_var):
        
        # Loop through each inventory
        for ds in db_var:
            
            # Loop through each exchange and only keep the exchange if it is not of type 'Final waste flows'
            # Otherwise, exclude the exchange
            ds["exchanges"] = [exc for exc in ds["exchanges"] if (exc.get("input") or exc["categories"] != ("Final waste flows",))]
        
        return db_var
    
    
    def extract_geography_from_SimaPro_name(db_var, placeholder_for_not_identified_locations: str = "not identified"):
        
        """ Extract the geography from the SimaPro inventory names and write it to a key 'location'.
        The original SimaPro name is set to ``SimaPro_name``."""
        
        # Check function input type
        hp.check_function_input_type(technosphere_strategies.extract_geography_from_SimaPro_name, locals())
        
        # Define the patterns needed
        # Extract everything in between curly brackets
        # pat_curly_brackets = "^(?P<name_I>.*)\{(?P<country>[A-Za-z0-9\s\-\&\/\+\,]{2,})\}(?P<name_II>.*)$"
        pat_curly_brackets = "^(?P<name_I>.*\{)(?P<country>[A-Za-z0-9\s\-\&\/\+\,]{2,})(?P<name_II>\}.*)$"
        
        # A general SimaPro pattern. Characters after '/' are identified as locations
        # pat_simapro_general = "^(?P<name_I>.*)\/(?P<country>[A-Za-z\-]{2,})(?P<name_II>)\s([A-Za-z0-9\s]{2,})?(S|U)$"
        pat_simapro_general = "^(?P<name_I>.*\/)(?P<country>[A-Za-z\-]{2,})(?P<name_II>\s([A-Za-z0-9\s]{2,})?(S|U))$"
        
        # Specific pattern that is used in the SALCA database
        # pat_SALCA = "^(?P<name_I>.*)\/(?P<unit>[A-Za-z0-9]+)\/(?P<country>[A-Z]{2,})(\/I)?(?P<name_II>)\s(.*)(S|U)$"
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
    

class XML_strategies():
    
    def assign_flow_field_as_code(db_var):
        
        """ Assign the value of field 'flow' (= UUID) as code for biosphere flows.
        Only valid for XML data from ecoinvent!"""
        
        # Loop through each inventory in the database
        for ds in db_var:
            
            # Loop through each exchange in 'ds'
            for exc in ds["exchanges"]:
                
                # Check if the flow is of type biosphere
                if exc["type"] == "biosphere":
                    
                    # If yes, add a new field 'code'. Use the value of field 'flow' as new value.
                    exc["code"] = exc["flow"]
                    
                    # Delete the old field 'flow'
                    del exc["flow"]
                
        return db_var
    
    
    
    # Specify the filepath where the elementary flows are stored --> file is a XML file
    filepath_ElementaryExchanges = XML_FILEFOLDER_PATH.parent / "MasterData" / "ElementaryExchanges.xml"

    def create_XML_biosphere(filepath_ElementaryExchanges: pathlib.Path = filepath_ElementaryExchanges):
        
        # Those packages are imported here specifically because they are only used for this function
        from lxml import objectify
        from bw2io.importers.ecospold2_biosphere import EMISSIONS_CATEGORIES
        from bw2io.strategies import (drop_unspecified_subcategories,
                                      normalize_units,
                                      ensure_categories_are_tuples)
        
        # This funtion has been taken from the Brightway source script and copied here
        # This function extracts the flow data from the XML elementary flow file from ecoinvent
        def extract_flow_data(o):
            
            # For each flow, create a dictionary
            ds = {
                "categories": (
                    o.compartment.compartment.text,
                    o.compartment.subcompartment.text,
                ),
                "code": o.get("id"),
                "CAS number": o.get("casNumber"),
                "name": o.name.text,
                "database": BIOSPHERE_NAME,
                "exchanges": [],
                "unit": o.unitName.text,
            }
            ds["type"] = EMISSIONS_CATEGORIES.get(
                ds["categories"][0], ds["categories"][0]
            )
            return ds

        # Read the XML file and get the roots
        root = objectify.parse(open(filepath_ElementaryExchanges, encoding = "utf-8")).getroot()

        # Extract each elementary flow from the XML file with all corresponding fields
        flow_data = bw2data.utils.recursive_str_to_unicode([extract_flow_data(ds) for ds in root.iterchildren()])

        # We apply some strategies
        flow_data = normalize_units(copy.deepcopy(flow_data))
        flow_data = drop_unspecified_subcategories(copy.deepcopy(flow_data))
        flow_data = ensure_categories_are_tuples(copy.deepcopy(flow_data))
        
        return flow_data


    
    # Function to assign the biosphere categories from the XML files to the exchange data during ecospold import
    def assign_categories_from_XML_to_biosphere_flows(db_var):
        
        # First, extract the categories from the raw XML data
        biosphere_flows = XML_strategies.create_XML_biosphere()
        
        # Create a code-categories mapping for each biosphere flow
        categories_mapping = {m["code"]: m["categories"] for m in biosphere_flows}
        
        # Loop through each inventory
        for ds in db_var:
            
            # Loop through each exchange
            for exc in ds["exchanges"]:
                
                # Check if the current exchange is of type 'biopshere'
                if exc["type"] == "biosphere":
                    
                    # Assign the categories --> compare the current exchange code with the mapping dictionary and find the respective category match
                    exc["categories"] = categories_mapping[exc["code"]]
                    
        return db_var
        
    # Very specific function to the XML/ecospold data from ecoinvent
    # This function transform inventory and exchange dictionaries, so that we have a (almost) identical structure with dictionaries from SimaPro import
    def modify_fields_to_SimaPro_standard(db_var):
        
        # Initialize a mapping dictionary
        tech_subs_mapping = {}
        
        # Loop through each inventory in the database
        for ds in db_var:
            
            # Extract original data of certain fields and store them in variables
            name = ds["name"]
            reference_product = ds["reference product"]
            location = ds["location"]
            
            # Write new fields in SimaPro standard
            ds["activity_name"] = name
            ds["name"] = reference_product[0].upper() + reference_product.lower()[1:] + " {{COUNTRY}}| " + name.lower() + " | " + XML_TO_SIMAPRO_MODEL_TYPE_MAPPING[XML_MODEL_TYPE] + ", " + XML_TO_SIMAPRO_PROCESS_TYPE_MAPPING[XML_PROCESS_TYPE]
            ds["SimaPro_name"] = reference_product[0].upper() + reference_product.lower()[1:] + " {" + location + "}| " + name.lower() + " | Cut-off, U"
            ds["categories"]= SIMAPRO_CATEGORY_TYPE_COMPARTMENT_MAPPING["material"]
            ds["SimaPro_categories"] = SIMAPRO_CATEGORY_TYPE_COMPARTMENT_MAPPING["material"]
            ds["SimaPro_category_type"] = "material"
            ds["synonyms"] = tuple(ds["synonyms"])
            ds["allocation"] = ds.get("properties", {}).get("allocation factor", {}).get("amount", 1)
            ds["output amount"] = float(ds["production amount"] * ds["allocation"])
            ds["code"] = ds["activity"] + "_" + ds["flow"] 
            
            # We try to backward normalize the ecoinvent unit to SimaPro standard
            try:
                ds["SimaPro_unit"] = backward_unit_normalization_mapping[ds["unit"]]
                
            except:
                # If we fail, we need to raise an error
                raise ValueError("Inventory unit (" + str(ds["unit"]) + ") could not be transformed to 'SimaPro_unit' (backward mapped).")

            
            # Delete unused fields
            del ds["flow"], ds["activity"]
            
            # Convert list of classification systems into dictionary of tuples
            # For ecoinvent data, we currently catch only the CPC and ISIC classification systems
            classification_systems = ["CPC", "ISIC"]
            
            # Search for the classification systems and identify the respective tuple
            classifications = [[(n, m[1:]) for n in classification_systems if re.search(n, m[0])][0] for m in ds.get("classifications", []) if re.search("|".join(classification_systems), m[0])]
            
            # If we could successfully identify the classification systems (list is not empty), we add them
            if classifications != []:
                
                # We construct a dictionary for each classification system, where the first tuple element specifies the classification system
                ds |= {system + "_classification": (system,) + cats for system, cats in classifications}
                
                # We use the first classification system in the list of classification systems found as SimaPro_classification
                ds["SimaPro_classification"] = [("ecoinvent",) + ds.get(m + "_classification")[1:] for m in classification_systems if ds.get(m + "_classification") is not None][0]
                
                # We now delete the old classifications key, because we do not need it anymore
                del ds["classifications"]
            
            else:
                # If no classification systems were found, we add an unclassified tuple
                ds["SimaPro_classification"] = ("ecoinvent", "not classified")
             
            # We now extract all the products of the current activity to a list
            products = [m for m in ds["exchanges"] if m["type"] == "production"]
            
            # We check how many products were found. Important: we can only work with activities that have one production exchange. Otherwise, we need to raise an error
            assert len(products) == 1, str(len(products)) + " production exchange(s) found"
            
            # We can now use the only list element as product
            product = products[0]
            
            # We move the 'properties' field from the production exchange to the inventory 'ds' for better accessibility
            if "properties" in product:
                ds["properties"] = product["properties"]
            
            # We loop through each exchange to transform data
            for exc in ds["exchanges"]:
                
                # # Remove 
                # del exc["activity"], exc["flow"]
                
                # Modify the production exchange
                if exc["type"] == "production":
                    
                    # Similar to the 'properties' field above, we loop through specific fields that we have modified in 'ds' and update the fields in the production exchange.
                    fields = ["output amount", "activity_name", "name", "SimaPro_name", "categories", "SimaPro_categories", "unit", "SimaPro_unit", "location"]
                    
                    # Modify fields
                    for field in fields:
                        exc[field] = ds[field]
                    
                    # Modify allocation field manually
                    exc["allocation"] = ds["allocation"] * 100
                    
                    # Modify input field manually
                    exc["input"] = (ds["database"], ds["code"])

                # Delete fields that we do not use anymore.
                # Deletion applies to all types of exchanges
                fields_to_delete = ["classifications", "properties"]
                
                # Loop through each field and delete, if possible
                for del_field in fields_to_delete:
                    
                    # Try to delete. If field is not found, pass
                    try: del exc[del_field]
                    except: pass
                
                # Add certain fields for the biosphere flows
                if exc["type"] == "biosphere":
                    
                    # Add location, if not existing. Take the GLO as default.
                    if "location" not in exc:
                        exc["location"] = "GLO"
                    
                    # Create the SimaPro name from the original name together with the location, separated by a comma (this is the SimaPro standard)
                    if "SimaPro_name" not in exc:    
                        exc["SimaPro_name"] = exc["name"] + (", " + exc["location"] if exc["location"] != "GLO" else "")
                    
                    # Backward map the unit to the SimaPro standard
                    if "SimaPro_unit" not in exc:
                        exc["SimaPro_unit"] = backward_unit_normalization_mapping.get(exc["unit"], exc["unit"])
                    
                    # Backward map the categories to the SimaPro standard
                    if "SimaPro_categories" not in exc:
                        
                        # Extract the current categories
                        categories = exc.get("categories")
                        
                        # Extract the top and sub categories and write to individual variables for mapping afterwards
                        top_cat_orig = categories[0].lower() if categories is not None and len(categories) > 0 else ""
                        sub_cat_orig = categories[1].lower() if categories is not None and len(categories) > 1 else ""
                        
                        # Try to map the top and sub categories 'back' to SimaPro standard using the mapping from Brightway
                        top_cat = BACKWARD_SIMAPRO_BIO_TOPCATEGORIES_MAPPING.get(top_cat_orig, top_cat_orig)
                        sub_cat = BACKWARD_SIMAPRO_BIO_SUBCATEGORIES_MAPPING.get(sub_cat_orig, sub_cat_orig)
                        
                        # If we find the a top category match, we were successful and write it to the biosphere exchange
                        if top_cat != "":
                            exc["SimaPro_categories"] = (top_cat, sub_cat) if sub_cat != "" else (top_cat,)
                            
                        else:
                            # If there is no top category provided and we can not map it, raise error
                            raise ValueError("No top category provided for biosphere flow:\n - 'SimaPro_name' = {}\n - 'unit' = {}\n - 'code' = {}".format(exc["SimaPro_name"], exc["unit"], exc["code"]))
                            
                            # # If we were not successful, we use the original categories as SimaPro categories
                            # exc["SimaPro_categories"] = categories
                        
                    # Add an empty field for the CAS number, if the field is not yet existing
                    if "CAS number" not in exc:
                        exc["CAS number"] = ""
                
                # If the current exchange is of one type, that we don't know yet, let's raise an error to inform us
                # Why do we do that? Because we might need to adjust our scripts!
                known_exchange_types = ["production", "biosphere", "substitution", "technosphere"] 
                assert exc["type"] in known_exchange_types, "Exchange type '" + str(exc["type"]) + "' not known. Consider checking and modifying the strategy 'modify_fields_to_SimaPro_standard'."
            
            # Add current inventory to mapping
            tech_subs_mapping[ds["code"]] = ds
        
        # Specify the fields that we need to add or adapt in technosphere and substitution exchanges
        fields_to_adapt_or_add_in_technosphere_and_substitution_exchanges = ["activity_name",
                                                                             "name",
                                                                             "SimaPro_name",
                                                                             "categories",
                                                                             "SimaPro_categories",
                                                                             "unit",
                                                                             "SimaPro_unit",
                                                                             "location"]
        
        # We need to loop again through it to adapt the technosphere and substitution exchanges
        for ds in db_var:
            for exc in ds["exchanges"]:
                
                # Specify the field names that we want to delete because we do not use them anymore.
                fields_to_delete = ["activity", "flow"]
                
                # We delete the activity and flow codes and go on if the current exchange is not of type 'technosphere' or 'substitution'
                if exc["type"] not in ["technosphere", "substitution"]:
                    
                    # Loop through each field and delete, if possible
                    for del_field in fields_to_delete:
                        
                        # Try to delete. If field is not found, pass
                        try: del exc[del_field]
                        except: pass
                    
                    continue
                
                # We retrieve the inventory that corresponds to the current exchange
                # The inventory stores the new information, that we need to adapt to the fields in the exchange
                map_to = tech_subs_mapping[exc["activity"] + "_" + exc["flow"]]
                
                # We loop through each field specified beforehand that needs an update
                for adapt_field in fields_to_adapt_or_add_in_technosphere_and_substitution_exchanges:
                    
                    # We retrieve the field value from the inventory
                    map_to_value = map_to.get(adapt_field)
                    
                    # We update the exchange field if the value is not None
                    if map_to_value is not None:
                        exc[adapt_field] = map_to_value
                        
                # Loop through each field and delete, if possible
                for del_field in fields_to_delete:
                    
                    # Try to delete. If field is not found, pass
                    try: del exc[del_field]
                    except: pass
        
        return db_var
    
    
    

class migration_strategies():
    
    
    def create_structured_migration_dictionary_from_EXCEL(EXCEL_dataframe: pd.DataFrame):
        
        # Check function input type
        hp.check_function_input_type(migration_strategies.create_structured_migration_dictionary_from_EXCEL, locals())
        
        # Fill NaN values with empty string
        df = EXCEL_dataframe.fillna("")
        
        # Extract columns
        cols = list(df.columns)
        
        # Separate FROM and TO columns
        FROM_cols = [m for m in cols if "FROM_" in m]
        TO_cols = [m for m in cols if "TO_" in m or m == "multiplier"]
        
        # We need information for all FROM cols. Otherwise, the excel is invalid
        if any([[n for n in list(df[m]) if n == ""] != [] for m in FROM_cols]):
            raise ValueError("Invalid migration Excel. Some of the FROM columns contain empty values.")
        
        # Create fields --> all names from the FROM columns
        fields = [m.replace("FROM_", "") for m in FROM_cols]
        
        # Initialize a list for the 'data'
        data = []
        
        # Loop through each row in the dataframe
        for idx, row in df.iterrows():
            
            # The first element is the identifier (uses the FROM cols)
            element_1 = [row[m] for m in FROM_cols]
            
            # The second element specifies which fields should be mapped to which values (uses TO cols)
            element_2 = {(m.replace("TO_", "")): row[m] for m in TO_cols if row[m] != ""}
            
            # Append to the data list
            data += [element_1, element_2]
            
        return {"fields": fields, "data": data} 
        
    
    def create_migration_mapping(JSON_dict: dict):
        
        # Check function input type
        hp.check_function_input_type(migration_strategies.create_migration_mapping, locals())
        
        # Raise error if format is wrong
        if "data" not in JSON_dict or "fields" not in JSON_dict:
            raise ValueError("Invalid migration dictionary. 'data' and 'fields' need to be provided.")
        
        # 'data' needs to be a list
        if not isinstance(JSON_dict["data"], list):
            raise ValueError("Invalid migration dictionary. 'data' needs to be a list.")
        
        # More specifically, 'data' needs to be a list of lists which contains a list as a first element and a dictionary as a second element
        if not all([True if isinstance(m[0], list) and isinstance(m[1], dict) else False for m in JSON_dict["data"]]):
            raise ValueError("Invalid migration dictionary. 'data' needs to be a list of list which contains a list as a first element and a dictionary as a second element.")
        
        # 'fields' needs to be a list
        if not isinstance(JSON_dict["fields"], list):
            raise ValueError("Invalid migration dictionary. 'fields' needs to be a list.")
        
        # 'fields' needs to be a list of strings
        if not all([isinstance(m, str) for m in JSON_dict["fields"]]):
            raise ValueError("Invalid migration dictionary. 'fields' needs to be a list of strings.")
             
        # Initialize mapping dictionary
        mapping = {}

        # Construct mapping dictionary. Looping through all elements
        for FROM_orig, TO_orig in JSON_dict["data"]:
            
            # First, we need to adapt the FROMs and the TOs
            # We use ast literal to evaluate the elements and merge them to the corresponding Python type
            # We also check for lists and replace them with the immutable tuple types
            
            # Initialize new variables
            FROM_tuple = ()
            TO_dict = {}
            
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
            
        return tuple(JSON_dict["fields"]), mapping

#%%
# # Opening JSON file
# fp = "/Users/cedricfurrer/Documents/Brightway_data_v2/LCI_Databases/ECO_fromXML/mapping_XML_to_SimaPro/biosphere/Output/biosphere_migration_data.json"
# f = open(fp)
 
# # returns JSON object as 
# # a dictionary
# JSON = json.load(f)
 
# # Closing file
# f.close()
# import ast
# aaa = migration_strategies.create_migration_mapping(JSON)
# aaaa = ast.literal_eval("('water', 'ground-')")

#%%

    
    def apply_migration_mapping(db_var, fields: tuple, migration_mapping: dict):
        
        # Check function input type
        hp.check_function_input_type(migration_strategies.apply_migration_mapping, locals())
        
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
    
    
    
    
    def migrate_from_JSON_file(db_var, JSON_migration_filepath: pathlib.Path | None = None):
        
        # Check function input type
        hp.check_function_input_type(migration_strategies.migrate_from_JSON_file, locals())
        
        # Return the original database if no migration is specified
        if JSON_migration_filepath is None:
            return db_var
        
        # Opening JSON file
        f = open(JSON_migration_filepath)
         
        # returns JSON object as 
        # a dictionary
        JSON = json.load(f)
         
        # Closing file
        f.close()
        
        # Create mapping of the JSON custom migration file
        fields, migration_mapping = migration_strategies.create_migration_mapping(JSON_dict = JSON)
        
        # Apply migration
        new_db_var = migration_strategies.apply_migration_mapping(db_var = db_var,
                                                                  fields = fields,
                                                                  migration_mapping = migration_mapping)
                
        return new_db_var
    
    
    def migrate_from_EXCEL_file(db_var, EXCEL_migration_filepath: pathlib.Path | None = None):
        
        # Check function input type
        hp.check_function_input_type(migration_strategies.migrate_from_EXCEL_file, locals())
        
        # Return the original database if no migration is specified
        if EXCEL_migration_filepath is None:
            return db_var
        
        # Read excel file
        EXCEL = pd.read_excel(EXCEL_migration_filepath)
        
        # Convert Excel file to dictionary with the migration structure
        JSON_dict = migration_strategies.create_structured_migration_dictionary_from_EXCEL(EXCEL_dataframe = EXCEL)
        
        # Create mapping of the JSON custom migration file
        fields, migration_mapping = migration_strategies.create_migration_mapping(JSON_dict = JSON_dict)
        
        # Apply migration
        new_db_var = migration_strategies.apply_migration_mapping(db_var = db_var,
                                                                  fields = fields,
                                                                  migration_mapping = migration_mapping)
                
        return new_db_var
        
        
        
    
    def transform_dictionary_into_custom_migration_dictionary(transform_dict: dict):
        
        """ Transform a dictionary into a dictionary with specific Brightway2 custom migration format
        Dictionary keys containing string fragments such as "FROM", "TO", "multiplier" and "strategy" will be used. Other keys will be omitted! 
        This function is the inverse of 'transform_custom_migration_dictionary_into_dictionary'
        
        Examples:
        ---------
            >>> transform_dictionary_into_custom_migration_dictionary({"FROM_name": ["substance A"], "TO_name": ["A"], "FROM_categories": [("air",)], "TO_categories": [("air", "stratosphere")], "multiplier": [1], "strategy": ["some_strategy"]})
            {"fields": ("name", "categories"), "data": [[("substance A", ("air",)), {"name": "A", "categories": ("air", "stratosphere"), "multiplier": 1, "strategy": "some_strategy"}]]}
        
        Parameters
        ----------
        transform_dict : dict
            A dictionary containing key/value pairs where dict.values are lists with equal length!
        
        Returns
        -------
        final : dict
            A custom migration dictionary in the specific Brightway2 format.
        """
        
        # Check function input type
        hp.check_function_input_type(migration_strategies.transform_dictionary_into_custom_migration_dictionary, locals())
        
        # Get the dictionary keys
        keys = transform_dict.keys()

        # Extract, if there is at least one FROM-key
        check_for_FROM = [m for m in keys if bool(re.search("FROM_", m))] == []
        
        # Make sure, there is at least one "FROM_"-key. If not, return None and print a statement
        if check_for_FROM:
            print("\nThe dictionary provided ('transform_dict') can not be transformed into a Brightway2 custom migration dictionary because no 'FROM' key was provided. Current keys provided are:\n" + str(list(keys)) + "\n\nNone is returned instead.")
            return None
        
        # Ensure that input values are of type list, if not raise error
        for key in keys:
            if not isinstance(transform_dict[key], list):
                raise TypeError("The values of the keys in 'transform_dict' need to be passed as type of <class 'list'>. However, the current type of the key '" + str(key) + "' is " + str(type(key)) + ". Transform!")
        
        # Get the length of lists (from dict.values)
        len_keys = [len(transform_dict[m]) for m in keys]
        
        # Check whether all lists from the dictionary are of the same length or not. If not, an error is thrown.
        if len(set(len_keys)) > 1:
            raise ValueError("Lists of the keys of 'transform_dict' are not of equal length." + str([str(a) + " = " + str(b) for a, b in zip(keys, len_keys)]))
        
        # Extract all the keys, where the pattern 'FROM_' is found. Those keys will be the 'fields'.
        FROM_keys = [m for m in transform_dict.keys() if re.search("^FROM_.*", m)] 
        fields = tuple([re.match("(^FROM_)(.*)", m)[2] for m in FROM_keys])
        FROM_data = None
        
        # Initialize a list for warning message
        warning_message_None = []
        warning_message_NaN = []
        
        # Loop through each FROM key and merge the dictionary value data to a combined tuple
        for i in FROM_keys:
            
            # Get the values list of the 'i' key
            key_data_orig = [m for m in transform_dict[i]]
            
            # Check whether a list element of 'key_data_orig' contains a None or a NaN.
            bool_None = None in key_data_orig
            bool_NaN = hp.check_if_value_is_nan(key_data_orig)
            
            # If a None has been detected, add the name of the FROM_key to the list which will be printed afterwards
            if bool_None:
                warning_message_None += [i]
                
            # If a NaN has been detected, add the name of the FROM_key to the list which will be printed afterwards
            if bool_NaN:
                warning_message_NaN += [i]
            
            # Write data to dictionary 'FROM_data'
            if FROM_data is None:
                FROM_data = [[m] for m in key_data_orig]
            else:
                FROM_data = [o + [p] for o, p in zip(FROM_data, key_data_orig)]
        
        # Warning for detected None's
        if warning_message_None != []:
            print("\nWarning: One or several of the inputs are None for dictionary key(s):\n" + str(list(set(warning_message_None))))
            
        # Warning for detected None's
        if warning_message_NaN != []:
            print("\nWarning: One or several of the inputs are NaN for dictionary key(s):\n" + str(list(set(warning_message_NaN))))
        
        # Extract all the keys, where the pattern 'TO_', 'multiplier' and 'strategy' is found. The values in those keys will serve as the Brightway2 migration data.
        TO_keys = [m for m in transform_dict.keys() if re.search("^TO_.*|multiplier|strategy", m)] 
        
        # Make an integer with the maximum number of list length (to loop through afterwards)
        int_for_loop = int(list(set(len_keys))[0])
        TO_dict = []
        
        # Loop through each list element (ii) of all keys in 'transform_dict'
        for ii in range(0, int_for_loop):
            
            # Remove the "TO_" from the key names, if existing
            keys_without_TO = [re.match("^(TO_)?(.*|multiplier|strategy)$", e)[2] for e in TO_keys]
            
            # Write a list containing the values of each key
            data_without_TO_orig = [transform_dict[y][ii] for y in TO_keys]
            
            # Clean that list by replacing NaN and None with None
            data_without_TO_cleaned = [x if not hp.check_if_value_is_nan(x) and x is not None else None for x in data_without_TO_orig]
            
            # Write dictionary with key/value pairs and append to 'TO_dict'
            TO_data_curr = {k: d for k, d in zip(keys_without_TO, data_without_TO_cleaned) if d is not None}
            TO_dict.append(TO_data_curr)
        
        
        # Create a dictionary for FROM
        FROM_dict = [{k: v for k, v in zip(fields, m)} for m in FROM_data]
        
        # Goal: Compare FROM and TO. Compare each key from FROM_dict and TO_dict. If it is the same, then replace the value with None
        for iii in range(0, len(TO_dict)):
            
            # Extract current loop elements (iii)
            TO_dict_curr = TO_dict[iii]
            FROM_dict_curr = FROM_dict[iii]
            
            # For each element 'iii', compare the key/value pairs in the 'TO_dict' with the ones from 'FROM_dict'
            for iiii in TO_dict_curr.keys():
                
                # Create bool variable which returns True if it is the same and False if it is different.
                bool_validation = TO_dict_curr.get(iiii) == FROM_dict_curr.get(iiii)
                
                # Replace the dict.value in 'TO_dict' if it is the same as in 'FROM_dict'
                if bool_validation:
                    TO_dict[iii][iiii] = None
                
        # Remove None or NaN from the TO_dict
        TO_dict_final = [{k: v for k, v in m.items() if v is not None and not hp.check_if_value_is_nan(v)} for m in TO_dict]
        
        # The variable 'FROM_data' is currently a list, but should be a tuple.
        final_data = [[tuple(a), b] for a, b in zip(FROM_data, TO_dict_final) if b != {}]
        
        # If 'final_data' is an empty list, replace with None
        if final_data == []:
            final_data = None
        
        # Combine data into the specific final dictionary to be returned
        final = {"fields": fields, "data": final_data}
        
        return final



    def transform_custom_migration_dictionary_into_dictionary(custom_migration_dict: dict):
        
        """ Transform a dictionary with specific Brightway2 custom migration format into a normal list dictionary
        "FROM_" and "TO_" will automatically be added to the dictionary keys except for "multiplier" and "strategy".
        This function is the inverse of 'transform_dictionary_into_custom_migration_dictionary'
        
        Examples:
        ---------
            >>> transform_custom_migration_dictionary_into_dictionary({"fields": ("name", "categories"), "data": [[("substance A", ("air",)), {"name": "A", "categories": ("air", "stratosphere"), "multiplier": 1, "strategy": "some_strategy"}]]})
            {"FROM_name": ["substance A"], "FROM_categories": [("air",)], "TO_name": ["A"], "TO_categories": [("air", "stratosphere")], "multiplier": [1], "strategy": ["some_strategy"]}
            
        Parameters
        ----------
        custom_migration_dict : dict
            A custom migration dictionary in the specific Brightway2 format.
        
        Returns
        -------
        final_dict : dict
            A dictionary containing key/value pairs where dict.values are lists with equal length!
            
        """
        
        # Check function input type
        hp.check_function_input_type(migration_strategies.transform_custom_migration_dictionary_into_dictionary, locals())
        
        # Get fields list
        get_FROM_fields = custom_migration_dict.get("fields")
        
        # Get fields data list
        get_FROM_data = [m[0] for m in custom_migration_dict.get("data")]
        
        # # Replace empty list or NaN values with None
        # get_FROM_data = [[None if m == [] or hp.check_if_value_is_nan(m) else m for m in n] for n in get_FROM_data_orig]
        
        # First, create a dictionary where the fields will be written as keys and the fields data will be written as value to the dictionary.
        # Note: fields will be extended with "FROM_" at the beginning.
        FROM_dict = {}
        for i in range(0, len(get_FROM_fields)):
            FROM_dict[str("FROM_" + get_FROM_fields[i])] = [m[i] for m in get_FROM_data]
        
        # Get additional or changed fields from migration
        get_TO_fields_orig = [list(m[1].keys()) for m in custom_migration_dict.get("data")]
        get_TO_fields = set([item for sublist in get_TO_fields_orig for item in sublist] + list(get_FROM_fields))
        
        # Get additional or changed fields data from migration
        get_TO_data = [m[1] for m in custom_migration_dict.get("data")]

        # Second, create a dictionary where the fields from migration will be written as keys and the fields data from migration will be written as value to the dictionary.
        # Note: fields from migration will be extended with "TO_" at the beginning.
        TO_dict = {}
        for i in get_TO_fields:
            
            # Get data to append to 'TO_dict'
            data_to_append_orig = [m.get(i) for m in get_TO_data]
            
            # # Replace empty list or NaN values with None
            # data_to_append_cleaned = [None if n == [] or hp.check_if_value_is_nan(n) else n for n in data_to_append_orig]
            
            # Add key to dictionary
            TO_dict[str("TO_" + i)] = data_to_append_orig
        
        # Rename keys 'TO_multiplier' and 'TO_strategy'
        if "TO_multiplier" in get_TO_fields:
            TO_dict["multiplier"] = TO_dict.pop("TO_multiplier")
        
        if "TO_strategy" in get_TO_fields:
            TO_dict["strategy"] = TO_dict.pop("TO_strategy")
        
        # Append both dictionary
        final_dict = FROM_dict | TO_dict
        
        return final_dict



    def export_custom_migration_dictionary_to_JSON(export_dict: dict,
                                                   name_export_json: str,
                                                   output_path: str | pathlib.Path):
        
        """ Export a Brightway2 custom migration dictionary to a JSON file 
        
        Format of Brightway2 custom migration dictionary:
            [{"fields": ("name", "categories", "unit", ...)}, "data": [("substance_A", ("air", ), "litre", ...), {"name": "substance_B", "unit": "kilogram", ...}]]
        
        Parameters
        ----------
        export_dict : Brightway2 custom migration dictionary
            Dictionary which should be exported.
        
        name_export_json : str
            The name of the JSON file that will be written. Needs the file extension ".json" --> e.g. "migration_export.json"
        
        output_path: str | pathlib.Path
            The path to where the file should be written.
        
        Returns
        -------
        JSON
            A JSON file named as 'name_export_json' is exported to the 'output_path'.
        """
        
        # Check function input type
        hp.check_function_input_type(migration_strategies.export_custom_migration_dictionary_to_JSON, locals())
        
        with open(os.path.join(str(output_path), name_export_json), 'w') as fp:
            json.dump(export_dict, fp)



    def export_custom_migration_dictionary_to_XLSX(export_dict: dict,
                                                   name_export_xlsx: str,
                                                   output_path: str | pathlib.Path,
                                                   xlsx_sheet_name: str):
        
        """ Export a Brightway2 custom migration dictionary to a XLSX file 
        
        Format of Brightway2 custom migration dictionary:
            [{"fields": ("name", "categories", "unit", ...)}, "data": [("substance_A", ("air", ), "litre", ...), {"name": "substance_B", "unit": "kilogram", ...}]]
        
        Parameters
        ----------
        export_dict : Brightway2 custom migration dictionary
            Dictionary which should be exported.
        
        name_export_xlsx : str
            The name of the XLSX file that will be written. Needs the file extension ".xlsx" --> e.g. "migration_export.xlsx"
        
        xlsx_sheet_name : str
            The sheet name to where the data should be written.
        
        output_path: str | pathlib.Path
            The path to where the file should be written.
        
        Returns
        -------
        XLSX
            A XLSX file named as 'name_export_xlsx' is exported to the 'output_path'.
        """
        
        # Check function input type
        hp.check_function_input_type(migration_strategies.export_custom_migration_dictionary_to_XLSX, locals())
        
        # Convert to pathlib Path first if provided as string
        if isinstance(output_path, str):
            output_path = pathlib.Path(output_path)
            
        # Transform custom migration dictionary into a ordinary list dictionary for export
        export_dict_translated = migration_strategies.transform_custom_migration_dictionary_into_dictionary(export_dict)
        
        # Write dictionary to XLSX file
        xlsx_df = pd.DataFrame(export_dict_translated)
        xlsx_df.to_excel(filename = (output_path / name_export_xlsx), df = xlsx_df, sheet_name = xlsx_sheet_name)

    
    def import_custom_migration_dictionary(import_path: str | pathlib.Path,
                                           sheetname: str | None = None):
        
        """ Import a custom migration from a XLSX into a specific Brightway2 custom migration dictionary format.
        
        The function provides the data import as well as some type conversion.
            - "categories" are converted from str to tuple
            - "multiplier" is converted to float
        
        Conversion of import data into specific Brightway2 custom migration format is done by the function 'transform_dictionary_into_custom_migration_dictionary'.
        
        Parameters
        ----------
        import_path : str | pathlib.Path
            The file path of the XLSX custom migration file to be read in.
        
        sheetname : str, optional
            The name of the sheet to be read in. The default is set to None but needs to be one of the following: technosphere, substitution or biosphere.
        
        Returns
        -------
        import_data : dict
            A dictionary with the biosphere custom migration in the specific Brightway2 format.
        """
        
        # Check function input type
        hp.check_function_input_type(migration_strategies.import_custom_migration_dictionary, locals())

        # Import biosphere migration data frame from import path. If not specified, the general file 'data_custom_migration.xlsx' is imported
        custom_migration_import = pd.read_excel(import_path, sheet_name = sheetname)
        
        # Raise error if sheet name is not provided as input or if the sheet name is None of the ones that should be
        if sheetname is None or sheetname not in ["biosphere", "technosphere", "substitution"]:
            raise ValueError("'Sheetname' is currently '" + str(sheetname) + "' but needs to be one of the following: 'technosphere', 'substitution' or 'biosphere'.")
        
        # Initialize a dictionary
        input_data = {}    
        
        # Loop through each row in the data frame
        for column in custom_migration_import:
            
            # For biosphere, the imported categories need to be converted from str to tuple
            if re.search("categories", column):
                data_curr = [ast.literal_eval(m) if not pd.isnull(m) and m is not None else None for m in list(custom_migration_import[column].values)]
            
            # Make sure that multiplier is of type float
            elif column == "multiplier":
                data_curr = [float(m) if not pd.isnull(m) and m is not None else None for m in list(custom_migration_import[column].values)]
            
            # Skip column 'side_note'
            elif column == "side_note":
                continue
            
            # Else just use data as is from import
            else:
                data_curr = [m if not pd.isnull(m) and m is not None else None for m in list(custom_migration_import[column].values)]
            
            # Append key/value pair to dictionary 'input_data'
            input_data[column] = data_curr
        
        # Add strategy name to dictionary if not already available.
        if "strategy" not in input_data.keys():
            
            # Get first the length of the lists in the dictionary.
            len_keys = [len(input_data[m]) for m in input_data.keys()][0]
            
            # Add strategy name
            input_data["strategy"] = [sheetname] * len_keys
        
        # Transform dictionary into specific Brightway2 custom migration format
        import_data = migration_strategies.transform_dictionary_into_custom_migration_dictionary(input_data)
        
        return import_data



#%%
# bw.projects.set_current("AgriFood Economy v1")
# bio = {(m["database"], m["code"]): m.as_dict() for m in bw.Database("biosphere3")}
# eco = {(m["database"], m["code"]): m.as_dict() for m in bw.Database("ecoinvent v3.9.1 cut-off - from SimaPro - Unit")}

# example_db = [{"name": "A",
#                "unit": "kilogram",
#                "location": "RER",
#                "exchanges": [
#                    {"type": "biosphere", "name": "Transformation, to annual crop", "top_category": "natural resource", "sub_category": "", "unit": "square meter", "location": "GLO", "amount": 23.4},
#                    {"type": "biosphere", "name": "Transformation, to annual crop", "top_category": "natural resource", "sub_category": "aa", "unit": "square meter", "location": "GLO", "amount": 3},
#                    {"type": "technosphere", "name": "Electricity, low voltage {{COUNTRY}}| market for electricity, low voltage | Cut-off, U", "unit": "kilowatt hour", "location": "DE", "amount": 0, "uncertainty type": 0}
#                    ]}]

# reg_example_db = exchange_strategies.regionalize(copy.deepcopy(example_db),
#                                                  regionalize_biosphere = True,
#                                                  regionalize_technosphere = True,
#                                                  loaded_biosphere_databases = [bio],
#                                                  loaded_technosphere_databases = [eco],
#                                                  local_output_path = None,
#                                                  write_mapping_xlsx = False,
#                                                  tag_regionalized_flow = True,
#                                                  )

# print("\n".join([str(m["location"] + " --> " + n["location"]) for m, n in zip(example_db[0]["exchanges"], reg_example_db[0]["exchanges"])]))


    #%%

    # data = [{"type": "process",
    #          "code": "dfkj23409sdfj",
    #          "simapro metadata": {"Category type": "material"},
    #          "database": "Agribalyse",
    #          "exchanges": [{"type": "production",
    #                         "name": "Inventory A {GLO} U",
    #                         "categories": ("Products",),
    #                         "allocation": 40,
    #                         "amount": 1,
    #                         "unit": "kg",},
                           
    #                        {"type": "production",
    #                         "name": "Inventory B {DE} U",
    #                         "categories": ("Products",),
    #                         "allocation": 50,
    #                         "amount": 2,
    #                         "unit": "kg",},
                           
    #                        {"type": "production",
    #                         "name": "Inventory B {DE} U",
    #                         "categories": ("Products",),
    #                         "allocation": 50,
    #                         "amount": 3,
    #                         "unit": "kg",},
                           
    #                        {"type": "technosphere",
    #                         "name": "Electricity, low voltage {CH}| market for electricity, low voltage | Cut-off, U",
    #                         "categories": ("Materials/fuels"),
    #                         "unit": "kWh",
    #                         "amount": 1},
                           
    #                        {"type": "biosphere",
    #                         "name": "Water",
    #                         "categories": ("Emissions into air", ""),
    #                         "unit": "kg",
    #                         "amount": 1}
    #                        ]}]


    # see1 = SimaPro_LCI_import_strategies(copy.deepcopy(data),
    #                                      strategy_remove_duplicates=False)

