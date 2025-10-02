import pathlib
here: pathlib.Path = pathlib.Path(__file__).parent

if __name__ == "__main__":
    import os
    os.chdir(here.parent)

import re
import pathlib
import pandas as pd
from bw2io.units import normalize_units as normalize_units_function


#%% Create correspondence mapping from ecoinvent files

def create_correspondence_mapping(path_to_correspondence_files: pathlib.Path,
                                  model_type: str,
                                  map_to_version: tuple,
                                  output_path: pathlib.Path) -> list[dict]:

    # Pattern to extract the version names from the excel correspondence files
    correspondence_file_pattern: str = ("^Correspondence\-File\-v"
                                        "(?P<FROM_version>[0-9\.]+)"
                                        "\-v"
                                        "(?P<TO_version>[0-9\.]+)"
                                        "(\.xlsx)$"
                                        )
    
    # Apply regex pattern
    version_list_orig: list[tuple] = [(k.name, re.match(correspondence_file_pattern, k.name)) for k in path_to_correspondence_files.rglob("*.xlsx")]
    
    # Raise error, if no files were found
    if version_list_orig == []:
        raise ValueError("No correspondence files detected. Correspondence files should be of type '.xlsx' and be in the format 'Correspondence-File-vX.x-v.Y.y' (e.g. 'Correspondence-File-v3.9.1-v3.10') and contain one of the sheet names 'cutoff', 'apos' or 'consequential'.")
    
    # Raise error, if model type is None of the defaults
    if model_type not in ["cutoff", "apos", "consequential"]:
        raise ValueError("Current model type '{}' is not allowed. Use one of the following: 'cutoff', 'apos' or 'consequential'".format(model_type))
    
    # Raise error, if the version tuple is of wrong format
    wrong_version_tuple: list = [m for m in map_to_version if not isinstance(m, int)]
    if wrong_version_tuple != []:
        raise ValueError("Version tuple only accepts integers as items, e.g. (3, 10). Currently, the following version tuple is provided: '{}'".format(map_to_version))
    
    # Convert version strings to tuples and sort the list so that the highest version is coming first
    version_list: list[dict] = sorted([dict({k: tuple(int(n) for n in v.split(".")) for k, v in m.groupdict().items()}, **{"file": a}) for a, m in version_list_orig if m is not None], key = lambda x: x["TO_version"], reverse = False)
    
    # Empty list to store header names
    all_headers: list = []
    
    # Names of sheets in the excel file depending on the ecoinvent model type
    sheet_name_mapping: dict = {"cutoff": ["Cut-off", "Cut-Off", "cut-off", "cutoff"],
                                "apos": ["apos", "APOS"],
                                "consequential": ["consequential", "Consequential", "conseq"]}
    
    # Sheet names to search for in the excel files
    sheetnames = sheet_name_mapping[model_type]
    
    # Relevant columns in the excel files
    # ID_cols = ["activityID", "Activity UUID"]
    actID_cols = ["activityID", "Activity UUID"]
    prodID_cols = ["Product UUID"]
    ref_cols = ["product name", "Product Name"]
    act_cols = ["activityName", "Activity Name"]
    loc_cols = ["geography", "Geography"]
    unit_cols = ["unit", "Unit"]
    mult_cols = ["amount of the replacement", "Replacement amount", "replacement amount", "replacement share"]
    
    # Initialize
    all_data: dict = {}
    
    # # Specify to which version the mapping should apply to
    # map_to_version = (3, 10)
    
    # Loop through each excel file (= version) independently
    for item in version_list:
        
        # Write relevant fields from current 'item' variable as separate variables for easier handling
        filename: str = item["file"]
        FROM_version: tuple = item["FROM_version"]
        TO_version: tuple = item["TO_version"]
        
        # Go on if the current version is greater than the one we want to map to
        if FROM_version > map_to_version:
            continue
        
        # Print where we are
        print(filename)
        
        # Make a version string, based on the version tuple from the element
        FROM_version_str: str = ".".join([str(m) for m in FROM_version])
        TO_version_str: str = ".".join([str(m) for m in TO_version])
        
        # Read the correspondence excel file for the current versions
        excel: dict[pd.DataFrame] = pd.read_excel(path_to_correspondence_files / filename, sheet_name = None)
        
        # Only use the sheet that we want to use --> either cut-off, apos or consequential
        # Because the files use different names for the systems, we need to search for the appropriate one
        sheet: list[str] = [m for m in sheetnames if m in excel]
        
        # If we could not identify the excel sheet for the current system, we raise an error
        assert len(sheet) == 1, "None or more than one sheet name matches"
        
        # The raw data to use
        df_orig: pd.DataFrame = excel[sheet[0]]
        
        # We need to adjust the header
        # We need to skip the first rows. First, we identify where the header is located
        # We can find the header, as the first line, where no NaN values are found.
        # We add a column to the dataframe. Each row is checked whether any value in that row is NaN. If yes, it is True, otherwise it is False.
        df_orig["_header_selection"] = df_orig.isnull().any(axis = 1)
        
        # We locate the header as the first row, where all values are not empty (NaN)
        new_header_idx: int = [idx for idx, m in df_orig.iterrows() if not m["_header_selection"]][0]
        
        # We extract the new header based on the location found beforehand
        new_header_orig: list[str] = list(df_orig.loc[new_header_idx])
        
        # We identify the duplicated headers
        doubles: list[str] = [m for m in new_header_orig if new_header_orig.count(m) > 1]
        
        # Initialize new lists
        seen: list = []
        new_header: list = []
        
        # Loop through each old column name
        for head in new_header_orig:
            
            # Check if the current column/head appears twice (or more) and whether it has already seen or not in that loop
            if head in doubles and head in seen:
                
                # If it is a double and it has been seen, that means the current header is the one belonging to the 'TO'
                new_header += [head + "_" + TO_version_str]
            
            elif head in doubles:
                
                # If it has NOT been seen but it is a double, it is the one from 'FROM'
                new_header += [head + "_" + FROM_version_str]
                
            else:
                # Otherwise, we leave the head as it is and append it to the list
                new_header += [head]
            
            # At the end of the loop, we update the variables
            seen += [head]
            all_headers += [head]
        
        
        # We now remove the first rows which we don't need anymore
        df: pd.DataFrame = df_orig.copy().fillna("").iloc[new_header_idx+1:len(df_orig)]
        
        # Add new headers to the dataframe
        df.columns = new_header
        
        # Define function to find column names for the different types of information that we need
        def find_colnames(possible_cols: list, raise_error_if_no_columns_were_found: bool = True) -> (str | None, str | None):
            
            # For each possible column, check if it is available in the new header and gather in the list
            found = [(m + "_" + FROM_version_str, m + "_" + TO_version_str) for m in possible_cols if m + "_" + FROM_version_str in new_header and m + "_" + FROM_version_str in new_header]
            
            # Raise error, if specified and return
            if raise_error_if_no_columns_were_found:
                
                # Raise error if no columns were identified
                assert len(found) > 0, "No columns found. Used possible columns '" + str(possible_cols) + "'. Error occured in file '" + str(filename) + "'" 
            
                # Return the results from the first element --> FROM and TO
                return found[0][0], found[0][1]
            
            else:
                # If column names have been found, return the results from the first element --> FROM and TO
                if len(found) > 0:
                    return found[0][0], found[0][1]
                
                # Otherwise return None's
                else:
                    return None, None
                
        
        # Extract the column names for the current excel file
        FROM_colname_actID, TO_colname_actID = find_colnames(actID_cols, True)
        FROM_colname_prodID, TO_colname_prodID = find_colnames(prodID_cols, False)
        FROM_colname_ref, TO_colname_ref = find_colnames(ref_cols, True)
        FROM_colname_act, TO_colname_act = find_colnames(act_cols, True)
        FROM_colname_loc, TO_colname_loc = find_colnames(loc_cols, True)
        FROM_colname_unit, TO_colname_unit = find_colnames(unit_cols, True)
        
        # Extract column for the multiplier separately
        colname_mult_orig: list[str] = [m for m in mult_cols if m in new_header]
        
        # Raise error if the multiplier column was not found
        assert len(colname_mult_orig) > 0, "No multiplier column found. Used possible columns '" + str(mult_cols) + "'. Error occured in file '" + str(filename) + "'"
        
        # Use the first element as column name for multiplier
        colname_mult: str = colname_mult_orig[0]
        
        # Loop through all entries in the dataframe
        for idx, row in df.iterrows():
            
            # Write column values to variables
            FROM_actID, TO_actID = row.get(FROM_colname_actID), row.get(TO_colname_actID)
            FROM_prodID, TO_prodID = row.get(FROM_colname_prodID), row.get(TO_colname_prodID)
            FROM_ref, TO_ref = row.get(FROM_colname_ref), row.get(TO_colname_ref)
            FROM_act, TO_act = row.get(FROM_colname_act), row.get(TO_colname_act)
            FROM_loc, TO_loc = row.get(FROM_colname_loc), row.get(TO_colname_loc)
            FROM_unit_orig, TO_unit_orig = row.get(FROM_colname_unit), row.get(TO_colname_unit)
            multiplier = row.get(colname_mult)
            
            # Use normalize unit function from Brightway
            FROM_unit, TO_unit = normalize_units_function(FROM_unit_orig), normalize_units_function(TO_unit_orig)
            
            # Set multiplier to 1 if it is a strange string like 'NEW' or 'not recommended share' or simply an empty string
            if not isinstance(multiplier, float | int):
                multiplier = 1
            
            # We need to do the inverse for the multiplier
            # According to the documentation from correspondence file v3.4/v3.5 (comment of cell N3)
            # If the multiplier is 1 it means the eiv3.4 activity is replaced by exactly one activity in eiv3.5.
            # If the multiplier is different (below one) it represents the share of the replacement activity.
            # E.g. if the value here is 0.25 then 25% of the eiv3.4 activity should be replaced by the specific activity in eiv3.5.
            # The share of the eiv3.4 matches of course sum up to 1.
            multiplier = 1 / multiplier if multiplier != 0 else 0
            
            # Create FROM_ID
            FROM_ID = (
                       FROM_version,
                       FROM_actID,
                       FROM_prodID,
                       FROM_ref,
                       FROM_act,
                       FROM_loc,
                       FROM_unit)
            
            # Create TO_ID
            TO_ID = (
                     TO_version,
                     TO_actID,
                     TO_prodID,
                     TO_ref,
                     TO_act,
                     TO_loc,
                     TO_unit
                     )
            
            # Create ID
            ID = FROM_ID + TO_ID
            
            # Raise error if key is already available
            assert ID not in all_data, "Key is already present. Error occured for key '" + str(ID) + "' when reading file '" + str(filename) + "'"
                
            # Append to data dictionary
            all_data[ID]: dict = {
                                "FROM_version": FROM_version,
                                "FROM_activity_UUID": FROM_actID,
                                "FROM_product_UUID": FROM_prodID,
                                "FROM_activity": FROM_act,
                                "FROM_reference_product": FROM_ref,
                                "FROM_location": FROM_loc,
                                "FROM_unit": FROM_unit,
                                "multiplier": multiplier,
                                "TO_version": TO_version,
                                "TO_activity_UUID": TO_actID,
                                "TO_product_UUID": TO_prodID,
                                "TO_activity": TO_act,
                                "TO_reference_product": TO_ref,
                                "TO_location": TO_loc,
                                "TO_unit": TO_unit
                                }
            
    # Create dataframe with standardized data
    standardized: pd.DataFrame = pd.DataFrame(list(all_data.values()))
    
    # Write dataframe
    standardized.to_excel(output_path / "correspondence_files_standardized.xlsx")
    
    # Initialize variable to store temporary mapping file (for mapping of datasets within versionss)
    combined_mapping: dict = {}
    
    # !!! TODO from here
    # Two sets of fields can be used to uniquely identify the activities of ecoinvent
    UUID_relevant_fields: list = ["activity_UUID", "product_UUID"]
    name_relevant_fields: list = ["activity", "reference_product", "location", "unit"]
    
    # Create temporary mapping file. Loop through all entries found beforehand
    for _, item in all_data.items():
        
        # Check if all FROM and TO fields are provided. Note: the product UUID can be empty sometimes because it was introduced into the correspondence files only later
        # That means, those UUID's with only the activity UUID are skipped
        empty_FROM_fields_for_UUID: bool = any([item["FROM_" + m] == "" or item["FROM_" + m] is None for m in UUID_relevant_fields])
        empty_FROM_fields_for_name: bool = any([item["FROM_" + m] == "" or item["FROM_" + m] is None for m in name_relevant_fields])
        empty_TO_fields_for_UUID: bool = any([item["TO_" + m] == "" or item["TO_" + m] is None for m in UUID_relevant_fields])
        empty_TO_fields_for_name: bool = any([item["TO_" + m] == "" or item["TO_" + m] is None for m in name_relevant_fields])
        
        # Extract the FROM version of the current item
        FROM_version: tuple = item["FROM_version"]
        
        # Write the TO fields of the item to a separate variable. We use it afterwards for the mapping dictionary, to indicate to where the value is mapped to
        TO_item: dict = {k: v for k, v in item.items() if "TO_" in k or "multiplier" in k}
        
        # We initialize a dictionary with the version if not yet done    
        if FROM_version not in combined_mapping:
            combined_mapping[FROM_version]: dict = {}
        
        # Internal mapping file is only valid if all FROM and TO fields are provided.
        # First, we add the FROM UUIDs as keys
        if not empty_FROM_fields_for_UUID and not empty_TO_fields_for_UUID:
            
            # Extract the activity and product UUIDs and use it as unique key
            key_UUID: tuple = tuple([item["FROM_" + m] for m in UUID_relevant_fields])
            
            # Check if the key has already been registered.
            if key_UUID not in combined_mapping[FROM_version]:
                
                # If no, we initialize a new key/value pair
                combined_mapping[FROM_version][key_UUID]: list[dict] = [TO_item]
                
            else:
                # Otherwise, we add to the existing key/value pair
                combined_mapping[FROM_version][key_UUID] += [TO_item]
            
        
        # Second, we add the activity name, reference product, unit and location combination as keys
        if not empty_FROM_fields_for_name and not empty_TO_fields_for_name:
            
            # Extract the fields
            key_name: tuple = tuple([item["FROM_" + m].lower() if isinstance(item["FROM_" + m], str) else item["FROM_" + m] for m in name_relevant_fields])
            
            # Again, check if the key has already been registered.
            if key_name not in combined_mapping[FROM_version]:
                
                # If no, we initialize a new key/value pair
                combined_mapping[FROM_version][key_name]: list[dict] = [TO_item]
                
            else:
                # Otherwise, we add to the existing key/value pair
                combined_mapping[FROM_version][key_name] += [TO_item]



    # Initialize a list to store the items that are removed (= no mapping specified)
    removed: list = []
    
    # Initialize lists to store the mapped items
    # one list for all successfully mapped items (where we end up with version specified in 'map_to_version')
    # and one list for all unsuccessfully mapped items (where version does not match 'map_to_version')
    successfully_mapped: list = []
    unsuccessfully_mapped: list = []
    
    # Loop through all elements previously imported from all excel files
    for _, item in all_data.items():
        
        # Extract only the FROM fields of the current item
        FROM: dict = {k: v for k, v in item.items() if "FROM_" in k}
    
        # If the FROM_activity_UUID is specified, but we cannot find a TO_activity_UUID, that means that the current item is not mapped and therefore not used anymore
        # Items are also removed if the multiplier is 0
        # In that case, we write it to a special list
        if (item["FROM_activity_UUID"] != "" and item["TO_activity_UUID"] == "") or item["multiplier"] == 0:
            removed += [{**{k.replace("FROM_", ""): v for k, v in FROM.items() if k != "FROM_version"}, **{"removed_in_version": item["TO_version"]}}]
            continue
        
        # If FROM_acivity_UUID or FROM_activity_name is empty, that means that the dataset has newly been introduced
        # We go to the next dataset because mapping would not make sense
        if item["FROM_activity_UUID"] == "" or item["FROM_activity"] == "":
            continue
        
        # Based on the FROM_version of the current item, extract all the versions that come when climbing the ladder up
        versions_to_loop_through: list[dict] = [m for m in version_list if m["FROM_version"] > item["FROM_version"] and m["TO_version"] <= map_to_version]
        
        # We use the current item as starting point. We only keep the 'TO' information and the multiplier
        start: dict = {k: v for k, v in item.items() if "TO_" in k or "multiplier" in k}
        
        # We initialize the going list where we add all results that we find when checking and matching other datasets from other versions
        # In the beginning, we only add our starting point. This list however will be extended during the loop afterwards
        going: list[list[dict]] = [[start]]
        
        # We loop through each version individually (order matters!), in order to extract all the mappings in the chain until we get to the end version 
        for idx, version in enumerate(versions_to_loop_through):
            
            # Extract a dictionary with all valid datasets of the current version that we want to map to        
            mappable_items: list[dict] = combined_mapping.get(version["FROM_version"])
            
            # For each element that we saved in the going list, we try to find a new match with the new version in the current loop
            # In order to go on, we extract the last elements from the going list by slicing
            last_relevant_items: list[dict] = going[-1]
            
            # Initialize a list to store all newly matched datasets
            found: list = []
            
            # Loop through each relevant list element to find the corresponding matching dataset with the version number in the current loop
            for n in last_relevant_items:
                
                # We try to find the new item using two options
                # 1... using the UUID's first
                found_I: (dict | None) = mappable_items.get(tuple([n["TO_" + m] for m in UUID_relevant_fields]))
                
                # 2... using the unique fields (such as name, unit, etc.)
                found_II: (dict | None) = mappable_items.get(tuple([n["TO_" + m].lower() if isinstance(n["TO_" + m], str) else n["TO_" + m] for m in name_relevant_fields]))
                
                # If we have found a new match, add it to the list
                if found_I is not None:
                    
                    # We also update the 'multiplier'. We use the old multiplier and multiply it with the new one
                    # Note: we multiply with the inverse (see description further above)
                    found += [dict(m, **{"multiplier": (1 / m["multiplier"] if m["multiplier"] != 0 else 0) * n["multiplier"]}) for m in found_I]
                
                # The same for the second option
                if found_II is not None:
                    found += [dict(m, **{"multiplier": (1 / m["multiplier"] if m["multiplier"] != 0 else 0) * n["multiplier"]}) for m in found_II]
            
            # Remove duplicates, if any
            found_duplicates_removed: list[dict] = list({str(m): m for m in found}.values())
    
            # Append newly found matching items to the going list, so that they can be used as elements in the following loop
            if found_duplicates_removed != []:
                going += [found_duplicates_removed]
        
        # After having screened all versions, we should have ended up with the final result as last element of the going list
        # We therefore extract the last element
        final_TOs_orig: list[dict] = going[-1]
        
        # For each element in the last element of going, we now add the FROM element
        final_TOs: list[dict] = [{**FROM,
                                  **{k1: v1 for k1, v1 in m.items() if "TO_" in k1 or "multiplier" in k1}} for m in final_TOs_orig]
        
        # We append to the variables
        # Successfully: if the final 'TO_version' matches the version that we indicated that we want to end up with ('map_to_version')
        successfully_mapped += [m for m in final_TOs if m["TO_version"] == map_to_version]
        
        # Otherwise, the match found is unsuccessful
        unsuccessfully_mapped += [m for m in final_TOs if m["TO_version"] != map_to_version]
    
    # Documentation dataframe
    documentation_dict: dict = {"correspondence_mapping": "A list of all datasets from one to the other version.",
                                "successful": "A list of all datasets that were successfully mapped from one correspondence file (version X) to the other correspondence file (version {})".format(".".join([str(m) for m in map_to_version])),
                                "unsuccessful": "A list of all datasets that were unsuccessfully mapped from one correspondence file (version X) to the other correspondence file (version {}). This can happen if e.g. datasets are not longer maintained or they were simply not connected well, e.g. if dataset codes from previous version were not matching codes from the next version.".format(".".join([str(m) for m in map_to_version])),
                                "removed": "A list of datasets that were removed from one correspondence file to the other correspondence file (no mapping specified). This happen if database maintainer was removing datasets from one to the other version.",}
    documentation: pd.DataFrame = pd.DataFrame([{"Sheetname": "", "Description": v} for k, v in documentation_dict.items()])
    
    # Initialize a list to store the items that are removed (= no mapping specified)
    
    # Initialize lists to store the mapped items
    # one list for all successfully mapped items (where we end up with version specified in 'map_to_version')
    # and one list for all unsuccessfully mapped items (where version does not match 'map_to_version')
    
    # Write dataframes to Excel
    with pd.ExcelWriter(output_path / "correspondence_files.xlsx", engine = "xlsxwriter") as writer:
        documentation.to_excel(writer, sheet_name = "documentation", index = False)
        pd.DataFrame(successfully_mapped + unsuccessfully_mapped).to_excel(writer, sheet_name = "correspondence_mapping", index = False)
        pd.DataFrame(successfully_mapped).to_excel(writer, sheet_name = "successful", index = False)   
        pd.DataFrame(unsuccessfully_mapped).to_excel(writer, sheet_name = "unsuccessful", index = False) 
        pd.DataFrame(removed).to_excel(writer, sheet_name = "removed", index = False) 
    
    return successfully_mapped + unsuccessfully_mapped

