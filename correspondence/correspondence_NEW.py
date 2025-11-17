import pathlib
here: pathlib.Path = pathlib.Path(__file__).parent

if __name__ == "__main__":
    import os
    os.chdir(here.parent)

import re
import pathlib
import pandas as pd
from bw2io.units import normalize_units as normalize_units_function
here: pathlib.Path = pathlib.Path(__file__).parent

#%% Create correspondence mapping from ecoinvent files
 
path_to_correspondence_files: pathlib.Path = here / "data"
model_type = "cutoff"
map_to_version = (3, 12)
    
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
orig_multipliers: list = [] # !!!

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
        
        # !!!
        # There might be cases where the multiplier is not a number
        # We need to handle such exceptions explicitely
        if not isinstance(multiplier, (int | float)):
            
            # Let's handle how to manage the multiplier if it is a text
            if isinstance(multiplier, str):
            
                # If it is an empty text string, we can (safely?) assume that it is a 1
                if multiplier.strip() == "":
                    multiplier: float = float(1)
                
                # Newly introduced datasets actually do not need a multiplier, but we use here a 1 as a placeholder
                elif multiplier.strip().lower() == "new":
                    multiplier: float = float(1)
                  
                # In the case where we find "no recommended share", we set the multiplier to 0. That means, those activities will not be mapped further (see code below).
                elif re.search("no recommended share", multiplier.strip().lower()):
                    multiplier: float = float(0)
                
                else:
                    # For any other text that we do not yet capture, we raise an error to see how to deal with it
                    raise ValueError("Strange text multiplier specified in correspondence file '{}'. No rule has been defined on how to handle the mulitplier '{}' yet.".format(filename, multiplier))
                    
            else:
                # If the multiplier is something else than a text, let's raise an error. We need to define new rules if that happens.
                raise ValueError("Multiplier's type ('{}') is not a number and no rules have been set to handle it right now.".format(type(multiplier)))
        
        orig_multipliers += [multiplier] # !!!
        
        # # Set multiplier to 1 if it is a strange string like 'NEW' or 'not recommended share' or simply an empty string # !!!
        # if not isinstance(multiplier, float | int): # !!!
        #     multiplier = 1 # !!!
        
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
standardized: pd.DataFrame = pd.DataFrame(list(all_data.values())).replace({"": None})


#%%


final: dict[str, pd.DataFrame] = {}
final_raw: dict[str, pd.DataFrame] = {}
final_failed: dict[str, pd.DataFrame] = {}
final_deleted: dict[str, pd.DataFrame] = {}
final_newly_introduced: dict[str, pd.DataFrame] = {}

TO_version: tuple = (3, 12)
versions_to_loop_through: set = set(standardized["FROM_version"])
indexes: int = 0

for FROM_version in versions_to_loop_through:
    
    if FROM_version >= TO_version:
        continue
    
    print(FROM_version, " -> ", TO_version)
    
    cols_1: tuple[str] = ("version", "activity", "reference_product", "location", "unit")
    cols_2: tuple[str] = ("version", "activity_UUID", "product_UUID")
    cols_1_without_version: tuple[str] = cols_1[1:]
    cols_2_without_version: tuple[str] = cols_2[1:]
    
    curr_df: pd.DataFrame = standardized.query("FROM_version == @FROM_version").add_suffix("_1")
    counter: int = 1
    counters: list[int] = []
    
    while True:
        
        # Add current counter to list
        counters += [counter]
        
        left_on_1: list[str] = [("TO_" + m + "_" + str(counter)) for m in cols_1]
        right_on_1: list[str] = [("FROM_" + m + "_" + str(counter + 1)) for m in cols_1]
        
        # Exclude rows that have incomplete cols_1
        # excluded_1 = curr_df.copy().replace({"": pd.NA, None: pd.NA}).loc[:, left_on_1].isna().any(axis = 1)
        mask_1 = curr_df[left_on_1].isna().any(axis = 1) | (curr_df[left_on_1] == "").any(axis = 1)
        excluded_1 = curr_df.copy()[mask_1]
        curr_df = curr_df.copy()[~mask_1]
        
        # Merge on cols 1
        curr_df = curr_df.copy().merge(
            standardized.add_suffix("_" + str(counter + 1)),
            how = "left",
            left_on = left_on_1,
            right_on = right_on_1,
        )
        
        unmatched = curr_df[right_on_1].isna().any(axis = 1)
        unmatched_df = curr_df[unmatched].drop(columns = curr_df.filter(regex = "_" + str(counter + 1) + "$").columns)
        
        left_on_2: list[str] = [("TO_" + m + "_" + str(counter)) for m in cols_2]
        right_on_2: list[str] = [("FROM_" + m + "_" + str(counter + 1)) for m in cols_2]
        
        # Exclude rows that have incomplete cols_2
        mask_2 = unmatched_df[left_on_2].isna().any(axis = 1) | (unmatched_df[left_on_2] == "").any(axis = 1)
        excluded_2 = unmatched_df.copy()[mask_2]
        unmatched_df = unmatched_df.copy()[~mask_2]
        
        # Merge on cols 2
        matched_df = unmatched_df.merge(
            standardized.add_suffix("_" + str(counter + 1)),
            how = "left",
            left_on = left_on_2,
            right_on = right_on_2,
        )
        
        curr_df: pd.DataFrame = pd.concat([curr_df.copy()[~unmatched],
                                           matched_df.copy(),
                                           excluded_1.copy(),
                                           excluded_2.copy()])
        
        if curr_df[right_on_1 + right_on_2].isna().all().all():
            curr_df = curr_df.drop(columns = curr_df.filter(regex = "_" + str(counter + 1) + "$").columns)
            break
        
        if TO_version in set(curr_df["TO_version_" + str(counter)]):
            curr_df = curr_df.drop(columns = curr_df.filter(regex = "_" + str(counter + 1) + "$").columns)
            break
        
        if counter == 50: # !!! Safety step
            raise ValueError("Aborted manually, was not breaking out of the while loop!")
        
        # Raise counter at the end
        counter += 1
    
    curr_df = curr_df.copy().replace({float("NaN"): None})
    curr_df["INDEX"] = [m + indexes for m in range(len(curr_df))]
    curr_df.set_index(["INDEX"])
    # curr_df.index = curr_df["INDEX"]
    indexes += len(curr_df.copy()) + 1
    curr_df["Multipliers"] = curr_df.copy().filter(regex = "multiplier").astype(str).agg(" * ".join, axis = 1)
    curr_df["Multiplier"] = curr_df.copy().filter(regex = "multiplier").prod(axis = 1).astype(float)
    curr_df["Versions"] = curr_df.copy().filter(regex = "FROM_version_{}|TO_version".format(min(counters))).astype(str).agg(" -> ".join, axis = 1)
    final_raw[(FROM_version, TO_version)] = curr_df.copy()
    
    curr_df = curr_df.copy().filter(regex = "FROM_(.*)_{}|TO_(.*)_{}|Multiplier|Versions|INDEX".format(min(counters), max(counters)))        
    curr_df.columns = curr_df.copy().columns.str.replace("_[0-9]+$", "", regex = True)
    
    failed = curr_df.copy()["TO_version"].isna()
    failed_df = curr_df.copy()[failed]
    curr_df = curr_df.copy()[~failed]
        
    deleted = curr_df.copy().filter(regex = "|".join(["TO_" + m for m in cols_1_without_version + cols_2_without_version])).isna().all(axis = 1)
    deleted_df = curr_df.copy()[deleted]
    curr_df = curr_df.copy()[~deleted]
    
    newly_introduced = curr_df.copy().filter(regex = "|".join(["FROM_" + m for m in cols_1_without_version + cols_2_without_version])).isna().all(axis = 1)
    newly_introduced_df = curr_df.copy()[newly_introduced]
    curr_df = curr_df.copy()[~newly_introduced]
    
    final[(FROM_version, TO_version)] = curr_df.copy()
    final_failed[(FROM_version, TO_version)] = failed_df.copy()
    final_deleted[(FROM_version, TO_version)] = deleted_df.copy()
    final_newly_introduced[(FROM_version, TO_version)] = newly_introduced_df.copy()

final_df: pd.DataFrame = pd.concat([m for _, m in final.items()])# .drop(columns = ["Index"])
final_failed_df: pd.DataFrame = pd.concat([m for _, m in final_deleted.items()])# .drop(columns = ["Index"])
final_deleted_df: pd.DataFrame = pd.concat([m for _, m in final_deleted.items()])
final_newly_introduced: pd.DataFrame = pd.concat([m for _, m in final_newly_introduced.items()])
documentation: dict[str, str] = {"": ""}

# Write dataframes to Excel
with pd.ExcelWriter(here / "correspondence_files.xlsx", engine = "xlsxwriter") as writer:
#     # documentation.to_excel(writer, sheet_name = "documentation", index = False)
    for (FROM_version, TO_version), df in final.items():
        df.to_excel(writer, sheet_name = ".".join([str(m) for m in FROM_version]) + " -> " + ".".join([str(m) for m in TO_version]), index = False)
    
    final_df.to_excel(writer, sheet_name = "all", index = False)
    final_failed_df.to_excel(writer, sheet_name = "failed", index = False)
    final_deleted_df.to_excel(writer, sheet_name = "deleted", index = False)
    final_newly_introduced.to_excel(writer, sheet_name = "newly_introduced", index = False)


with pd.ExcelWriter(here / "correspondence_files_RAW.xlsx", engine = "xlsxwriter") as writer:
    pd.concat([m for _, m in final_raw.items()]).to_excel(writer, sheet_name = "all_RAW", index = False)


for idx, line in final_df.iterrows():
    ...

from pprint import pprint
pprint([(min(m.index), max(m.index)) for _, m in final.items()])
pprint([m.index for _, m in final.items()])