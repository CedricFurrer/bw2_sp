import pathlib
here: pathlib.Path = pathlib.Path(__file__).parent

if __name__ == "__main__":
    import os
    os.chdir(here.parent)

import re
import pathlib
import pandas as pd
import pydantic
from bw2io.units import normalize_units as normalize_units_function
here: pathlib.Path = pathlib.Path(__file__).parent



class Correspondence():
    
    def __init__(self,
                 ecoinvent_model_type: str) -> None:
        
        self.FROM_definer: str = "FROM"
        self.TO_definer: str = "TO"
        self.whitespace_replacer: str = "_"
        self.version_replacer: str = "."
        
        self.ecoinvent_model_type: str = ecoinvent_model_type
        self.ecoinvent_model_type_cutoff: str = "cutoff"
        self.ecoinvent_model_type_apos: str = "apos"
        self.ecoinvent_model_type_consequential: str = "consequential"
        self.allowed_ecoinvent_model_types: list[str] = [self.ecoinvent_model_type_cutoff, self.ecoinvent_model_type_apos, self.ecoinvent_model_type_consequential]
        
        self.key_name_version: str = "version"
        self.key_name_version_uppered: str = self.key_name_version[0].upper() + self.key_name_version[1:].lower()
        self.key_name_FROM_version: str = self.whitespace_replacer.join((self.FROM_definer, self.key_name_version))
        self.key_name_TO_version: str = self.whitespace_replacer.join((self.TO_definer, self.key_name_version))
        self.key_name_activity_uuid: str = "activity_uuid"
        self.key_name_reference_product_uuid: str = "reference_product_uuid"
        self.key_name_activity_name: str = "activity_name"
        self.key_name_reference_product_name: str = "reference_product_name"
        self.key_name_location_name: str = "location"
        self.key_name_unit_name: str = "unit"
        self.key_name_multiplier: str = "multiplier"
        self.key_name_multiplier_uppered: str = self.key_name_multiplier[0].upper() + self.key_name_multiplier[1:].lower()
        self.key_name_multiplier_uppered_plural: str = self.key_name_multiplier_uppered + "s"
        
        self.raw_data: dict = {}
        self.df_interlinked_data_raw: dict = {}
        self.interlinked_data: dict = {}
        self.check_multipliers: dict = {}
        
        self.identifier_1: tuple[str, str] = tuple([self.key_name_activity_uuid, self.key_name_reference_product_uuid])
        self.identifier_2: tuple[str, str, str, str] = tuple([self.key_name_activity_name, self.key_name_reference_product_name, self.key_name_location_name, self.key_name_unit_name])
        self.identifier_1_uppered: tuple[str, str] = tuple([m[0].upper() + m[1:].lower() for m in self.identifier_1])
        self.identifier_2_uppered: tuple[str, str, str, str] = tuple([m[0].upper() + m[1:].lower() for m in self.identifier_2])
        self.FROM_identifier_1: tuple[str, str] = tuple([self.whitespace_replacer.join((self.FROM_definer, m)) for m in self.identifier_1])
        self.FROM_identifier_2: tuple[str, str] = tuple([self.whitespace_replacer.join((self.TO_definer, m)) for m in self.identifier_2])
        self.TO_identifier_1: tuple[str, str] = tuple([self.whitespace_replacer.join((self.FROM_definer, m)) for m in self.identifier_1])
        self.TO_identifier_2: tuple[str, str] = tuple([self.whitespace_replacer.join((self.TO_definer, m)) for m in self.identifier_2])


        # Raise error, if model type is None of the defaults
        if ecoinvent_model_type not in self.allowed_ecoinvent_model_types:
            raise ValueError("Current model type '{}' is not allowed. Use one of the following: {}".format(self.ecoinvent_model_type, self.allowed_ecoinvent_model_types))
        
        
        
    def read_correspondence_dataframe(self,
                                      filepath_correspondence_excel: pathlib.Path,
                                      FROM_version: tuple[int, int],
                                      TO_version: tuple[int, int],
                                      ) -> None:
        
        # Names of sheets in the excel file depending on the ecoinvent model type
        sheet_name_mapping: dict = {self.ecoinvent_model_type_cutoff: ["Cut-off", "Cut-Off", "cut-off", "cutoff"],
                                    self.ecoinvent_model_type_apos: ["apos", "APOS"],
                                    self.ecoinvent_model_type_consequential: ["consequential", "Consequential", "conseq"]}

        # Sheet names to search for in the excel files
        sheetnames: list[str] = sheet_name_mapping[self.ecoinvent_model_type]

        # Relevant columns in the excel files
        actID_cols: list[str] = ["activityID", "Activity UUID"]
        prodID_cols: list[str] = ["Product UUID"]
        ref_cols: list[str] = ["product name", "Product Name"]
        act_cols: list[str] = ["activityName", "Activity Name"]
        loc_cols: list[str] = ["geography", "Geography"]
        unit_cols: list[str] = ["unit", "Unit"]
        mult_cols: list[str] = ["amount of the replacement", "Replacement amount", "replacement amount", "replacement share"]
    
        # Make a version string, based on the version tuple from the element
        FROM_version_str: str = self.version_replacer.join([str(m) for m in FROM_version])
        TO_version_str: str = self.version_replacer.join([str(m) for m in TO_version])
        
        # Print statement
        print("Reading", FROM_version, " -> ", TO_version)
        
        # Read the correspondence excel file for the current versions
        excel: dict[pd.DataFrame] = pd.read_excel(filepath_correspondence_excel, sheet_name = None)
        
        # Only use the sheet that we want to use --> either cut-off, apos or consequential
        # Because the files use different names for the systems, we need to search for the appropriate one
        sheet: list[str] = [m for m in sheetnames if m in excel]
        
        # If we could not identify the excel sheet for the current system, we raise an error
        assert len(sheet) == 1, "None or more than one sheet name ({}) matches in the excel ({})".format(sheet, filepath_correspondence_excel)
        
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
                new_header += [head + self.whitespace_replacer + TO_version_str]
            
            elif head in doubles:
                
                # If it has NOT been seen but it is a double, it is the one from 'FROM'
                new_header += [head + self.whitespace_replacer + FROM_version_str]
                
            else:
                # Otherwise, we leave the head as it is and append it to the list
                new_header += [head]
            
            # At the end of the loop, we update the variables
            seen += [head]
            # all_headers += [head]
        
        
        # We now remove the first rows which we don't need anymore
        df: pd.DataFrame = df_orig.copy().fillna("").iloc[new_header_idx+1:len(df_orig)]
        
        # Add new headers to the dataframe
        df.columns = new_header
        
        # Check if column names exist and extract the column names in the excel file
        FROM_colname_actID: str = self.check_if_at_least_one_item_exists_in_list_and_return_first_occurence(items = [self.whitespace_replacer.join((m, FROM_version_str)) for m in actID_cols],
                                                                                                            in_list = new_header,
                                                                                                            raise_error_if_not_exist = True,
                                                                                                            return_element_if_not_exists = None)
        
        TO_colname_actID: str = self.check_if_at_least_one_item_exists_in_list_and_return_first_occurence(items = [self.whitespace_replacer.join((m, TO_version_str)) for m in actID_cols],
                                                                                                          in_list = new_header,
                                                                                                          raise_error_if_not_exist = True,
                                                                                                          return_element_if_not_exists = None)
        
        FROM_colname_prodID: (str | None) = self.check_if_at_least_one_item_exists_in_list_and_return_first_occurence(items = [self.whitespace_replacer.join((m, FROM_version_str)) for m in prodID_cols],
                                                                                                                      in_list = new_header,
                                                                                                                      raise_error_if_not_exist = False,
                                                                                                                      return_element_if_not_exists = None)
        
        TO_colname_prodID: (str | None) = self.check_if_at_least_one_item_exists_in_list_and_return_first_occurence(items = [self.whitespace_replacer.join((m, TO_version_str)) for m in prodID_cols],
                                                                                                                    in_list = new_header,
                                                                                                                    raise_error_if_not_exist = False,
                                                                                                                    return_element_if_not_exists = None)
        
        FROM_colname_ref: str = self.check_if_at_least_one_item_exists_in_list_and_return_first_occurence(items = [self.whitespace_replacer.join((m, FROM_version_str)) for m in ref_cols],
                                                                                                          in_list = new_header,
                                                                                                          raise_error_if_not_exist = True,
                                                                                                          return_element_if_not_exists = None)
        
        TO_colname_ref: str = self.check_if_at_least_one_item_exists_in_list_and_return_first_occurence(items = [self.whitespace_replacer.join((m, TO_version_str)) for m in ref_cols],
                                                                                                        in_list = new_header,
                                                                                                        raise_error_if_not_exist = True,
                                                                                                        return_element_if_not_exists = None)
        
        FROM_colname_act: str = self.check_if_at_least_one_item_exists_in_list_and_return_first_occurence(items = [self.whitespace_replacer.join((m, FROM_version_str)) for m in act_cols],
                                                                                                          in_list = new_header,
                                                                                                          raise_error_if_not_exist = True,
                                                                                                          return_element_if_not_exists = None)
        
        TO_colname_act: str = self.check_if_at_least_one_item_exists_in_list_and_return_first_occurence(items = [self.whitespace_replacer.join((m, TO_version_str)) for m in act_cols],
                                                                                                        in_list = new_header,
                                                                                                        raise_error_if_not_exist = True,
                                                                                                        return_element_if_not_exists = None)
        
        FROM_colname_loc: str = self.check_if_at_least_one_item_exists_in_list_and_return_first_occurence(items = [self.whitespace_replacer.join((m, FROM_version_str)) for m in loc_cols],
                                                                                                          in_list = new_header,
                                                                                                          raise_error_if_not_exist = True,
                                                                                                          return_element_if_not_exists = None)
        
        TO_colname_loc: str = self.check_if_at_least_one_item_exists_in_list_and_return_first_occurence(items = [self.whitespace_replacer.join((m, TO_version_str)) for m in loc_cols],
                                                                                                        in_list = new_header,
                                                                                                        raise_error_if_not_exist = True,
                                                                                                        return_element_if_not_exists = None)
        
        FROM_colname_unit: str = self.check_if_at_least_one_item_exists_in_list_and_return_first_occurence(items = [self.whitespace_replacer.join((m, FROM_version_str)) for m in unit_cols],
                                                                                                           in_list = new_header,
                                                                                                           raise_error_if_not_exist = True,
                                                                                                           return_element_if_not_exists = None)
        
        TO_colname_unit: str = self.check_if_at_least_one_item_exists_in_list_and_return_first_occurence(items = [self.whitespace_replacer.join((m, TO_version_str)) for m in unit_cols],
                                                                                                         in_list = new_header,
                                                                                                         raise_error_if_not_exist = True,
                                                                                                         return_element_if_not_exists = None)
        
        # Extract column for the multiplier separately
        colname_mult_orig: list[str] = [m for m in mult_cols if m in new_header]
        
        # Raise error if the multiplier column was not found
        assert len(colname_mult_orig) > 0, "No multiplier column found. Used possible columns '{}'. Error occured in file '{}'".format(mult_cols, filepath_correspondence_excel)
        
        # Use the first element as column name for multiplier
        colname_mult: (int | float | str) = colname_mult_orig[0]
        
        if FROM_version not in self.raw_data:
            self.raw_data[FROM_version]: dict = {}
            
        if TO_version not in self.raw_data[FROM_version]:
            self.raw_data[FROM_version][TO_version]: dict = {}
        
        if FROM_version not in self.check_multipliers:
            self.check_multipliers[FROM_version]: dict = {}
            
        if TO_version not in self.check_multipliers[FROM_version]:
            self.check_multipliers[FROM_version][TO_version]: dict = {}
        
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
            
            
            # Create FROM_ID
            FROM_ID: tuple = (FROM_actID,
                              FROM_prodID,
                              FROM_ref,
                              FROM_act,
                              FROM_loc,
                              FROM_unit
                              )
                               
            # Create TO_ID
            TO_ID: tuple = (TO_actID,
                            TO_prodID,
                            TO_ref,
                            TO_act,
                            TO_loc,
                            TO_unit
                            )
            
            # Initialize ID in dictionary if not yet existing
            if FROM_ID not in self.check_multipliers[FROM_version][TO_version]:
                self.check_multipliers[FROM_version][TO_version][FROM_ID]: float = float(multiplier)
                
            else:
                # Add to multipliers checking dictionary
                self.check_multipliers[FROM_version][TO_version][FROM_ID] += float(multiplier)
            
            
            # !!! We need to do the inverse for the multiplier. Do we???
            # According to the documentation from correspondence file v3.4/v3.5 (comment of cell N3)
            # If the multiplier is 1 it means the eiv3.4 activity is replaced by exactly one activity in eiv3.5.
            # If the multiplier is different (below one) it represents the share of the replacement activity.
            # E.g. if the value here is 0.25 then 25% of the eiv3.4 activity should be replaced by the specific activity in eiv3.5.
            # The share of the eiv3.4 matches of course sum up to 1.
            # inversed_multiplier: (int | float) = 1 / multiplier if multiplier != 0 else 0
            
            # Create ID
            ID: tuple = FROM_ID + TO_ID
            
            # Raise error if key is already available
            assert ID not in self.raw_data[FROM_version][TO_version], "Key is already present. Error occured for key '{}' when reading file '{}'".format(ID, filepath_correspondence_excel)
                
            # Append to data dictionary
            self.raw_data[FROM_version][TO_version][ID]: dict = {
                self.whitespace_replacer.join((self.FROM_definer, self.key_name_version)): FROM_version,
                self.whitespace_replacer.join((self.FROM_definer, self.key_name_activity_uuid)): FROM_actID,
                self.whitespace_replacer.join((self.FROM_definer, self.key_name_reference_product_uuid)): FROM_prodID,
                self.whitespace_replacer.join((self.FROM_definer, self.key_name_activity_name)): FROM_act,
                self.whitespace_replacer.join((self.FROM_definer, self.key_name_reference_product_name)): FROM_ref,
                self.whitespace_replacer.join((self.FROM_definer, self.key_name_location_name)): FROM_loc,
                self.whitespace_replacer.join((self.FROM_definer, self.key_name_unit_name)): FROM_unit,
                self.key_name_multiplier: multiplier,
                self.whitespace_replacer.join((self.TO_definer, self.key_name_version)): TO_version,
                self.whitespace_replacer.join((self.TO_definer, self.key_name_activity_uuid)): TO_actID,
                self.whitespace_replacer.join((self.TO_definer, self.key_name_reference_product_uuid)): TO_prodID,
                self.whitespace_replacer.join((self.TO_definer, self.key_name_activity_name)): TO_act,
                self.whitespace_replacer.join((self.TO_definer, self.key_name_reference_product_name)): TO_ref,
                self.whitespace_replacer.join((self.TO_definer, self.key_name_location_name)): TO_loc,
                self.whitespace_replacer.join((self.TO_definer, self.key_name_unit_name)): TO_unit,
                }
    
    def check_if_multipliers_sum_to_1(self) -> list[dict]:
        
        problematic_data: dict = {(FROM_version, ID, TO_version): v for FROM_version, m in self.check_multipliers.items() for TO_version, n in m.items() for ID, v in n.items() if (v < 0.99 or v > 1.01) and (v != 0) and ("" not in ID) and (None not in ID)}
        multipliers_not_summing_to_1: list[dict] = []
        
        for (FROM_version, ID, TO_version), multiplier_summed in problematic_data.items():
            multipliers_not_summing_to_1 += [{**self.raw_data[FROM_version][TO_version][ID], **{"multiplier_summed": multiplier_summed}}]
        
        return multipliers_not_summing_to_1
        

    # Used here to identify if columns exist or not
    def check_if_at_least_one_item_exists_in_list_and_return_first_occurence(self,
                                                                             items: list,
                                                                             in_list: list,
                                                                             raise_error_if_not_exist: bool,
                                                                             return_element_if_not_exists: (str | float | None) = None) -> list:
        
        # Identify the items that can not be found in the list
        found: list = [m for m in items if m in in_list]
        
        # Check if error should be raised
        if raise_error_if_not_exist:
            
            # If at least one element has been found, raise error
            if len(found) == 0:
                raise ValueError("None of the item(s) '{}' can be found in list '{}'".format(items, in_list))
        
        # Return the elements and replace if they are not found
        return found[0] if len(found) > 0 else return_element_if_not_exists
    
    
    def interlink_correspondence_files(self,
                                       FROM_version: tuple[int, int],
                                       TO_version: tuple[int, int]) -> None:
        
        # Go on if there is no 
        if FROM_version not in self.raw_data:
            return
        
        # Return if FROM_version is greater than the version we should map to
        if FROM_version >= TO_version:
            return
        
        # Write all data to a long list
        all_data: list[dict] = [{**{self.key_name_FROM_version: a},
                                 **e, **{self.key_name_TO_version: c}} for a, b in self.raw_data.items() for c, d in b.items() for _, e in d.items()]
        
        # Query the data we want to map, excluding the entries that have a multiplier of 0
        data: list[dict] = [m for m in all_data if m[self.key_name_FROM_version] == FROM_version and m[self.key_name_multiplier] != 0]    
        
        # Replace the NaN's with None
        df_all_data: pd.DataFrame = pd.DataFrame(all_data).replace({"": None})
        df_data: pd.DataFrame = pd.DataFrame(data).replace({"": None})
        
        # Print statement
        print("Interlinking versions:", FROM_version, "->", TO_version)
        
        # Initialize counter
        counter: int = 1
        counters: list[int] = []
        
        # !!! Add the first counter to the dataframe
        df_data.columns = [self.whitespace_replacer.join((m, str(counter))) for m in list(df_data.columns).copy()]
        
        # Go into a while loop
        while True:
            
            # Add current counter to list
            counters += [counter]
            
            # First set of columns to join the dataframes on
            left_on_1: list[str] = [self.whitespace_replacer.join((m, str(counter))) for m in self.TO_identifier_2]
            right_on_1: list[str] = [self.whitespace_replacer.join((m, str(counter + 1))) for m in self.FROM_identifier_2]
            
            # Exclude rows that have incomplete identifier columns
            mask_1: pd.Series = df_data[left_on_1].isna().any(axis = 1) | (df_data[left_on_1] == "").any(axis = 1)
            excluded_1: pd.DataFrame = df_data.copy()[mask_1]
            df_data: pd.DataFrame = df_data.copy()[~mask_1]
            
            # Merge data on first identifier columns
            df_data: pd.DataFrame = df_data.copy().merge(
                df_all_data.add_suffix(self.whitespace_replacer + str(counter + 1)),
                how = "left",
                left_on = left_on_1,
                right_on = right_on_1,
            )
            
            # Check which of the columns were not properly matched
            # We then continue with only the unmatched data and try to match it with the other identifier columns
            unmatched: pd.Series = df_data[right_on_1].isna().any(axis = 1)
            unmatched_df: pd.DataFrame = df_data[unmatched].drop(columns = df_data.filter(regex = self.whitespace_replacer + str(counter + 1) + "$").columns)
            
            # Second set of columns to join the dataframes on
            left_on_2: list[str] = [self.whitespace_replacer.join((m, str(counter))) for m in self.TO_identifier_1]
            right_on_2: list[str] = [self.whitespace_replacer.join((m, str(counter + 1))) for m in self.FROM_identifier_1]
            
            # Exclude rows that have incomplete identifier columns
            mask_2: pd.Series = unmatched_df[left_on_2].isna().any(axis = 1) | (unmatched_df[left_on_2] == "").any(axis = 1)
            excluded_2: pd.DataFrame = unmatched_df.copy()[mask_2]
            unmatched_df: pd.DataFrame = unmatched_df.copy()[~mask_2]
            
            # Merge on second set of identifier columns
            matched_df: pd.DataFrame = unmatched_df.merge(
                df_all_data.add_suffix(self.whitespace_replacer + str(counter + 1)),
                how = "left",
                left_on = left_on_2,
                right_on = right_on_2,
            )
            
            # Lets combine everything again
            # The unmatched, matched and excluded data
            df_data: pd.DataFrame = pd.concat([df_data.copy()[~unmatched],
                                               matched_df.copy(),
                                               excluded_1.copy(),
                                               excluded_2.copy()])
            
            # If we encounter only empty columns that have been newly attached to our df, that means we can end the while loop
            if df_data[right_on_1 + right_on_2].isna().all().all():
                
                # We drop the last columns that were added again and then break the while loop
                df_data: pd.DataFrame = df_data.drop(columns = df_data.filter(regex = self.whitespace_replacer + str(counter + 1) + "$").columns)
                break
            
            # The same happens if we have reached our final version we want to map to
            if TO_version in set(df_data[self.whitespace_replacer.join((self.TO_definer, self.key_name_version, str(counter)))]):
                
                # We drop the last columns that were added again and then break the while loop
                df_data: pd.DataFrame = df_data.drop(columns = df_data.filter(regex = self.whitespace_replacer + str(counter + 1) + "$").columns)
                break
            
            # !!! This is purely a saftey step! In case that we would not end the while loop, we break it manually after n steps and raise an error
            if counter == 50:
                raise ValueError("Aborted manually, was not breaking out of the while loop!")
            
            # Raise counter at the end
            counter += 1
        
        # Let's replace the NaN's with None's to better be able to work with!
        df_data: pd.DataFrame = df_data.copy().replace({float("NaN"): None})
        
        # We construct the multiplier by multiplying all multipliers that were used ('prod')
        df_data[self.key_name_multiplier_uppered]: pd.Series = df_data.copy().filter(regex = self.key_name_multiplier).prod(axis = 1).astype(float)
        
        # We then add the current dataframe as raw results to the dictionary
        self.df_interlinked_data_raw[(FROM_version, TO_version)] = df_data.copy()
        
        # In order to understand what has happened during interlinking, we merge all the multiplier columns together using a '*'
        df_data[self.key_name_multiplier_uppered_plural]: pd.Series = df_data.copy().filter(regex = self.key_name_multiplier).astype(str).agg(" * ".join, axis = 1)
        
        # Similarly as for the multiplier, we'd like to understand what happened in between with all the other fields when interlinking
        # We loop through all the identifier columns and merge the together
        for i in (self.key_name_version,) + self.identifier_2 + self.identifier_1:
            df_data[i[0].upper() + i[1:].lower()]: pd.Series = df_data.copy().filter(regex = "{}{}{}{}{}$|{}{}{}".format(self.FROM_definer, self.whitespace_replacer, i, self.whitespace_replacer, min(counters), self.TO_definer, self.whitespace_replacer, i)).astype(str).agg(" -> ".join, axis = 1)
        
        # Now we only keep the columns with the minimum and maximum counter at the end of the column name,
        # ... plus the ones that we introduced beforehand
        # We use regex to do that...
        # !!! df_data: pd.DataFrame = df_data.copy().filter(regex = "(^FROM_(.*)_{}$)|(^TO_(.*)_{}$)|^Multiplier|{}".format(min(counters), max(counters), "|".join(["(^" + m[0].upper() + m[1:].lower() + "$)" for m in cols_1 + cols_2_without_version])))        
        df_data: pd.DataFrame = df_data.copy().filter(regex = "(^{}{}(.*){}{}$)|(^{}{}(.*){}{}$)|^{}|{}".format(self.FROM_definer, self.whitespace_replacer, self.whitespace_replacer, min(counters), self.TO_definer, self.whitespace_replacer, self.whitespace_replacer, max(counters), self.key_name_multiplier_uppered, "|".join(["(^" + m + "$)" for m in (self.key_name_version_uppered,) + self.identifier_2_uppered + self.identifier_1_uppered])))        
        
        # We remove the numbers at the end of the column names again, since we do not need them anymore
        df_data.columns = df_data.copy().columns.str.replace(self.whitespace_replacer + "[0-9]+$", "", regex = True)
        
        # Write the data where interlinktion failed to separate dataframe/dictionary
        # First we extract the data where interlinktion failed
        # This is by default the ones where either multiplier is 0, or the TO version is empty or not at the level where we want it to be
        interlinktion_failed: pd.Series = (df_data[self.key_name_TO_version].isna()) | (df_data[self.key_name_TO_version] != TO_version) | (df_data[self.key_name_multiplier_uppered] == 0)
        df_where_interlinktion_failed: pd.DataFrame = df_data.copy()[interlinktion_failed]
        df_data: pd.DataFrame = df_data.copy()[~interlinktion_failed]
        
        # Write the data which was deleted from one version to another to a separate dataframe
        # This is by default the ones where all TO columns are empty
        deleted: pd.Series = df_data.copy().filter(regex = "|".join(self.TO_identifier_2 + self.TO_identifier_1)).isna().all(axis = 1)
        df_deleted: pd.DataFrame = df_data.copy()[deleted]
        df_data: pd.DataFrame = df_data.copy()[~deleted]
        
        # Write the data which was newly introduced from one to another version to a separate dataframe
        # This is by default the ones where all FROM columns are empty
        newly_introduced: pd.Series = df_data.copy().filter(regex = "|".join(self.FROM_identifier_2 + self.FROM_identifier_1)).isna().all(axis = 1)
        df_newly_introduced: pd.DataFrame = df_data.copy()[newly_introduced]
        df_data: pd.DataFrame = df_data.copy()[~newly_introduced]
        
        # Initialize new attributes if not yet existing
        if not hasattr(self, "df_interlinked_data"):
            self.df_interlinked_data: dict = {}
            
        if not hasattr(self, "df_failed_interlinktion"):
            self.df_failed_interlinktion: dict = {}
        
        if not hasattr(self, "df_deleted"):
            self.df_deleted: dict = {}
            
        if not hasattr(self, "df_newly_introduced"):
            self.df_newly_introduced: dict = {}
        
        # Add to self object
        self.df_interlinked_data[(FROM_version, TO_version)]: pd.DataFrame = df_data.copy()
        self.df_failed_interlinktion[(FROM_version, TO_version)]: pd.DataFrame = df_where_interlinktion_failed.copy()
        self.df_deleted[(FROM_version, TO_version)]: pd.DataFrame = df_deleted.copy()
        self.df_newly_introduced[(FROM_version, TO_version)]: pd.DataFrame = df_newly_introduced.copy()
    
    def map_by_names(self,
                     FROM_version: tuple[int],
                     TO_version: tuple[int],
                     activity_name: str,
                     reference_product_name: str,
                     unit: str,
                     location: str) -> dict:
        
        if not hasattr(self, "mapping_by_names"):
            self.mapping_by_names: dict = {}
            
        if self.mapping_by_names.get((FROM_version, TO_version)) is None:
            ...
            if self.df_interlinked_data.get((FROM_version, TO_version)) is None:
                self.interlink_correspondence_files(FROM_version, TO_version)
                
            for idx, line in self.df_interlinked_data[(FROM_version, TO_version)].iterrows():
                ...
                
        
#%%
files: list[tuple] = [("Correspondence-File-v3.1-v3.2.xlsx", (3, 1), (3, 2)),
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

correspondence: Correspondence = Correspondence(ecoinvent_model_type = "cutoff")

for filename, FROM_version, TO_version in files:
    
    # if TO_version == (3, 4):
    #     break
    
    correspondence.read_correspondence_dataframe(filepath_correspondence_excel = here / "data" / filename,
                                                 FROM_version = FROM_version,
                                                 TO_version = TO_version
                                                 )

correspondence_raw_data: dict = correspondence.raw_data

#%%
correspondence.interlink_correspondence_files((3, 1), (3, 3))
correspondence.interlink_correspondence_files((3, 5), (3, 12))
correspondence.interlink_correspondence_files((3, 8), (3, 12))
correspondence.interlink_correspondence_files((3, 9), (3, 12))

df_multipliers_not_summing_to_1: pd.DataFrame = pd.DataFrame(correspondence.check_if_multipliers_sum_to_1())
df_multipliers_not_summing_to_1.to_excel(here / "multipliers_not_summing_to_1_new.xlsx", index = False)

df_interlinked_data: dict[tuple, pd.DataFrame] = correspondence.df_interlinked_data

# #%% Create correspondence mapping from ecoinvent files
 
# path_to_correspondence_files: pathlib.Path = here / "data"
# model_type = "cutoff"
# map_to_version = (3, 12)
    
# # Pattern to extract the version names from the excel correspondence files
# correspondence_file_pattern: str = ("^Correspondence\-File\-v"
#                                     "(?P<FROM_version>[0-9\.]+)"
#                                     "\-v"
#                                     "(?P<TO_version>[0-9\.]+)"
#                                     "(\.xlsx)$"
#                                     )
    
# # Apply regex pattern
# version_list_orig: list[tuple] = [(k.name, re.match(correspondence_file_pattern, k.name)) for k in path_to_correspondence_files.rglob("*.xlsx")]
    
# # Raise error, if no files were found
# if version_list_orig == []:
#     raise ValueError("No correspondence files detected. Correspondence files should be of type '.xlsx' and be in the format 'Correspondence-File-vX.x-v.Y.y' (e.g. 'Correspondence-File-v3.9.1-v3.10') and contain one of the sheet names 'cutoff', 'apos' or 'consequential'.")

# # Raise error, if model type is None of the defaults
# if model_type not in ["cutoff", "apos", "consequential"]:
#     raise ValueError("Current model type '{}' is not allowed. Use one of the following: 'cutoff', 'apos' or 'consequential'".format(model_type))

# # Raise error, if the version tuple is of wrong format
# wrong_version_tuple: list = [m for m in map_to_version if not isinstance(m, int)]
# if wrong_version_tuple != []:
#     raise ValueError("Version tuple only accepts integers as items, e.g. (3, 10). Currently, the following version tuple is provided: '{}'".format(map_to_version))

# # Convert version strings to tuples and sort the list so that the highest version is coming first
# version_list: list[dict] = sorted([dict({k: tuple(int(n) for n in v.split(".")) for k, v in m.groupdict().items()}, **{"file": a}) for a, m in version_list_orig if m is not None], key = lambda x: x["TO_version"], reverse = False)

# # Empty list to store header names
# all_headers: list = []

# # Names of sheets in the excel file depending on the ecoinvent model type
# sheet_name_mapping: dict = {"cutoff": ["Cut-off", "Cut-Off", "cut-off", "cutoff"],
#                             "apos": ["apos", "APOS"],
#                             "consequential": ["consequential", "Consequential", "conseq"]}

# # Sheet names to search for in the excel files
# sheetnames = sheet_name_mapping[model_type]

# # Relevant columns in the excel files
# # ID_cols = ["activityID", "Activity UUID"]
# actID_cols = ["activityID", "Activity UUID"]
# prodID_cols = ["Product UUID"]
# ref_cols = ["product name", "Product Name"]
# act_cols = ["activityName", "Activity Name"]
# loc_cols = ["geography", "Geography"]
# unit_cols = ["unit", "Unit"]
# mult_cols = ["amount of the replacement", "Replacement amount", "replacement amount", "replacement share"]

# # Initialize
# all_data: dict = {}

# # # Specify to which version the mapping should apply to
# # map_to_version = (3, 10)
# check_multipliers: dict = {}

# # Loop through each excel file (= version) independently
# for item in version_list:
    
#     # Write relevant fields from current 'item' variable as separate variables for easier handling
#     filename: str = item["file"]
#     FROM_version: tuple = item["FROM_version"]
#     TO_version: tuple = item["TO_version"]
    
#     # Go on if the current version is greater than the one we want to map to
#     if FROM_version > map_to_version:
#         continue
    
#     # Print where we are
#     print(filename)
    
#     # Make a version string, based on the version tuple from the element
#     FROM_version_str: str = ".".join([str(m) for m in FROM_version])
#     TO_version_str: str = ".".join([str(m) for m in TO_version])
    
#     # Read the correspondence excel file for the current versions
#     excel: dict[pd.DataFrame] = pd.read_excel(path_to_correspondence_files / filename, sheet_name = None)
    
#     # Only use the sheet that we want to use --> either cut-off, apos or consequential
#     # Because the files use different names for the systems, we need to search for the appropriate one
#     sheet: list[str] = [m for m in sheetnames if m in excel]
    
#     # If we could not identify the excel sheet for the current system, we raise an error
#     assert len(sheet) == 1, "None or more than one sheet name matches"
    
#     # The raw data to use
#     df_orig: pd.DataFrame = excel[sheet[0]]
    
#     # We need to adjust the header
#     # We need to skip the first rows. First, we identify where the header is located
#     # We can find the header, as the first line, where no NaN values are found.
#     # We add a column to the dataframe. Each row is checked whether any value in that row is NaN. If yes, it is True, otherwise it is False.
#     df_orig["_header_selection"] = df_orig.isnull().any(axis = 1)
    
#     # We locate the header as the first row, where all values are not empty (NaN)
#     new_header_idx: int = [idx for idx, m in df_orig.iterrows() if not m["_header_selection"]][0]
    
#     # We extract the new header based on the location found beforehand
#     new_header_orig: list[str] = list(df_orig.loc[new_header_idx])
    
#     # We identify the duplicated headers
#     doubles: list[str] = [m for m in new_header_orig if new_header_orig.count(m) > 1]
    
#     # Initialize new lists
#     seen: list = []
#     new_header: list = []
    
#     # Loop through each old column name
#     for head in new_header_orig:
        
#         # Check if the current column/head appears twice (or more) and whether it has already seen or not in that loop
#         if head in doubles and head in seen:
            
#             # If it is a double and it has been seen, that means the current header is the one belonging to the 'TO'
#             new_header += [head + "_" + TO_version_str]
        
#         elif head in doubles:
            
#             # If it has NOT been seen but it is a double, it is the one from 'FROM'
#             new_header += [head + "_" + FROM_version_str]
            
#         else:
#             # Otherwise, we leave the head as it is and append it to the list
#             new_header += [head]
        
#         # At the end of the loop, we update the variables
#         seen += [head]
#         all_headers += [head]
    
    
#     # We now remove the first rows which we don't need anymore
#     df: pd.DataFrame = df_orig.copy().fillna("").iloc[new_header_idx+1:len(df_orig)]
    
#     # Add new headers to the dataframe
#     df.columns = new_header
    
#     # Define function to find column names for the different types of information that we need
#     def find_colnames(possible_cols: list, raise_error_if_no_columns_were_found: bool = True) -> (str | None, str | None):
        
#         # For each possible column, check if it is available in the new header and gather in the list
#         found = [(m + "_" + FROM_version_str, m + "_" + TO_version_str) for m in possible_cols if m + "_" + FROM_version_str in new_header and m + "_" + FROM_version_str in new_header]
        
#         # Raise error, if specified and return
#         if raise_error_if_no_columns_were_found:
            
#             # Raise error if no columns were identified
#             assert len(found) > 0, "No columns found. Used possible columns '" + str(possible_cols) + "'. Error occured in file '" + str(filename) + "'" 
        
#             # Return the results from the first element --> FROM and TO
#             return found[0][0], found[0][1]
        
#         else:
#             # If column names have been found, return the results from the first element --> FROM and TO
#             if len(found) > 0:
#                 return found[0][0], found[0][1]
            
#             # Otherwise return None's
#             else:
#                 return None, None
            
    
#     # Extract the column names for the current excel file
#     FROM_colname_actID, TO_colname_actID = find_colnames(actID_cols, True)
#     FROM_colname_prodID, TO_colname_prodID = find_colnames(prodID_cols, False)
#     FROM_colname_ref, TO_colname_ref = find_colnames(ref_cols, True)
#     FROM_colname_act, TO_colname_act = find_colnames(act_cols, True)
#     FROM_colname_loc, TO_colname_loc = find_colnames(loc_cols, True)
#     FROM_colname_unit, TO_colname_unit = find_colnames(unit_cols, True)
    
#     # Extract column for the multiplier separately
#     colname_mult_orig: list[str] = [m for m in mult_cols if m in new_header]
    
#     # Raise error if the multiplier column was not found
#     assert len(colname_mult_orig) > 0, "No multiplier column found. Used possible columns '" + str(mult_cols) + "'. Error occured in file '" + str(filename) + "'"
    
#     # Use the first element as column name for multiplier
#     colname_mult: str = colname_mult_orig[0]
    
#     # Loop through all entries in the dataframe
#     for idx, row in df.iterrows():
        
#         # Write column values to variables
#         FROM_actID, TO_actID = row.get(FROM_colname_actID), row.get(TO_colname_actID)
#         FROM_prodID, TO_prodID = row.get(FROM_colname_prodID), row.get(TO_colname_prodID)
#         FROM_ref, TO_ref = row.get(FROM_colname_ref), row.get(TO_colname_ref)
#         FROM_act, TO_act = row.get(FROM_colname_act), row.get(TO_colname_act)
#         FROM_loc, TO_loc = row.get(FROM_colname_loc), row.get(TO_colname_loc)
#         FROM_unit_orig, TO_unit_orig = row.get(FROM_colname_unit), row.get(TO_colname_unit)
#         multiplier = row.get(colname_mult)
        
#         # Use normalize unit function from Brightway
#         FROM_unit, TO_unit = normalize_units_function(FROM_unit_orig), normalize_units_function(TO_unit_orig)
        
#         # There might be cases where the multiplier is not a number
#         # We need to handle such exceptions explicitely
#         if not isinstance(multiplier, (int | float)):
            
#             # Let's handle how to manage the multiplier if it is a text
#             if isinstance(multiplier, str):
            
#                 # If it is an empty text string, we can (safely?) assume that it is a 1
#                 if multiplier.strip() == "":
#                     multiplier: float = float(1)
                
#                 # Newly introduced datasets actually do not need a multiplier, but we use here a 1 as a placeholder
#                 elif multiplier.strip().lower() == "new":
#                     multiplier: float = float(1)
                  
#                 # In the case where we find "no recommended share", we set the multiplier to 0. That means, those activities will not be mapped further (see code below).
#                 elif re.search("no recommended share", multiplier.strip().lower()):
#                     multiplier: float = float(0)
                
#                 else:
#                     # For any other text that we do not yet capture, we raise an error to see how to deal with it
#                     raise ValueError("Strange text multiplier specified in correspondence file '{}'. No rule has been defined on how to handle the mulitplier '{}' yet.".format(filename, multiplier))
                    
#             else:
#                 # If the multiplier is something else than a text, let's raise an error. We need to define new rules if that happens.
#                 raise ValueError("Multiplier's type ('{}') is not a number and no rules have been set to handle it right now.".format(type(multiplier)))
        
        
#         # Create FROM_ID
#         FROM_ID = (
#                    FROM_version,
#                    FROM_actID,
#                    FROM_prodID,
#                    FROM_ref,
#                    FROM_act,
#                    FROM_loc,
#                    FROM_unit)
        
#         # Create TO_ID
#         TO_ID = (
#                  TO_version,
#                  TO_actID,
#                  TO_prodID,
#                  TO_ref,
#                  TO_act,
#                  TO_loc,
#                  TO_unit
#                  )
        
#         # Initialize ID in dictionary if not yet existing
#         if FROM_ID + (TO_version,) not in check_multipliers:
#             check_multipliers[FROM_ID + (TO_version,)]: float = float(multiplier)
            
#         else:
#             # Add to multipliers checking dictionary
#             check_multipliers[FROM_ID + (TO_version,)] += multiplier
        
        
#         # !!! We need to do the inverse for the multiplier. Do we???
#         # According to the documentation from correspondence file v3.4/v3.5 (comment of cell N3)
#         # If the multiplier is 1 it means the eiv3.4 activity is replaced by exactly one activity in eiv3.5.
#         # If the multiplier is different (below one) it represents the share of the replacement activity.
#         # E.g. if the value here is 0.25 then 25% of the eiv3.4 activity should be replaced by the specific activity in eiv3.5.
#         # The share of the eiv3.4 matches of course sum up to 1.
#         # inversed_multiplier: (int | float) = 1 / multiplier if multiplier != 0 else 0
        
#         # Create ID
#         ID = FROM_ID + TO_ID
        
#         # Raise error if key is already available
#         assert ID not in all_data, "Key is already present. Error occured for key '" + str(ID) + "' when reading file '" + str(filename) + "'"
            
#         # Append to data dictionary
#         all_data[ID]: dict = {
#                             "FROM_version": FROM_version,
#                             "FROM_activity_UUID": FROM_actID,
#                             "FROM_product_UUID": FROM_prodID,
#                             "FROM_activity": FROM_act,
#                             "FROM_reference_product": FROM_ref,
#                             "FROM_location": FROM_loc,
#                             "FROM_unit": FROM_unit,
#                             "multiplier": multiplier,
#                             "TO_version": TO_version,
#                             "TO_activity_UUID": TO_actID,
#                             "TO_product_UUID": TO_prodID,
#                             "TO_activity": TO_act,
#                             "TO_reference_product": TO_ref,
#                             "TO_location": TO_loc,
#                             "TO_unit": TO_unit
#                             }
        
# # Create dataframe with standardized data
# standardized: pd.DataFrame = pd.DataFrame(list(all_data.values())).replace({"": None})

# #%%
# multipliers_not_summing_to_1_orig: dict = {k: v for k, v in check_multipliers.items() if (v < 0.99 or v > 1.01) and (v != 0) and ("" not in k) and (None not in k)}
# # multipliers_not_summing_to_1_orig: dict = {k: v for k, v in check_multipliers.items() if (v != 0) and ("" not in k) and (None not in k)}
# # multipliers_not_summing_to_1_orig: dict = {k: v for k, v in check_multipliers.items() if (v < 0.99 or v > 1.01) and (v != 0) and ("" not in k) and (None not in k)}
# # print(set(check_multipliers.values()))
# to_extractable_items: dict = {(FROM_version, FROM_act_uuid, FROM_prod_uuid, FROM_act, FROM_prod, FROM_loc, FROM_unit, TO_version): v for (FROM_version, FROM_act_uuid, FROM_prod_uuid, FROM_act, FROM_prod, FROM_loc, FROM_unit, TO_version), v in multipliers_not_summing_to_1_orig.items()}

# multipliers_not_summing_to_1: list[dict] = []
# for item in list(all_data.values()):
#     ID: tuple = (item["FROM_version"], item["FROM_activity_UUID"], item["FROM_product_UUID"], item["FROM_activity"], item["FROM_reference_product"], item["FROM_location"], item["FROM_unit"], item["TO_version"])
#     if to_extractable_items.get(ID) is not None:
#         multipliers_not_summing_to_1 += [{**item, **{"multiplier_summed": to_extractable_items.get(ID)}}]
        
# df_multipliers_not_summing_to_1: pd.DataFrame = pd.DataFrame(multipliers_not_summing_to_1)
# df_multipliers_not_summing_to_1.to_excel(here / "multipliers_not_summing_to_1.xlsx", index = False)

# #%%

# final: dict[str, pd.DataFrame] = {}
# final_raw: dict[str, pd.DataFrame] = {}
# final_failed: dict[str, pd.DataFrame] = {}
# final_deleted: dict[str, pd.DataFrame] = {}
# final_newly_introduced: dict[str, pd.DataFrame] = {}

# TO_version: tuple = (3, 12)
# versions_to_loop_through: set = set(standardized["FROM_version"])

# for FROM_version in versions_to_loop_through:
    
#     if FROM_version >= TO_version:
#         continue
    
#     print(FROM_version, "->", TO_version)
    
#     cols_1: tuple[str] = ("version", "activity", "reference_product", "location", "unit")
#     cols_2: tuple[str] = ("version", "activity_UUID", "product_UUID")
#     cols_1_without_version: tuple[str] = cols_1[1:]
#     cols_2_without_version: tuple[str] = cols_2[1:]
    
#     curr_df: pd.DataFrame = standardized.query("FROM_version == @FROM_version and multiplier != 0").add_suffix("_1")
#     counter: int = 1
#     counters: list[int] = []
    
#     while True:
        
#         # Add current counter to list
#         counters += [counter]
        
#         left_on_1: list[str] = [("TO_" + m + "_" + str(counter)) for m in cols_1]
#         right_on_1: list[str] = [("FROM_" + m + "_" + str(counter + 1)) for m in cols_1]
        
#         # Exclude rows that have incomplete cols_1
#         # excluded_1 = curr_df.copy().replace({"": pd.NA, None: pd.NA}).loc[:, left_on_1].isna().any(axis = 1)
#         mask_1 = curr_df[left_on_1].isna().any(axis = 1) | (curr_df[left_on_1] == "").any(axis = 1)
#         excluded_1 = curr_df.copy()[mask_1]
#         curr_df = curr_df.copy()[~mask_1]
        
#         # Merge on cols 1
#         curr_df = curr_df.copy().merge(
#             standardized.add_suffix("_" + str(counter + 1)),
#             how = "left",
#             left_on = left_on_1,
#             right_on = right_on_1,
#         )
        
#         unmatched = curr_df[right_on_1].isna().any(axis = 1)
#         unmatched_df = curr_df[unmatched].drop(columns = curr_df.filter(regex = "_" + str(counter + 1) + "$").columns)
        
#         left_on_2: list[str] = [("TO_" + m + "_" + str(counter)) for m in cols_2]
#         right_on_2: list[str] = [("FROM_" + m + "_" + str(counter + 1)) for m in cols_2]
        
#         # Exclude rows that have incomplete cols_2
#         mask_2 = unmatched_df[left_on_2].isna().any(axis = 1) | (unmatched_df[left_on_2] == "").any(axis = 1)
#         excluded_2 = unmatched_df.copy()[mask_2]
#         unmatched_df = unmatched_df.copy()[~mask_2]
        
#         # Merge on cols 2
#         matched_df = unmatched_df.merge(
#             standardized.add_suffix("_" + str(counter + 1)),
#             how = "left",
#             left_on = left_on_2,
#             right_on = right_on_2,
#         )
        
#         curr_df: pd.DataFrame = pd.concat([curr_df.copy()[~unmatched],
#                                            matched_df.copy(),
#                                            excluded_1.copy(),
#                                            excluded_2.copy()])
        
#         if curr_df[right_on_1 + right_on_2].isna().all().all():
#             curr_df = curr_df.drop(columns = curr_df.filter(regex = "_" + str(counter + 1) + "$").columns)
#             break
        
#         if TO_version in set(curr_df["TO_version_" + str(counter)]):
#             curr_df = curr_df.drop(columns = curr_df.filter(regex = "_" + str(counter + 1) + "$").columns)
#             break
        
#         if counter == 50: # !!! Safety step
#             raise ValueError("Aborted manually, was not breaking out of the while loop!")
        
#         # Raise counter at the end
#         counter += 1
    
    
#     curr_df = curr_df.copy().replace({float("NaN"): None})
#     curr_df["Multiplier"] = curr_df.copy().filter(regex = "multiplier").prod(axis = 1).astype(float)
#     final_raw[(FROM_version, TO_version)] = curr_df.copy()
#     curr_df["Multipliers"] = curr_df.copy().filter(regex = "multiplier").astype(str).agg(" * ".join, axis = 1)
    
#     for i in cols_1:
#         curr_df[i[0].upper() + i[1:].lower()] = curr_df.copy().filter(regex = "FROM_{}_{}$|TO_{}".format(i, min(counters), i)).astype(str).agg(" -> ".join, axis = 1)
    
#     for i in cols_2_without_version:
#         curr_df[i[0].upper() + i[1:].lower()] = curr_df.copy().filter(regex = "FROM_{}_{}$|TO_{}".format(i, min(counters), i)).astype(str).agg(" -> ".join, axis = 1)
    
#     curr_df = curr_df.copy().filter(regex = "(^FROM_(.*)_{}$)|(^TO_(.*)_{}$)|^Multiplier|{}".format(min(counters), max(counters), "|".join(["(^" + m[0].upper() + m[1:].lower() + "$)" for m in cols_1 + cols_2_without_version])))        
#     curr_df.columns = curr_df.copy().columns.str.replace("_[0-9]+$", "", regex = True)
    
#     failed = (curr_df["TO_version"].isna()) | (curr_df["TO_version"] != TO_version) | (curr_df["Multiplier"] == 0)
#     failed_df = curr_df.copy()[failed]
#     curr_df = curr_df.copy()[~failed]
        
#     deleted = curr_df.copy().filter(regex = "|".join(["TO_" + m for m in cols_1_without_version + cols_2_without_version])).isna().all(axis = 1)
#     deleted_df = curr_df.copy()[deleted]
#     curr_df = curr_df.copy()[~deleted]
    
#     newly_introduced = curr_df.copy().filter(regex = "|".join(["FROM_" + m for m in cols_1_without_version + cols_2_without_version])).isna().all(axis = 1)
#     newly_introduced_df = curr_df.copy()[newly_introduced]
#     curr_df = curr_df.copy()[~newly_introduced]
    
#     final[(FROM_version, TO_version)] = curr_df.copy()
#     final_failed[(FROM_version, TO_version)] = failed_df.copy()
#     final_deleted[(FROM_version, TO_version)] = deleted_df.copy()
#     final_newly_introduced[(FROM_version, TO_version)] = newly_introduced_df.copy()

# final_df: pd.DataFrame = pd.concat([m for _, m in final.items()])
# final_failed_df: pd.DataFrame = pd.concat([m for _, m in final_deleted.items()])
# final_deleted_df: pd.DataFrame = pd.concat([m for _, m in final_deleted.items()])
# final_newly_introduced: pd.DataFrame = pd.concat([m for _, m in final_newly_introduced.items()])
# documentation: dict[str, str] = {"all": "",
#                                  "failed": "",
#                                  "deleted": "",
#                                  "newly_introduced": ""}

# # Write dataframes to Excel
# with pd.ExcelWriter(here / "correspondence_files.xlsx", engine = "xlsxwriter") as writer:
# #     # documentation.to_excel(writer, sheet_name = "documentation", index = False)
#     for (FROM_version, TO_version), df in final.items():
#         df.to_excel(writer, sheet_name = ".".join([str(m) for m in FROM_version]) + " -> " + ".".join([str(m) for m in TO_version]), index = False)
    
#     final_df.to_excel(writer, sheet_name = "all", index = False)
#     final_failed_df.to_excel(writer, sheet_name = "failed", index = False)
#     final_deleted_df.to_excel(writer, sheet_name = "deleted", index = False)
#     final_newly_introduced.to_excel(writer, sheet_name = "newly_introduced", index = False)


# with pd.ExcelWriter(here / "correspondence_files_RAW.xlsx", engine = "xlsxwriter") as writer:
#     pd.concat([m for _, m in final_raw.items()]).to_excel(writer, sheet_name = "all_RAW", index = False)

# correspondence_mapping_by_uuids: dict = {}
# correspondence_mapping_by_names: dict = {}

# FROM_version: tuple = (3, 8)
# TO_version: tuple =(3, 10)
# df_to_create_migration_for: pd.DataFrame = final
# for idx, line in final_df.iterrows():
#     ...
    
    
