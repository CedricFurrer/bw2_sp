import copy
import json
import bw2io
import bw2data
import pathlib
import pandas as pd

if __name__ == "__main__":
    import os
    os.chdir(pathlib.Path(__file__).parent)

from utils import (map_using_SBERT,
                   change_brightway_project_directory)
from helper import (check_function_input_type)

#%% Helper functions

# Function to strip all whitespaces in all fields in a tuple and convert the fields to lowercase
def prep(fields: tuple):
    return tuple([m.lower().strip() for m in fields])

# Add additional string to the beginning of the keys of a dictionary
def update_keys(dct: dict, string: str):
    return {string + k: v for k, v in dct.items()}


#%% Function to link biosphere flows between two biospheres

def create_harmonized_biosphere_migration(biosphere_flows_1: list,
                                          biosphere_flows_2: list,
                                          manually_checked_SBERTs: (pd.DataFrame | None),
                                          output_path: pathlib.Path,
                                          ecoquery_username: (str | None),
                                          ecoquery_password: (str | None)) -> dict:
    
    # 1 = ecoinvent XML # !!!
    # 2 = SimaPro # !!!
    
    # Check function input type
    check_function_input_type(create_harmonized_biosphere_migration, locals())
    
    # Extract the lists of unique names of biosphere flows from the two biospheres
    unique_names_1: tuple = tuple(set([m["name"] for m in biosphere_flows_1]))
    unique_names_2: tuple = tuple(set([m["name"] for m in biosphere_flows_2]))

    # We now try to map using SBERT
    name_mapping_via_SBERT: pd.DataFrame = map_using_SBERT(unique_names_1, unique_names_2, 3)
    
    # A mapping dictionary for biosphere flows using the unique fields 'name', 'top_category', 'sub_category', 'unit', 'location'
    mapping: dict = {prep((m["name"], m["top_category"], m["sub_category"], m["unit"], m["location"])): m for m in biosphere_flows_2 if m["location"] == "GLO"}
    
    # Construct mapping dictionaries of ecoinvent to SimaPro names based on the results from the SBERT mapping beforehand
    # We are considering three different mappings. The first uses 100% correctly mapped names by only taking the mapped items with cosine score 1
    SBERT_mapping_1: dict = {prep((n["orig"],))[0]: n["mapped"] for idx, n in name_mapping_via_SBERT.iterrows() if n["score"] > 0.98}

    # The second one takes the second best mapped item with the second best cosine score
    SBERT_mapping_2: dict = {prep((n["orig"],))[0]: n["mapped"] for idx, n in name_mapping_via_SBERT.iterrows() if prep((n["orig"],))[0] not in SBERT_mapping_1 and n["ranking"] == 3}

    # The second one takes the third best mapped item with the third best cosine score
    SBERT_mapping_3: dict = {prep((n["orig"],))[0]: n["mapped"] for idx, n in name_mapping_via_SBERT.iterrows() if prep((n["orig"],))[0] not in SBERT_mapping_1 and n["ranking"] == 2}
    
    if manually_checked_SBERTs is not None:
        SBERT_mapping_2_validated: dict = {prep((m,))[0]: [{"mapped": n["mapped"], "multiplier": n["multiplier"]} for idx, n in manually_checked_SBERTs.query("orig == @m").iterrows()] for m in set(list(manually_checked_SBERTs)) if prep((m,))[0] not in SBERT_mapping_1}
    else:
        SBERT_mapping_2_validated: dict = SBERT_mapping_2 # !!! correct?
    
    # Extract all unique names with the respective CAS number from both lists of biosphere flows
    unique_names_CAS_1: set = set([(m["name"], m["CAS number"]) for idx, m in biosphere_flows_1 if m["CAS number"] is not None])
    unique_names_CAS_2: set = set([(m["name"], m["CAS number"]) for idx, m in biosphere_flows_2 if m["CAS number"] is not None])

    # Construct a mapping dictionary for flows from biosphere 2 where key is the CAS number and value is the name of the flow
    name_to_CAS_mapping: dict = {n: m for m, n in unique_names_CAS_2}

    # Construct a mapping file of biosphere 1 to biosphere 2 flow names via the CAS number
    name_mapping_via_CAS: list[dict] = [{"orig": m[0],
                                         "mapped": name_to_CAS_mapping.get(m[1]),
                                         "score": 1 if name_to_CAS_mapping.get(m[1]) is not None else 0} for m in biosphere_flows_1]

    # Construct a mapping of biosphere 1 to biosphere 2 flow names via the CAS nr.
    CAS_mapping: dict = {prep((n["orig"],))[0]: n["mapped"] for idx, n in name_mapping_via_CAS if n["score"] == 1}

    # Construct a custom unit mapping
    unit_mapping: dict = {"cubic meter": ("kilogram", 0.001),
                          "square meter": ("square meter-year", 365),
                          "standard cubic meter": ("cubic meter", 1),
                          "megajoule": ("kilowatt hour", 3.6)}
    
    # Function to get closest biosphere flow match between two dictionaries
    def get_closest_match(FROM_item: dict,
                          TO_item: dict,
                          mapping: dict) -> (dict | None):
        
        TO_item_simplified = prep((TO_item["name"], TO_item["top_category"], TO_item["sub_category"], TO_item["unit"], TO_item["location"]))
        found: (dict | None) = mapping.get(TO_item_simplified)
        
        if found is not None:
            return {**FROM_item, **update_keys(found, "TO_"), **{"multiplier": TO_item["multiplier"], "quality": TO_item["quality"], "quality_comment": TO_item["quality_comment"]}}
        else:
            return None
    
    
    # Initialize list
    mapping: list = []

    # Loop through each item that needs to be mapped
    for item in biosphere_flows_1:
        
        # Store all original information and append 'FROM_' string to the keys in the dictionary
        FROM_item = copy.deepcopy(update_keys(item, "FROM_"))
        
        # Make variables of the most relevant fields of the current item for easier handling
        name: str = item["name"]
        top_category: str = item["top_category"]
        sub_category: (str | None) = item["sub_category"] # !!! None or empty string?
        unit: str = item["unit"]
        location: str = "GLO"
        
        # Find new names and new units based on the mappings beforehand
        name_SBERT_mapped_1: str = SBERT_mapping_1.get(prep((name,))[0], "")
        names_SBERT_mapped_2_validated: (list[str] | list) = SBERT_mapping_2_validated.get(prep((name,))[0], [])
        name_SBERT_mapped_2: str = SBERT_mapping_2.get(prep((name,))[0], "")
        name_SBERT_mapped_3: str = SBERT_mapping_3.get(prep((name,))[0], "")
        name_CAS_mapped: str = CAS_mapping.get(prep((name,))[0], "")
        unit_mapped: str = unit_mapping.get(unit, (unit, float(1)))[0]
        unit_multiplier_mapped: float = unit_mapping.get(unit, (unit, float(1)))[1]
        
        # Initialize a dictionary with the raw information
        initial_ID: dict = {"name": name,
                            "top_category": top_category,
                            "sub_category": sub_category,
                            "unit": unit,
                            "location": location,
                            "multiplier": float(1)}
        
        # Hierarchy and documentation for the matches found
        mapping_info: dict[int, str] = {1: "Exact match",
                                        2: "using new name from SBERT mapping (cosine similarity of 1)",
                                        3: "using new name from manually validated SBERT mapping",
                                        4: "using new name from CAS mapping",
                                        5: "Exact match | empty sub category",
                                        6: "using new name from SBERT mapping (cosine similarity of 1) | empty sub category",
                                        7: "using new name from manually validated SBERT mapping | empty sub category",
                                        8: "using new name from CAS mapping and empty sub category",
                                        9: "using new name from SBERT mapping (2nd priority)",
                                        10: "using new name from SBERT mapping (2nd priority) | empty sub category",
                                        11: "using new name from SBERT mapping (3th priority)",
                                        12: "using new name from SBERT mapping (3th priority) | empty sub category"
                                        }
        
        # Assign the information according to their hierarchy to a dictionary
        prios_1_built: dict[int, dict] = {1: initial_ID,
                                          2: dict(initial_ID, **{"name": name_SBERT_mapped_1}),
                                          3: [dict(initial_ID, **{"name": m["mapped"], "multiplier": m["multiplier"]}) for m in names_SBERT_mapped_2_validated],
                                          4: dict(initial_ID, **{"name": name_CAS_mapped}),
                                          5: dict(initial_ID, **{"sub_category": ""}),
                                          6: dict(initial_ID, **{"name": name_SBERT_mapped_1, "sub_category": ""}),
                                          7: [dict(initial_ID, **{"name": m["mapped"], "sub_category": "", "multiplier": m["multiplier"]}) for m in names_SBERT_mapped_2_validated],
                                          8: dict(initial_ID, **{"name": name_CAS_mapped, "sub_category": ""}),  
                                          9: dict(initial_ID, **{"name": name_SBERT_mapped_2}),
                                          10: dict(initial_ID, **{"name": name_SBERT_mapped_2, "sub_category": ""}),
                                          11: dict(initial_ID, **{"name": name_SBERT_mapped_3}),
                                          12: dict(initial_ID, **{"name": name_SBERT_mapped_3, "sub_category": ""})
                                          }
        
        # Create a flattened list of all matches --> convert from dictionary to list of tuples
        prios_1_flattened: list = []
        
        # Loop through each key/value pair
        for k, v in prios_1_built.items():
            
            # We flatten the list before appending, if the current value is of type list
            if isinstance(v, list):
                prios_1_flattened += [(k, m) for m in v]
            else:
                # Otherwise, we just append the key/value pair
                prios_1_flattened += [(k, v)]
        
        # We add the documentation of the priority
        prios_1_standardized: list[dict] = [dict(v, **{"quality": k, "quality_comment": mapping_info[k]}) for k, v in prios_1_flattened]
        
        # We remove the priorities where no match was found
        prios_1_cleaned: list[dict] = [m for m in copy.deepcopy(prios_1_standardized) if m["name"] != ""]
        
        # We now basically duplicate the priorities
        # Why? Because each priority could be also possible with another/transformed unit
        mapping_info_units: dict = {k + 12: v + " | new unit" for k, v in mapping_info.items()}
        
        # We try to map the current unit, if possible
        prios_2_cleaned: list[dict] = [dict(m, **{"unit": unit_mapped, "multiplier": m["multiplier"] * unit_multiplier_mapped, "quality": m["quality"] + 12, "quality_comment": mapping_info_units[m["quality"] + 12]}) for m in copy.deepcopy(prios_1_cleaned)]
        
        # Find the closest match between two dicts
        found_orig: list[dict | None] = [get_closest_match(FROM_item = FROM_item,
                                                           TO_item = TO_item,
                                                           mapping = mapping) for TO_item in prios_1_cleaned] + [get_closest_match(FROM_item = FROM_item,
                                                                                                                                   TO_item = TO_item,
                                                                                                                                   mapping = mapping) for TO_item in prios_2_cleaned]
        # We remove the unmatched entries
        found_cleaned: list[dict] = [m for m in found_orig if m is not None]
        
        # We now append the best match with the highest priority to our list
        # We check if at least one match was found
        if len(found_cleaned) > 0:
            
            # If yes, we sort the list starting with the highest quality (in our case = lowest number)
            found: list[dict] = sorted(found_cleaned, key = lambda x: x["quality"])
            
            # We then use the first element
            mapping += [found[0]]
        else:
            # Otherwise, we append only the FROM item indicating that there was no good match found
            mapping += [FROM_item]
    
    # Identifying fields
    ID_fields = ("name", "categories", "unit")
    
    # Create pandas dataframe of the result from the mapping
    result: pd.DataFrame = pd.DataFrame(mapping)

    # Add an ID of each flow
    result["ID"] = [tuple([tuple(map(lambda x: x.lower().strip(), flow.get("FROM_" + m))) if isinstance(flow.get("FROM_" + m), tuple) else flow.get("FROM_" + m, "").lower().strip() for m in ID_fields]) for flow in mapping]
    
    try:
        # Extract biosphere flows which are not used in any LCIA methods
        df_flows_not_in_methods, methods_mapping = create_dataframe_of_elementary_flows_that_are_not_used_in_methods(version = "3.10",
                                                                                                                     model_type = "cutoff",
                                                                                                                     output_path = pathlib.Path(__file__).parent / "notebook" / "notebook_data",
                                                                                                                     username = ecoquery_username,
                                                                                                                     password = ecoquery_password,
                                                                                                                     delete_temp_project = False)
    
        # Make a dictionary of flows which were not found at all
        no_methods_mapping = {k: v for k, v in methods_mapping.items() if v == "- "}
        
        # Check if the flows are used in LCIA methods in ecoinvent
        result["flow_used_in_any_method_from_ecoinvent_XML"] = [False if m in no_methods_mapping else True for m in list(result["ID"])]
    
        # Write the methods in which the flows are used
        result["method_names_where_flow_is_used"] = ["" if m in no_methods_mapping else methods_mapping[m] for m in list(result["ID"])]
    
    except:
        pass
    
    # Construct two new dataframes
    # ... filter the results and show only the successfully mapped items
    successful: pd.DataFrame = result[result["quality"].isna() == False]

    # Exclude some data quality fields where mapping is inappropriate
    exclude_data_quality: list[int] = [9, 10, 11, 12, 21, 22, 23, 24]

    # ... filter the results and show only the unsucessfully mapped items
    missing: pd.DataFrame = result[(result["quality"].isna() == True) | (result["quality"].isin(exclude_data_quality))]

    # We need to check the SBERT mapping manually
    # We therefore write the list of all SBERT mappings that we used, exluding the ones where cosine similarity was 1
    SBERT_used: pd.DataFrame = successful.query("quality in @exclude_data_quality")
    SBERT_used_unique: list = list(set(list(SBERT_used.FROM_name)))
    SBERT_to_check: pd.DataFrame = name_mapping_via_SBERT.query("orig in @SBERT_used_unique")

    # Create summary dataframe
    # Create detailed grouping of successfully matched flows using the quality criteria
    successful_summary_1: pd.DataFrame = successful.groupby(["quality", "quality_comment"]).agg({"quality": "count"}).rename(columns = {"quality": "n"}).reset_index().rename(columns = {"quality_comment": "description"}).assign(**{"type": "matched"})
    successful_summary_2: pd.DataFrame = pd.DataFrame({"description": "Total", "n": len(successful), "type": "matched"}, index = [1])

    # Create an empty row
    empty: pd.DataFrame = pd.DataFrame({"n": float("NaN")}, index = [1])

    # Create row with the total number of flows which are missing
    missing_summary: pd.DataFrame = pd.DataFrame({"description": "Total", "n": len(missing), "type": "unmatched"}, index = [1])

    # Create row for the total of flows (matched and unmatched)
    total_summary: pd.DataFrame = pd.DataFrame({"n": len(successful) + len(missing), "description": "Total", "type": "matched + unmatched"}, index = [1])

    # Concat the dataframes
    summary: pd.DataFrame = pd.concat([successful_summary_1, empty, successful_summary_2, missing_summary, total_summary])
    
    # Documentation dataframe
    documentation: pd.DataFrame = pd.DataFrame([{"quality": k, "quality_comment": v} for k, v in mapping_info.items()])
    
    # Write dataframes to Excel
    with pd.ExcelWriter(output_path / "biosphere_harmonization.xlsx", engine = "xlsxwriter") as writer:
        documentation.to_excel(writer, sheet_name = "documentation", index = False)
        successful.to_excel(writer, sheet_name = "biosphere_matched", index = False)    
        missing.to_excel(writer, sheet_name = "biosphere_unmatched", index = False)  
        SBERT_to_check.to_excel(writer, sheet_name = "SBERT_to_check", index = False)
        summary.to_excel(writer, sheet_name = "biosphere_summary", index = False)

    # Construction of the Brightway2 migration dictionary
    # Specify the fields that should be used as 'fields' in the migration dictionary
    FROM_fields = ("code",)

    # Specify the fields to which should be mapped to
    TO_fields = ("name",
                 "SimaPro_name",
                 "categories",
                 "top_category",
                 "sub_category",
                 "SimaPro_categories",
                 "unit",
                 "SimaPro_unit",
                 "location")

    # Initialize empty migration dictionary
    migration_data = {"fields": FROM_fields + ("type",),
                      "data": []}

    # Write all items which have been successfully mapped to the data field in the migration dictionary
    for m in result.fillna("").to_dict("records"):
        
        # Field one always specifies to which item the mapping should be applied.
        one: list[str] = [str(m["FROM_" + n]) for n in FROM_fields] + ["biosphere"]
        
        # Field two always specifies the new data that should be applied to update the original data
        two: dict = dict(**{str(o): str(m["TO_" + o]) for o in TO_fields}, **{"multiplier": m["multiplier"]})
        
        # We do not map anything if mapping was not successful beforehand
        if m["quality"] == "" or m["quality"] in exclude_data_quality:
            two: dict = {n: m["FROM_" + n] for n in TO_fields if "FROM_" + n in m}

        # Add to migration dictionary
        migration_data["data"] += [[one, two]]
        
    return migration_data



#%% Check which flows are not used in LCIA methods from ecoinvent XML

# We create a dataframe for all flows that are found that are not used in any of the LCIA methods from ecoinvent XML
def create_dataframe_of_elementary_flows_that_are_not_used_in_methods(version: str,
                                                                      model_type: str,
                                                                      output_path: pathlib.Path,
                                                                      username: (str | None),
                                                                      password: (str | None),
                                                                      delete_temp_project: bool) -> (pd.DataFrame, dict):
    
    # Check function input type
    check_function_input_type(create_dataframe_of_elementary_flows_that_are_not_used_in_methods, locals())
    
    # We first read in all the data from ecoinvent XML
    # We write all the elementary flows and the LCIA methods to a Brightway project
    # Specify the folder where to store the Brightway project as well as the folder
    temp_brightway_folder: str = "_TEMP"
    temp_project: str = "TEMP_PROJECT"
    temp_folder_path: pathlib.Path = output_path / temp_brightway_folder
    
    # If the current directory is not yet available, create it first
    temp_folder_path.mkdir(exist_ok = True)
    
    # We change the Brightway project folder path to the one specified in the variables
    change_brightway_project_directory(output_path / temp_brightway_folder)
    
    # We open the project
    bw2data.projects.set_current(temp_project)
    
    # Let's first check, if the biosphere is already available.
    # If yes, we do not need to read it. Otherwise, we read the data directly from ecoinvent webpage by using the specific importer function
    if "biosphere" not in bw2data.databases:
        bw2io.ecoinvent.import_ecoinvent_release(version, model_type, username = username, password = password, lci = False, lcia = True, biosphere_name = "biosphere", use_mp = False)

    # We extract all biosphere flows from the background database
    all_biosphere_flows: list = bw2data.Database("biosphere")
    
    # We extract all the LCIA methods from the background
    methods_dict: dict = {m: bw2data.Method(m).load() for m in bw2data.methods}
    
    # We initialize two variables
    # The first one stores will store all biosphere flows which were not found in any of the methods
    flows_not_in_methods: dict = {}
    
    # The second one will store the biosphere flow as a key, and the method name(s) where it was found as value
    methods_mapping: dict = {}
    
    # We loop through each flow
    for flow in all_biosphere_flows:
        
        # Initialize variables
        # As a default, we say that the flow is not found
        available: bool = False
        
        # We initialize a list where we store all the LCIA method names where the flow is used
        methods_gathered: list = []
        
        # We loop through all LCIA methods and check, whether the current flow is used in that method or not
        for method_name, loaded in methods_dict.items():
            
            # Check if the flow code is present in the method
            if flow["code"] in [n[0][1] for n in loaded]:
                
                # If yes, we change the status to found (= True)
                available: bool = True
                
                # We also add the method name to our list
                methods_gathered += [", ".join(method_name)]
                
        # If the flow was not found, we store it to our list
        if not available and flow["code"] not in flows_not_in_methods:
            flows_not_in_methods[flow["code"]]: dict = {k: (" | ".join(v) if isinstance(v, list | tuple) else v) for k, v in flow.as_dict().items()}
        
        # We also append our methods mapping
        # We first extract the fields of the current flow which make the flow unique
        ID: tuple = tuple([tuple(map(lambda x: x.lower().strip(), flow.get(m))) if isinstance(flow.get(m), tuple) else flow.get(m, "").lower().strip() for m in ("name", "categories", "unit")])
        
        # We append the ID and all the methods where it was found/not found
        methods_mapping[ID]: str = "- " + "\n- ".join(["'" + a + "'" for a in list(set(methods_gathered))])
    
    # We write the flows to a dataframe
    df_flows_not_in_methods: pd.DataFrame = pd.DataFrame(list(flows_not_in_methods.values()))

    # If specified, we delete the project at the end
    if delete_temp_project:
        bw2data.projects.delete_project(temp_project, True)
    
    return df_flows_not_in_methods, methods_mapping




