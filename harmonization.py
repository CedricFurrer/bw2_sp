import pathlib

if __name__ == "__main__":
    import os
    os.chdir(pathlib.Path(__file__).parent)

import pandas as pd
from utils import (map_using_SBERT)
from helper import (check_function_input_type)

#%% Helper functions

# Function to strip all whitespaces in all fields in a tuple and convert the fields to lowercase
def prep(fields: tuple):
    return tuple([m.lower().strip() if m is not None else None for m in fields])

# Add additional string to the beginning of the keys of a dictionary
def update_keys(dct: dict, string: str):
    return {string + k: v for k, v in dct.items()}

#%% Function to link biosphere flows between two biospheres

def create_harmonized_biosphere_migration(biosphere_flows_1: list,
                                          biosphere_flows_2: list,
                                          manually_checked_SBERTs: (list | pd.DataFrame | None)) -> dict:
    
    # Check function input type
    check_function_input_type(create_harmonized_biosphere_migration, locals())
    
    # Map using SBERT
    df_SBERT_mapping: pd.DataFrame = map_using_SBERT(tuple(set([n["name"] for n in biosphere_flows_1])), tuple(set([m["name"] for m in biosphere_flows_2])), 3)
    
    # Initialize empty dictionaries
    SBERT_mapping_dict_1: dict = {}
    SBERT_mapping_dict_3: dict = {}
    
    for idx, row in df_SBERT_mapping.iterrows():
        
        if row["ranking"] == 3 and row["score"] >= 0.98:
            
            if row["orig"] not in SBERT_mapping_dict_1:
                SBERT_mapping_dict_1[row["orig"]]: dict = {}
            
            if row["mapped"] not in SBERT_mapping_dict_1:
                SBERT_mapping_dict_1[row["mapped"]]: dict = {}
            
            SBERT_mapping_dict_1[row["orig"]][row["mapped"]]: float = float(1)
            SBERT_mapping_dict_1[row["mapped"]][row["orig"]]: float = float(1)
            
        if row["orig"] not in SBERT_mapping_dict_3:
            SBERT_mapping_dict_3[row["orig"]]: dict = {}
            
        SBERT_mapping_dict_3[row["orig"]][row["mapped"]]: float = float(row["score"])
    
    
    if isinstance(manually_checked_SBERTs, pd.DataFrame):
        manually_checked_SBERTs: list[dict] = manually_checked_SBERTs.replace({float("NaN"): None}).to_dict("records")
    
    elif manually_checked_SBERTs is None:
        manually_checked_SBERTs: list = []
    
    SBERT_mapping: dict = SBERT_mapping_dict_1
    for m in manually_checked_SBERTs:
        
        if m.get("orig") is not None and m.get("mapped") is not None and m.get("multiplier") is not None:
            
            if m["orig"] not in SBERT_mapping:
                SBERT_mapping[m["orig"]]: dict = {}
                
            SBERT_mapping[m["orig"]][m["mapped"]]: float = float(m["multiplier"])
            
            if m.get("multiplier") != 0:
                
                if m["mapped"] not in SBERT_mapping:
                    SBERT_mapping[m["mapped"]]: dict = {}
                
                SBERT_mapping[m["mapped"]][m["orig"]]: float = float(1 / m["multiplier"])

    
    # Create biosphere mapping dictionary
    biosphere_mapping: dict = {}
    
    for m in biosphere_flows_2:
        
        name: (str | None) = m.get("name") if m.get("name") != "" else None
        SBERTs: dict = SBERT_mapping.get(name) if SBERT_mapping.get(name) is not None else {}
        CAS: (str | None) = m.get("CAS number") if m.get("CAS number") != "" else None
        top_category: (str | None) = m.get("top_category") if m.get("top_category") != "" else None
        sub_category: str = m.get("sub_category") if m.get("sub_category") != "" and m.get("sub_category") is not None else "unspecified"
        unit: (str | None) = m.get("unit") if m.get("unit") != "" else None
        location: (str | None) = m.get("location") if m.get("location") != "" else None
        
        all_fields: list[tuple, float] = (
            [((name, top_category, sub_category, unit, location), float(1))] +
            [((SBERT_name, top_category, sub_category, unit, location), SBERT_multiplier) for SBERT_name, SBERT_multiplier in SBERTs.items()] +
            [((CAS, top_category, sub_category, unit, location), float(1))]
            )
        
        no_units: list[tuple, float] = ([((name, top_category, sub_category, location), float(1))] +
                                        [((SBERT_name, top_category, sub_category, location), SBERT_multiplier) for SBERT_name, SBERT_multiplier in SBERTs.items()] +
                                        [((CAS, top_category, sub_category, location), float(1))]
                                        )
        
        for ID, multiplier in all_fields + no_units:
            
            if any([True if n is None else False for n in ID]):
                continue
            
            biosphere_mapping[prep(ID)]: dict = {**m, **{"multiplier": multiplier}}

    
    # Initialize a list to store migration data --> tuple of FROM and TO dicts
    successful_migration_data: dict[tuple[str, str, str, str, str], tuple[dict, dict]] = {}
    unsuccessful_migration_data: dict[tuple[str, str, str, str, str], tuple[dict, None]] = {}
    SBERT_to_map: list = []
    
    # Loop through each flow
    # Check if it should be mapped
    # Map if possible
    for exc in biosphere_flows_1:
            
        exc_name: (str | None) = None if exc.get("name") == "" else exc.get("name")
        exc_SBERTs: dict = SBERT_mapping.get(exc_name) if SBERT_mapping.get(exc_name) is not None else {}
        exc_CAS: (str | None) = exc.get("CAS number") if exc.get("CAS number") != "" else None
        exc_top_category: (str | None) = exc.get("top_category") if exc.get("top_category") != "" else None
        exc_sub_category: str = exc.get("sub_category") if exc.get("sub_category") != "" and exc.get("sub_category") is not None else "unspecified"
        exc_unit: (str | None) = None if exc.get("unit") == "" else exc.get("unit")
        exc_location: (str | None) = None if exc.get("location") == "" else exc.get("location")
        # found: None = None
        
        if (exc_name, exc_top_category, exc_sub_category, exc_unit, exc_location) in successful_migration_data:
            continue
        
        
        all_fields: list[tuple, float] = (
            [((exc_name, exc_top_category, exc_sub_category, exc_unit, exc_location), float(1))] +
            [((SBERT_name, exc_top_category, exc_sub_category, exc_unit, exc_location), SBERT_multiplier) for SBERT_name, SBERT_multiplier in exc_SBERTs.items()] +
            [((exc_CAS, exc_top_category, exc_sub_category, exc_unit, exc_location), float(1))]
            )
        
        empty_subcat: list[tuple, float] = (
            [((exc_name, exc_top_category, "unspecified", exc_unit, exc_location), float(1))] +
            [((SBERT_name, exc_top_category, "unspecified", exc_unit, exc_location), SBERT_multiplier) for SBERT_name, SBERT_multiplier in exc_SBERTs.items()] +
            [((exc_CAS, exc_top_category, "unspecified", exc_unit, exc_location), float(1))]
            )
        
        no_units: list[tuple, float] = (
            [((exc_name, exc_top_category, exc_sub_category, exc_location), float(1))] +
            [((SBERT_name, exc_top_category, exc_sub_category, exc_location), SBERT_multiplier) for SBERT_name, SBERT_multiplier in exc_SBERTs.items()] +
            [((exc_CAS, exc_top_category, exc_sub_category, exc_location), float(1))]
            )
        
        empty_subcat_and_no_units: list[tuple, float] = (
            [((exc_name, exc_top_category, "unspecified", exc_location), float(1))] +
            [((SBERT_name, exc_top_category, "unspecified", exc_location), SBERT_multiplier) for SBERT_name, SBERT_multiplier in exc_SBERTs.items()] +
            [((exc_CAS, exc_top_category, "unspecified", exc_location), float(1))]
            )

        
        for ID, multiplier in all_fields + empty_subcat + no_units + empty_subcat_and_no_units:
            
            if any([True if n is None else False for n in ID]):
                continue
            
            found: (dict | None) = biosphere_mapping.get(prep(ID))
            
            if found is not None:
                break
        
        
        
        if found is not None:
            successful_migration_data[(exc_name, exc_top_category, exc_sub_category, exc_unit, exc_location)]: tuple[dict, dict] = (exc, found)
        else:
            unsuccessful_migration_data[(exc_name, exc_top_category, exc_sub_category, exc_unit, exc_location)]: tuple[dict, None] = (exc, None)
            
            # SBERT_to_map += [{"orig": exc_name, "mapped": o} for o in SBERT_mapping_dict_3.get(exc_name, [])]
            SBERT_to_map += [{"orig": exc_name,
                              "mapped": o,
                              "score": ooo} for o, ooo in SBERT_mapping_dict_3.get(exc_name, {}).items()]
    
    
    # Construct a custom unit mapping
    unit_mapping: dict = {"cubic meter": {"kilogram": 1000},
                          "litre": {"cubic meter": 0.001},
                          "square meter": {"hectare": 1/10000},
                          "standard cubic meter": {"cubic meter": 1, "kilogram": 1000},
                          "megajoule": {"kilowatt hour": 1/3.6},
                          "kilowatt hour": {"megajoule": 3.6}
                          }
    
    # Specify the fields to which should be mapped to
    TO_fields = ("code",
                 "name",
                 "SimaPro_name",
                 "categories",
                 "top_category",
                 "sub_category",
                 "SimaPro_categories",
                 "unit",
                 "SimaPro_unit",
                 "location")
    
    # Initialize empty migration dictionary
    successful_migration_dict: dict = {"fields": ("code",),
                                       "data": []}
    
    # Write all items which have been successfully mapped to the data field in the migration dictionary
    for FROM, TO in successful_migration_data.values():
        
        if TO is None:
            continue
        
        if FROM["unit"] == TO["unit"]:
            unit_multiplier: float = float(1)
        
        else:
            unit_multiplier: (float | None) = unit_mapping.get(FROM["unit"], {}).get(TO["unit"])
            
            if unit_multiplier is None:
                print(FROM["name"], " -> ", TO["name"])
                print(FROM["location"], " -> ", TO["location"])
                print(FROM["unit"], " -> ", TO["unit"])
                print()
                raise ValueError("Multiplier could not be retrieved, FROM_unit = '{}', TO_unit = '{}'".format(FROM["unit"], TO["unit"]))
        
        # Retrieve any additional multiplier and multiply this one with the unit multiplier
        multiplier: float = TO.get("multiplier", float(1)) * unit_multiplier
        
        # Field one always specifies to which item the mapping should be applied.
        one: list[str] = [FROM[n] for n in successful_migration_dict["fields"]]
        
        # Field two always specifies the new data that should be applied to update the original data
        two: dict = dict(**{str(o): str(TO[o]) for o in TO_fields}, **{"multiplier": multiplier})
        
        # Add to migration dictionary
        successful_migration_dict["data"] += [[one + ["biosphere"], two]]
    
    # Add 'type' key
    successful_migration_dict["fields"] += ("type",)
    
    # Statistic message for console
    statistic_msg: str = """    
A total of {} unique biosphere flows were detected that need to be linked:
 - {} unique biosphere flows were successfully linked
 - {} unique biosphere flows remain unlinked
    """.format(len(successful_migration_data) + len(unsuccessful_migration_data), len(successful_migration_data), len(unsuccessful_migration_data))
    print(statistic_msg)
    
    # Return
    return {"biosphere_migration": successful_migration_dict,
            "successfully_migrated_biosphere_flows": [m for m in list(successful_migration_data.values())],
            "unsuccessfully_migrated_biosphere_flows": [m for m in list(unsuccessful_migration_data.values())],
            "SBERT_to_map": SBERT_to_map}



#%% Check which flows are not used in LCIA methods from ecoinvent XML

# We create a list for all flows that are not used in any of the LCIA methods from ecoinvent XML
def elementary_flows_that_are_not_used_in_XML_methods(elementary_flows: list,
                                                      method_df: pd.DataFrame) -> dict:
    
    # Check function input type
    check_function_input_type(elementary_flows_that_are_not_used_in_XML_methods, locals())
    
    method_data: list[dict] = method_df.fillna("").to_dict("records")
    methods: dict = {}
    
    for flow in method_data:
        method_name: tuple = (flow["Method"], flow["Category"], flow["Indicator"])
        name: str = prep((flow["Name"],))[0]
        categories: tuple = prep((flow["Compartment"], flow["Subcompartment"]))
        
        if (name, categories) not in methods:
            methods[(name, categories)]: str = ""
            
        methods[(name, categories)] += "\n - " + str(method_name)
    
    not_used: dict = {(m["database"], m["code"]): m for m in elementary_flows if methods.get((prep((m["name"],))[0], prep(m["categories"]))) is None}
    return not_used



#%% Function to link activities between two activity databases

def create_harmonized_activity_migration(flows_1: list,
                                         flows_2: list,
                                         manually_checked_SBERTs: (list | pd.DataFrame | None),
                                         ecoinvent_correspondence_mapping: (list | pd.DataFrame | None)) -> dict:
    
    # Check function input type
    check_function_input_type(create_harmonized_activity_migration, locals())
    
    # Map using SBERT
    df_SBERT_mapping: pd.DataFrame = map_using_SBERT(tuple(set([(n["name"], n["location"]) for n in flows_1])), tuple(set([(m["name"], m["location"]) for m in flows_2])), 5)    

    # Initialize empty dictionaries
    SBERT_mapping_dict_1: dict = {}
    SBERT_mapping_dict_5: dict = {}
    
    for idx, row in df_SBERT_mapping.iterrows():
        
        orig_name: str = row["orig"][0]
        orig_location: str = row["orig"][1]
        mapped_name: str = row["mapped"][0]
        mapped_location: str = row["mapped"][1]
        
        if row["ranking"] == 5 and row["score"] >= 0.95:
            
            if (orig_name, orig_location) not in SBERT_mapping_dict_1:
                SBERT_mapping_dict_1[(orig_name, orig_location)]: dict = {}
            
            if (mapped_name, mapped_location) not in SBERT_mapping_dict_1:
                SBERT_mapping_dict_1[(mapped_name, mapped_location)]: dict = {}
            
            SBERT_mapping_dict_1[(orig_name, orig_location)][(mapped_name, mapped_location)]: float = float(1)
            SBERT_mapping_dict_1[(mapped_name, mapped_location)][(orig_name, orig_location)]: float = float(1)
            
        if (orig_name, orig_location) not in SBERT_mapping_dict_5:
            SBERT_mapping_dict_5[(orig_name, orig_location)]: dict = {}
            
        SBERT_mapping_dict_5[(orig_name, orig_location)][(mapped_name, mapped_location)]: float = float(row["score"])
    
    
    if isinstance(manually_checked_SBERTs, pd.DataFrame):
        manually_checked_SBERTs: list[dict] = manually_checked_SBERTs.replace({float("NaN"): None}).to_dict("records")
    
    elif manually_checked_SBERTs is None:
        manually_checked_SBERTs: list = []
    
    SBERT_mapping: dict = SBERT_mapping_dict_1
    for m in manually_checked_SBERTs:
        
        if m.get("orig_name") is not None and m.get("orig_location") is not None and m.get("mapped_name") is not None and m.get("mapped_location") is not None and m.get("multiplier") is not None:
            
            if (m["orig_name"], m["orig_location"]) not in SBERT_mapping:
                SBERT_mapping[(m["orig_name"], m["orig_location"])]: dict = {}
                
            SBERT_mapping[(m["orig_name"], m["orig_location"])][(m["mapped_name"], m["mapped_location"])]: float = float(m["multiplier"])
            
            if m.get("multiplier") != 0:
                
                if (m["mapped_name"], m["mapped_location"]) not in SBERT_mapping:
                    SBERT_mapping[(m["mapped_name"], m["mapped_location"])]: dict = {}
                
                SBERT_mapping[(m["mapped_name"], m["mapped_location"])][(m["orig_name"], m["orig_location"])]: float = float(1 / m["multiplier"])
    


    # # Initialize a dictionary to store elements to which can be mapped to
    activity_mapping: dict = {}
    
    # Loop through each of the activity that can be mapped to
    for act in flows_2:
        
        SimaPro_name: (str | None) = None if act.get("SimaPro_name") == "" else act.get("SimaPro_name")
        name: (str | None) = None if act.get("name") == "" else act.get("name")     
        unit: (str | None) = None if act.get("unit") == "" else act.get("unit")
        location: (str | None) = None if act.get("location") == "" else act.get("location")
        SBERTs: dict = SBERT_mapping.get((name, location)) if SBERT_mapping.get((name, location)) is not None else {}
        activity_name: (str | None) = None if act.get("activity_name") == "" else act.get("activity_name")
        activity_code: (str | None) = None if act.get("activity_code") == "" else act.get("activity_code")
        reference_product_name: (str | None) = None if act.get("reference_product_name") == "" else act.get("reference_product_name")
        reference_product_code: (str | None) = None if act.get("reference_product_code") == "" else act.get("reference_product_code")
        created_SimaPro_name: str = "{} {{{}}}|{}".format(reference_product_name, location, activity_name) 
        
        SimaPro_fields: list[tuple, float] = (
            [((SimaPro_name, unit), float(1))] +
            [((created_SimaPro_name, unit), float(1))] +
            [((name, location, unit), float(1))] +
            [((SBERT_name, SBERT_location, unit), SBERT_multiplier) for (SBERT_name, SBERT_location), SBERT_multiplier in SBERTs.items()]
            )
        
        SimaPro_fields_and_no_units: list[tuple, float] = (
            [((SimaPro_name,), float(1))] +
            [((created_SimaPro_name,), float(1))] +
            [((name, location), float(1))] +
            [((SBERT_name, SBERT_location), SBERT_multiplier) for (SBERT_name, SBERT_location), SBERT_multiplier in SBERTs.items()]
            )
        
        XML_fields: list[tuple, float] = (
            [((activity_code, reference_product_code), float(1))] +
            [((activity_name, reference_product_name, location, unit), float(1))] +
            [((reference_product_name + " " + activity_name, location, unit), float(1))] +
            [((activity_name + " " + reference_product_name, location, unit), float(1))]
            )
        
        XML_fields_and_no_units: list[tuple, float] = (
            [((activity_name, reference_product_name, location), float(1))] +
            [((reference_product_name + " " + activity_name, location), float(1))] +
            [((activity_name + " " + reference_product_name, location), float(1))]
            )
        
        
        for ID, multiplier in SimaPro_fields + XML_fields + SimaPro_fields_and_no_units + XML_fields_and_no_units:
            
            if any([True if n is None else False for n in ID]):
                continue
            
            activity_mapping[prep(ID)]: dict = {**act, **{"multiplier": multiplier}}
    
    
    
    if isinstance(ecoinvent_correspondence_mapping, pd.DataFrame):
        correspondence_mapping: list[dict] = ecoinvent_correspondence_mapping.replace({float("NaN"): None}).to_dict("records")
    
    elif ecoinvent_correspondence_mapping is None:
        correspondence_mapping: list = []
        
    else:
        correspondence_mapping: list = ecoinvent_correspondence_mapping
    
    
    # Loop through each item from the correspondence files
    for m in correspondence_mapping:
        
        FROM_unit: str = m["FROM_unit"]
        FROM_location: str = m["FROM_location"]
        FROM_activity_name: str = m["FROM_activity"]
        FROM_activity_code: str = m["FROM_activity_UUID"]
        FROM_reference_product_name: str = m["FROM_reference_product"]
        FROM_reference_product_code: str = m["FROM_product_UUID"]
        
        TO_1: (dict | None) = activity_mapping.get(prep((FROM_activity_code, FROM_reference_product_code)))
        TO_2: (dict | None) = activity_mapping.get(prep((FROM_activity_name, FROM_reference_product_name, FROM_location, FROM_unit)))
        TO: (dict | None) = TO_1 if TO_1 is not None else (TO_2 if TO_2 is not None else None)
        
        if TO is None:
            continue
        
        if FROM_activity_code is not None and FROM_reference_product_code is not None:
            activity_mapping[(FROM_activity_code, FROM_reference_product_code)]: dict = TO
            
        if FROM_activity_name is not None and FROM_reference_product_name is not None and FROM_unit is not None and FROM_location is not None:
            activity_mapping[prep((FROM_activity_name, FROM_reference_product_name, FROM_location, FROM_unit))]: dict = TO
    
    
    # Initialize a list to store migration data --> tuple of FROM and TO dicts
    successful_migration_data: dict[tuple[str, str, str, str, str], tuple[dict, dict]] = {}
    unsuccessful_migration_data: dict[tuple[str, str, str, str, str], tuple[dict, None]] = {}
    SBERT_to_map: list = []
    
    # Loop through each flow
    # Check if it should be mapped
    # Map if possible
    for exc in flows_1:
            
        exc_SimaPro_name: (str | None) = None if exc.get("SimaPro_name") == "" else exc.get("SimaPro_name")
        exc_name: (str | None) = None if exc.get("name") == "" else exc.get("name")
        exc_unit: (str | None) = None if exc.get("unit") == "" else exc.get("unit")
        exc_SBERTs: dict = SBERT_mapping.get((exc_name, exc_unit)) if SBERT_mapping.get((exc_name, exc_unit)) is not None else {}
        exc_location: (str | None) = None if exc.get("location") == "" else exc.get("location")
        exc_activity_name: (str | None) = None if exc.get("activity_name") == "" else exc.get("activity_name")
        exc_activity_code: (str | None) = None if exc.get("activity_code") == "" else exc.get("activity_code")
        exc_reference_product_name: (str | None) = None if exc.get("reference_product_name") == "" else exc.get("reference_product_name")
        exc_reference_product_code: (str | None) = None if exc.get("reference_product_code") == "" else exc.get("reference_product_code")
        exc_created_SimaPro_name: str = "{} {{{}}}|{}".format(exc_reference_product_name, exc_location, exc_activity_name)         
        
        if (exc_name, exc_location, exc_unit) in successful_migration_data:
            continue
        
        
        SimaPro_fields: list[tuple, float] = (
            [((exc_SimaPro_name, exc_unit), float(1))] +
            [((exc_created_SimaPro_name, exc_unit), float(1))] +
            [((exc_name, exc_location, exc_unit), float(1))] +
            [((SBERT_name, SBERT_location, exc_unit), SBERT_multiplier) for (SBERT_name, SBERT_location), SBERT_multiplier in exc_SBERTs.items()]
            )
        
        SimaPro_fields_and_no_units: list[tuple, float] = (
            [((exc_SimaPro_name,), float(1))] +
            [((exc_created_SimaPro_name,), float(1))] +
            [((exc_name, exc_location), float(1))] +
            [((SBERT_name, SBERT_location), SBERT_multiplier) for (SBERT_name, SBERT_location), SBERT_multiplier in exc_SBERTs.items()]
            )
        
        XML_fields: list[tuple, float] = (
            [((exc_activity_code, exc_reference_product_code), float(1))] +
            [((exc_activity_name, exc_reference_product_name, exc_location, exc_unit), float(1))] +
            [((exc_reference_product_name + " " + exc_activity_name, exc_location, exc_unit), float(1))] +
            [((exc_activity_name + " " + exc_reference_product_name, exc_location, exc_unit), float(1))]
            )
        
        XML_fields_and_no_units: list[tuple, float] = (
            [((exc_activity_name, exc_reference_product_name, exc_location), float(1))] +
            [((exc_reference_product_name + " " + exc_activity_name, exc_location), float(1))] +
            [((exc_activity_name + " " + exc_reference_product_name, exc_location), float(1))]
            )
            
        for ID, multiplier in SimaPro_fields + XML_fields + SimaPro_fields_and_no_units + XML_fields_and_no_units:
            
            if any([True if n is None else False for n in ID]):
                continue
            
            found: (dict | None) = activity_mapping.get(prep(ID))

            if found is not None:
                break
        
        
        if found is not None:
            successful_migration_data[(exc_name, exc_location, exc_unit)]: tuple[dict, dict] = (exc, found)
        else:
            unsuccessful_migration_data[(exc_name, exc_location, exc_unit)]: tuple[dict, None] = (exc, None)
            
            SBERT_to_map += [{"orig_name": exc_name,
                              "orig_location": exc_location,
                              "mapped_name": o,
                              "mapped_location": oo,
                              "score": ooo} for (o, oo), ooo in SBERT_mapping_dict_5.get((exc_name, exc_location), {}).items()]
    
    
    # Construct a custom unit mapping
    unit_mapping: dict = {"cubic meter": {"kilogram": 1000},
                          "litre": {"cubic meter": 0.001},
                          "square meter": {"hectare": 1/10000},
                          "standard cubic meter": {"cubic meter": 1},
                          "megajoule": {"kilowatt hour": 1/3.6},
                          "kilowatt hour": {"megajoule": 3.6}
                          }
    
    # Specify the fields to which should be mapped to
    TO_fields = ("code",
                  "name",
                  "SimaPro_name",
                  "categories",
                  # "top_category",
                  # "sub_category",
                  "SimaPro_categories",
                  "unit",
                  "SimaPro_unit",
                  "location")
    
    # Initialize empty migration dictionary
    successful_migration_dict: dict = {"fields": ("name", "location", "unit"),
                                        "data": []}
    
    # Write all items which have been successfully mapped to the data field in the migration dictionary
    for FROM, TO in successful_migration_data.values():
        
        if TO is None:
            continue
        
        if FROM["unit"] == TO["unit"]:
            unit_multiplier: float = float(1)
        
        else:
            unit_multiplier: (float | None) = unit_mapping.get(FROM["unit"], {}).get(TO["unit"])
            
            if unit_multiplier is None:
                print(FROM["name"], " -> ", TO["name"])
                print(FROM["location"], " -> ", TO["location"])
                print(FROM["unit"], " -> ", TO["unit"])
                print()
                raise ValueError("Multiplier could not be retrieved, FROM_unit = '{}', TO_unit = '{}'".format(FROM["unit"], TO["unit"]))
        
        # Retrieve any additional multiplier and multiply this one with the unit multiplier
        multiplier: float = TO.get("multiplier", float(1)) * unit_multiplier
        
        # Field one always specifies to which item the mapping should be applied.
        one: list[str] = [FROM[n] for n in successful_migration_dict["fields"]]
        
        # Field two always specifies the new data that should be applied to update the original data
        two: dict = dict(**{str(o): str(TO[o]) for o in TO_fields}, **{"multiplier": multiplier})
        
        # Add to migration dictionary
        successful_migration_dict["data"] += [[one + ["technosphere"], two]]
        successful_migration_dict["data"] += [[one + ["substitution"], two]]
    
    # Add 'type' key
    successful_migration_dict["fields"] += ("type",)
    
    # Statistic message for console
    statistic_msg: str = """    
A total of {} unique activity flows were detected that need to be linked:
 - {} unique activity flows were successfully linked
 - {} unique activity flows remain unlinked
    """.format(len(successful_migration_data) + len(unsuccessful_migration_data), len(successful_migration_data), len(unsuccessful_migration_data))
    print(statistic_msg)
    
    # Return
    return {"activity_migration": successful_migration_dict,
            "successfully_migrated_activity_flows": [m for m in list(successful_migration_data.values())],
            "unsuccessfully_migrated_activity_flows": [m for m in list(unsuccessful_migration_data.values())],
            "SBERT_to_map": SBERT_to_map}


        

