import pathlib

if __name__ == "__main__":
    import os
    os.chdir(pathlib.Path(__file__).parent)

import pandas as pd
from utils import (map_using_SBERT)
from helper import (check_function_input_type)
from lci import (create_structured_migration_dictionary_from_excel)

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


def create_activity_IDs(activity_code: (str | None),
                        reference_product_code: (str | None),
                        SimaPro_name: (str | None),
                        activity_name: (str | None),
                        reference_product_name: (str | None),
                        location: (str | None),
                        unit: (str | None),
                        models: list,
                        systems: list,
                        ) -> list[tuple[(str | None)]]:
    
    
    if not isinstance(activity_code, str):
        activity_code = None
        
    if not isinstance(reference_product_code, str):
        reference_product_code = None
        
    if not isinstance(SimaPro_name, str):
        SimaPro_name = None
        
    if not isinstance(activity_name, str):
        activity_name = None
        
    if not isinstance(reference_product_name, str):
        reference_product_name = None
    
    if not isinstance(location, str):
        location = None
        
    if not isinstance(unit, str):
        unit = None
    
    # IDs are tuples of format
    # ("activity_code", "reference_product_code", "SimaPro_name", "activity_name", "reference_product_name", "location", "unit")
    IDs: list = []
     
    if SimaPro_name is not None and unit is not None:
        IDs += [(None, None, SimaPro_name, None, None, None, unit)]
    
    if reference_product_name is not None and activity_name is not None and location is not None and unit is not None and models != [] and systems != []:
        
        for model in models:
            
            if not isinstance(model, str):
                continue
            
            for system in systems:
                
                if not isinstance(system, str):
                    continue
            
                created_SimaPro_name: (str | None) = "{} {{{}}}|{} | {}, {}".format(reference_product_name, location, activity_name, model, system)
                IDs += [(None, None, created_SimaPro_name, None, None, None, unit)]
    
    if activity_name is None and reference_product_name is not None and location is not None and unit is not None:
        IDs += [(None, None, None, None, reference_product_name, location, unit)]
    
    if activity_name is not None and reference_product_name is not None and location is not None and unit is not None:
        IDs += [(None, None, None, activity_name, reference_product_name, location, unit)]
        IDs += [(None, None, None, None, reference_product_name + " " + activity_name, location, unit)]
        IDs += [(None, None, None, None, activity_name + " " + reference_product_name, location, unit)]
    
    if activity_code is not None and reference_product_code is not None:
        IDs += [(activity_code, reference_product_code, None, None, None, None, None)]

    return list(set(IDs))


def find_activity(IDs: list[tuple], multiplier: (float | int), mapping: dict) -> (list[tuple] | None):
    
    found: list[tuple] = [mapping[ID] for ID in IDs if mapping.get(ID) is not None]
    
    if len(found) > 0:
        return [(n[0], n[1] * multiplier) for n in found[0]]
    
    else:
        return None
        


def create_harmonized_activity_migration(flows_1: list,
                                         flows_2: list,
                                         df_custom_mapping: (pd.DataFrame | None),
                                         df_ecoinvent_correspondence_mapping: (pd.DataFrame | None),
                                         use_SBERT_for_mapping: bool) -> dict:
    
    # Check function input type
    check_function_input_type(create_harmonized_activity_migration, locals())
    
    models: list[str] = ["Cut-off", "Cut-Off", "cutoff"]
    systems: list[str] = ["U", "Unit", "S", "System"]
    
    direct_mapping: dict = {}
    custom_mapping: dict = {}
    correspondence_mapping: dict = {}
    SBERT_mapping: dict = {}
    
    # Loop through each of the activity that can be mapped to
    for act in flows_2:
        
        FROM_IDs: list[tuple] = create_activity_IDs(activity_code = act.get("activity_code"),
                                                    reference_product_code = act.get("reference_product_code"),
                                                    SimaPro_name = act.get("SimaPro_name"),
                                                    activity_name = act.get("activity_name"),
                                                    reference_product_name = act.get("reference_product_name"),
                                                    location = act.get("location"),
                                                    unit = act.get("unit"),
                                                    models = models,
                                                    systems = systems)
        
        for FROM_ID in FROM_IDs:
            if direct_mapping.get(FROM_ID) is None:
                direct_mapping[FROM_ID] = [(act, 1)]
        
    
    if isinstance(df_ecoinvent_correspondence_mapping, pd.DataFrame):
        
        for idx, row in df_ecoinvent_correspondence_mapping.iterrows():
            TO_IDs: list[tuple] = create_activity_IDs(activity_code = row["TO_activity_uuid"],
                                                      reference_product_code = row["TO_reference_product_uuid"],
                                                      SimaPro_name = None,
                                                      activity_name = row["TO_activity_name"],
                                                      reference_product_name = row["TO_reference_product_name"],
                                                      location = row["TO_location"],
                                                      unit = row["TO_unit"],
                                                      models = models,
                                                      systems = systems)
            
            found: (list[tuple] | None) = find_activity(IDs = TO_IDs, multiplier = row["Multiplier"], mapping = direct_mapping)
            
            if found is not None:
                FROM_IDs: list[tuple] = create_activity_IDs(activity_code = row["FROM_activity_uuid"],
                                                            reference_product_code = row["FROM_reference_product_uuid"],
                                                            SimaPro_name = None,
                                                            activity_name = row["FROM_activity_name"],
                                                            reference_product_name = row["FROM_reference_product_name"],
                                                            location = row["FROM_location"],
                                                            unit = row["FROM_unit"],
                                                            models = models,
                                                            systems = systems)
            
                for FROM_ID in FROM_IDs:
                    if correspondence_mapping.get(FROM_ID) is None:
                        correspondence_mapping[FROM_ID] = [found]
                    
                    else:
                        correspondence_mapping[FROM_ID] += [found]
                        
    
    if isinstance(df_custom_mapping, pd.DataFrame):
        
        for idx, row in df_custom_mapping.iterrows():
            TO_IDs: list[tuple] = create_activity_IDs(activity_code = row.get("TO_activity_uuid"),
                                                      reference_product_code = row.get("TO_reference_product_uuid"),
                                                      SimaPro_name = row.get("TO_SimaPro_name"),
                                                      activity_name = row.get("TO_activity_name"),
                                                      reference_product_name = row.get("TO_reference_product_name"),
                                                      location = row.get("TO_location"),
                                                      unit = row.get("TO_unit"),
                                                      models = models,
                                                      systems = systems)
            
            found: (list[tuple] | None) = find_activity(IDs = TO_IDs, multiplier = row.get("multiplier"), mapping = direct_mapping)
            
            if found is not None:
                FROM_IDs: list[tuple] = create_activity_IDs(activity_code = row.get("FROM_activity_uuid"),
                                                            reference_product_code = row.get("FROM_reference_product_uuid"),
                                                            SimaPro_name = row.get("FROM_SimaPro_name"),
                                                            activity_name = row.get("FROM_activity_name"),
                                                            reference_product_name = row.get("FROM_reference_product_name"),
                                                            location = row.get("FROM_location"),
                                                            unit = row.get("FROM_unit"),
                                                            models = models,
                                                            systems = systems)
            
                for FROM_ID in FROM_IDs:
                    if custom_mapping.get(FROM_ID) is None:
                        custom_mapping[FROM_ID] = [found]
                    
                    else:
                        custom_mapping[FROM_ID] += [found]

    
    if use_SBERT_for_mapping:
        
        best_n_SBERT: int = 5
        SBERT_cutoff_for_inclusion: float = 0.95
        keys_to_use_for_SBERT_mapping: tuple[str] = ("name", "location", "unit")
        
        FROM_SBERTs: tuple = tuple(set([tuple([m.get(n) for n in keys_to_use_for_SBERT_mapping]) for m in flows_1]))
        TO_SBERTs: tuple = tuple(set([tuple([m.get(n) for n in keys_to_use_for_SBERT_mapping]) for m in flows_2]))
        
        FROM_SBERTs_cleaned, TO_SBERTs_cleaned = [(a, b) for a, b in zip(FROM_SBERTs, TO_SBERTs) if all([m is not None for m in a]) and all([n is not None for n in b])]
    
        # Map using SBERT
        df_SBERT_mapping: pd.DataFrame = map_using_SBERT(FROM_SBERTs_cleaned, TO_SBERTs_cleaned, best_n_SBERT)    
        
        for idx, row in df_SBERT_mapping.iterrows():
            
            if row["ranking"] != best_n_SBERT and row["score"] < SBERT_cutoff_for_inclusion:
                continue
            
            TO_IDs: list[tuple] = create_activity_IDs(activity_code = None,
                                                      reference_product_code = None,
                                                      SimaPro_name = None,
                                                      activity_name = None,
                                                      reference_product_name = row["mapped"][0],
                                                      location = row["mapped"][1],
                                                      unit = row["mapped"][2],
                                                      models = models,
                                                      systems = systems)
            
            found: (list[tuple] | None) = find_activity(IDs = TO_IDs, multiplier = 1, mapping = direct_mapping)
            
            if found is not None:
                
                FROM_IDs: list[tuple] = create_activity_IDs(activity_code = None,
                                                            reference_product_code = None,
                                                            SimaPro_name = None,
                                                            activity_name = None,
                                                            reference_product_name = row["orig"][0],
                                                            location = row["orig"][1],
                                                            unit = row["orig"][2],
                                                            models = models,
                                                            systems = systems)
                
                for FROM_ID in FROM_IDs:
                    if SBERT_mapping.get(FROM_ID) is None:
                        SBERT_mapping[FROM_ID] = [found]
    
    
    # Initialize a list to store migration data --> tuple of FROM and TO dicts
    successful_migration_data: list = []
    unsuccessful_migration_data: list = []
    
    # Loop through each flow
    # Check if it should be mapped
    # Map if possible
    for exc in flows_1:
            
        exc_SimaPro_name = exc.get("SimaPro_name")
        exc_name = exc.get("name")
        exc_unit = exc.get("unit")
        exc_location = exc.get("location")
        exc_activity_name = exc.get("activity_name")
        exc_activity_code = exc.get("activity_code")
        exc_reference_product_name = exc.get("reference_product_name")
        exc_reference_product_code = exc.get("reference_product_code")
        
        if (exc_name, exc_location, exc_unit) in successful_migration_data:
            continue
        
        FROM_IDs: list[tuple] = create_activity_IDs(activity_code = exc_activity_code,
                                                    reference_product_code = exc_reference_product_code,
                                                    SimaPro_name = exc_SimaPro_name,
                                                    activity_name = exc_activity_name,
                                                    reference_product_name = exc_reference_product_name,
                                                    location = exc_location,
                                                    unit = exc_unit,
                                                    models = models,
                                                    systems = systems)
        
        found_from_direct_mapping = find_activity(IDs = FROM_IDs, multiplier = 1, mapping = direct_mapping)
        found_from_custom_mapping = find_activity(IDs = FROM_IDs, multiplier = 1, mapping = custom_mapping)
        found_from_correspondence_mapping = find_activity(IDs = FROM_IDs, multiplier = 1, mapping = correspondence_mapping)
        found_from_SBERT_mapping = find_activity(IDs = FROM_IDs, multiplier = 1, mapping = SBERT_mapping)
        
        if found_from_direct_mapping is not None:
            successful_migration_data += [(exc, found_from_direct_mapping, "Direct mapping")]
            
        elif found_from_custom_mapping is not None:
            successful_migration_data += [(exc, found_from_custom_mapping, "Custom mapping")]

        elif found_from_correspondence_mapping is not None:
            successful_migration_data += [(exc, found_from_correspondence_mapping, "Correspondence mapping")]
            
        elif found_from_SBERT_mapping is not None:
            successful_migration_data += [(exc, found_from_SBERT_mapping, "SBERT mapping")]
        
        else:
            unsuccessful_migration_data += [(exc, [], "Remains unmapped")]

    # Specify the field from which we want to map
    FROM_fields: tuple = ("name", "unit", "location")
        
    # Specify the fields to which should be mapped to
    TO_fields: tuple = ("code",
                        "name",
                        "SimaPro_name",
                        "categories",
                        # "top_category",
                        # "sub_category",
                        "SimaPro_categories",
                        "unit",
                        "SimaPro_unit",
                        "location"
                        )
    
    # Initialize empty migration dictionary
    successful_migration_dict: dict = {"fields": FROM_fields,
                                        "data": []}
    successful_migration_df: list = []
    unsuccessful_migration_df = [{**{"FROM_" + k: v for k, v in m.items()}, "Used_Mapping": used_mapping} for m, _, used_mapping in unsuccessful_migration_data]
    
    for FROM, TOs, used_mapping in successful_migration_data:
        
        FROM_tuple: tuple = tuple([FROM[m] for m in FROM_fields])
        TO_list_of_dicts: list[dict] = []
        
        for TO, multiplier in TOs:
            TO_dict: dict = {k: v for k, v in TO.items() if k in TO_fields}
            TO_list_of_dicts += [{**TO_dict, "multiplier": multiplier}]
        
        successful_migration_dict["data"] += [{FROM_tuple + ("technosphere",): TO_list_of_dicts}]
        # successful_migration_dict["data"] += [{FROM_tuple + ("substitution",): TO_list_of_dicts}]

        FROM_dict_for_df: dict = {"FROM_" + m: FROM[m] for m in FROM_fields}
        TO_dicts_for_df: dict = {**{("TO_" + m if m != "multiplier" else m): FROM[m] for m in TO_list_of_dicts}, "Used_Mapping": used_mapping}
        successful_migration_df += [{**FROM_dict_for_df, **m} for m in TO_dicts_for_df]
        
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
            "successfully_migrated_activity_flows": pd.DataFrame(successful_migration_df),
            "unsuccessfully_migrated_activity_flows": pd.DataFrame(unsuccessful_migration_df)
            }

    # # Construct a custom unit mapping
    # unit_mapping: dict = {"cubic meter": {"kilogram": 1000},
    #                       "litre": {"cubic meter": 0.001},
    #                       "square meter": {"hectare": 1/10000},
    #                       "standard cubic meter": {"cubic meter": 1},
    #                       "megajoule": {"kilowatt hour": 1/3.6},
    #                       "kilowatt hour": {"megajoule": 3.6}
    #                       }
    
    
    
#     # Initialize empty migration dictionary
#     successful_migration_dict: dict = {"fields": ("name", "location", "unit"),
#                                         "data": []}
    
#     # Write all items which have been successfully mapped to the data field in the migration dictionary
#     for FROM, TO in successful_migration_data.values():
        
#         if TO is None:
#             continue
        
#         if FROM["unit"] == TO["unit"]:
#             unit_multiplier: float = float(1)
        
#         else:
#             unit_multiplier: (float | None) = unit_mapping.get(FROM["unit"], {}).get(TO["unit"])
            
#             if unit_multiplier is None:
#                 print(FROM["name"], " -> ", TO["name"])
#                 print(FROM["location"], " -> ", TO["location"])
#                 print(FROM["unit"], " -> ", TO["unit"])
#                 print()
#                 raise ValueError("Multiplier could not be retrieved, FROM_unit = '{}', TO_unit = '{}'".format(FROM["unit"], TO["unit"]))
        
#         # Retrieve any additional multiplier and multiply this one with the unit multiplier
#         multiplier: float = TO.get("multiplier", float(1)) * unit_multiplier
        
#         # Field one always specifies to which item the mapping should be applied.
#         one: list[str] = [FROM[n] for n in successful_migration_dict["fields"]]
        
#         # Field two always specifies the new data that should be applied to update the original data
#         two: dict = dict(**{str(o): str(TO[o]) for o in TO_fields}, **{"multiplier": multiplier})
        
#         # Add to migration dictionary
#         successful_migration_dict["data"] += [[one + ["technosphere"], two]]
#         successful_migration_dict["data"] += [[one + ["substitution"], two]]
    
#     # Add 'type' key
#     successful_migration_dict["fields"] += ("type",)
    
#     # Statistic message for console
#     statistic_msg: str = """    
# A total of {} unique activity flows were detected that need to be linked:
#  - {} unique activity flows were successfully linked
#  - {} unique activity flows remain unlinked
#     """.format(len(successful_migration_data) + len(unsuccessful_migration_data), len(successful_migration_data), len(unsuccessful_migration_data))
#     print(statistic_msg)
    
#     # Return
#     return {"activity_migration": successful_migration_dict,
#             "successfully_migrated_activity_flows": [m for m in list(successful_migration_data.values())],
#             "unsuccessfully_migrated_activity_flows": [m for m in list(unsuccessful_migration_data.values())],
#             "SBERT_to_map": SBERT_to_map}


        

