import pathlib

if __name__ == "__main__":
    import os
    os.chdir(pathlib.Path(__file__).parent)
    
import os
import re
import pathlib
import datetime
import bw2data
import pandas as pd
import brightway2 as bw
from bw2calc.errors import NonsquareTechnosphere, AllArraysEmpty, EmptyBiosphere
import helper as hp


#%% Define useful functions for LCA calculation

# Function to extract all characterization factors to dictionary in list format
def extract_CF(selected_methods: list):
    
    # Load the biosphere database
    bio = {}
    for bio_name in [m for m in bw.databases if bool(re.search("biosphere", m))]:
        bio_as_list = [m.as_dict() for m in bw.Database(bio_name)]
        bio |= {(m["database"], m["code"]): m for m in bio_as_list}
    
    # Load the methods metadata
    metadata = {k: v for k, v in bw.methods.deserialize().items()}
    
    # Define the column names
    data_description = ["Method", "Flow_code", "Flow_name", "Flow_CAS_number", "Flow_categories", "Flow_location", "Characterization_factor", "Characterization_unit"]
    
    # Loop through each flow and extract the information and write to column name
    data_orig = [(method_name,
             flow[1],
             bio[flow].get("name", "Key 'name' not available"),
             bio[flow].get("CAS number", "Key 'CAS number' not available"),
             bio[flow].get("categories", "Key 'categories' not available"),
             bio[flow].get("location", "Key 'location' not available"),
             cf,
             str(metadata[method_name].get("unit", "Biosphere key 'unit' not available") + " / " + bio[flow].get("unit", "Method key 'unit' not available"))) for method_name in selected_methods for flow, cf in bw.Method(method_name).load()]
    
    # Make a dictionary 
    data = {m: [n[idx] for n in data_orig] for idx, m in enumerate(data_description)}
    
    return data

# Function to export all processes contributing to a LCA
def top_processes_by_name(characterized_matrix, activity_dict):
    processes_I = characterized_matrix.transpose().sum(axis = 1)
    processes_II = pd.DataFrame(processes_I, columns = ["Score"]).assign(Input = activity_dict).query("Score != 0").to_dict("records")
    return processes_II    
    
# Function to export all emissions contributing to a LCA
def top_emissions_by_name(characterized_matrix, biosphere_dict):
    emissions_I = characterized_matrix.sum(axis = 1)
    emissions_II = pd.DataFrame(emissions_I, columns = ["Score"]).assign(Input = biosphere_dict).query("Score != 0").to_dict("records")
    return emissions_II

# Function to export all raw elementary flows of an inventory
def reduce_inventory_to_biosphere_emissions(characterized_inventory, biosphere_dict):
    array = characterized_inventory.sum(axis = 1)
    data = [{"Method": None, "Input": key, "Flow_amount": array[idx, 0]} for idx, key in enumerate(biosphere_dict) if array[idx, 0] != 0]
    return data

# Function to reduce the results of the emission and process contribution using a cut off value
def apply_cutoff(v1, Scores, cut_off):
    
    # Initialize list
    return_list = []
    
    # Loop through each inventory
    for idx_I, m in enumerate(v1):
        
        # Loop through each method
        for idx_II, n in enumerate(m):
            
            # Initialize a rest variable with the value 0
            rest = 0
            
            # Loop through each results value for the given method and activity
            for o in n:
                
                # Extract the overall score for this activity and method (= summed up result) from the calculation before
                sum_score = Scores[idx_I][idx_II]["Score"]
                
                # Check whether the current data point exceeds the cutoff
                if abs(o["Score"]) > abs(sum_score * cut_off):
                    
                    # If yes, then we can keep the value and write it to the list
                    return_list += [o]
                else:
                    # Otherwise, we omit the data item and add the current score to the rest
                    rest += o["Score"]
            
            # If the rest value has been changed, write it
            if rest != 0:
                
                # Write to list
                return_list += [{"Activity_Input": Scores[idx_I][idx_II]["Activity_Input"], "Score": rest, "Input": "Rest", "Method": Scores[idx_I][idx_II]["Method"], }]
    
    return return_list

# Function to rename the keys of Brightway2 database dictionary
def prepare_database(extracted: dict, additional_name_fragment: str, start: int):
    
    # Specify the mapping dictionary
    internal_mapping = {# "type": {"new_name": "type", "idx": start + 2},
                        "name": {"new_name": "name", "idx": start + 3},
                        "SimaPro_name": {"new_name": "SimaPro_name", "idx": start + 4},
                        "categories": {"new_name": "categories", "idx": start + 5},
                        "inventory_category": {"new_name": "categories", "idx": start + 6},
                        "location": {"new_name": "location", "idx": start + 7},
                        "unit": {"new_name": "unit", "idx": start + 9}}
    
    # Initialise empty dictionary
    return_dict = {}
    
    # Loop through each key/value pair (= dictionary of the inventory) in the loaded Brightway2 database
    for k, v in extracted.items():
        
        # Extract the code and database from the inventory key
        code = k[1]
        database = k[0]
        
        # Add the code and the database to the new item with the correct name
        curr = {str(start + 1) + "_" + additional_name_fragment + "code": code,
                str(start + 0) + "_" + additional_name_fragment + "database": database}
        
        # Loop through all the key/value pairs of the inventory dictionary
        for k2, v2 in v.items():
            
            # Try to add a new name
            # This will only be possible, if the current key is mapped in the internal mapping file
            # Otherwise, a key error is raised and nothing is done. In that case, the key will be omitted and not be mapped
            try:
                # Extract the new name based on the old name
                new_name = str(internal_mapping[k2]["idx"]) + "_" + additional_name_fragment + internal_mapping[k2]["new_name"]
                
                # Add the new name with the current value to the current new dictionary
                curr[new_name] = v2
            
            except:
                # Otherwise, pass
                pass
        
        # Add the mapped inventory to the new dictionary, which will be returned
        return_dict[k] = curr
        
    return return_dict
        
# Create a mapping for each database
def load_mappings(database_names: list, progress_bar: bool = True):
    
    # Save time at the start
    time_start = datetime.datetime.now()
    
    # Initialise empty dictionaries
    loaded_activity = {}
    loaded_flow = {}
    
    # Prepare iterator, either with or without progress bar
    # Depending on what is specified
    if progress_bar:
        
        # Wrap progress bar around iterator
        i_database_names = hp.progressbar(database_names, prefix = "\nLoad Brightway2 database mappings ...")
    else:
        # Do nothing
        i_database_names = database_names
    
    # Loop through each database name
    for idx, database_name in enumerate(i_database_names):
        
        # Extract the database inventories as dictionaries to a list
        extracted_orig = [m.as_dict() for m in bw.Database(database_name)]
        
        # Make a dictionary with the database/code tuples as keys and the inventory dictionary as value
        extracted = {(m["database"], m["code"]): m for m in extracted_orig}
        
        # Transform the extracted dictionaries with the function 'prepare_database' and add to the initialized dictionaries
        loaded_activity |= prepare_database(extracted, "Activity_", 0)
        loaded_flow |= prepare_database(extracted, "Flow_", 10)
    
    # Print statements to console, if specified
    if progress_bar:
        print("    " + str(len(database_names)) + " databases loaded")
        print("    Loading time: " + str(round((datetime.datetime.now() - time_start).seconds, 0)) + " sec")
    
    return loaded_activity, loaded_flow

# Rearrange columns of a pandas dataframe based on a pattern
# '2_ColA' and '1_ColA' will be arranged into --> '1_ColA' and '2_ColA'
def rearrange_columns(columns):
    
    # Pattern to extract the number and the column name
    pat = "^(?P<idx>[0-9]+)\_(?P<col>.*)"
    
    # Match the pattern to the column names and extract to dictionary
    matches = [re.match(pat, m).groupdict() for m in columns]
    
    # Sort the column names based on the number indicated
    col_tuples = sorted([(m["idx"], m["col"]) for m in matches], key = lambda x: int(x[0]))
    
    # Return a list with the new names
    return [str(m[0]) + "_" + str(m[1]) for m in col_tuples]

# Rename the column names
# Extract the first part (= number) and the second part (= name) and only give back the name
def rename_columns(columns):
    
    # Pattern to extract the number and the column name
    pat = "^(?P<idx>[0-9]+)\_(?P<col>.*)"
    
    # Match the pattern to the column names and extract to dictionary
    matches = [re.match(pat, m).groupdict() for m in columns]
    
    # Sort the column names based on the number indicated
    col_tuples = sorted([(m["idx"], m["col"]) for m in matches], key = lambda x: int(x[0]))
    
    # Return a dictionary of the old and the new name
    return {str(m[0]) + "_" + str(m[1]): m[1] for m in col_tuples}
    


#%% Function 'run_LCA' calculation

def run_LCA(activities: list,
            methods: list,
            write_LCI_exchanges: bool = True,
            write_LCI_exchanges_as_emissions: bool = True,
            write_LCIA_impacts_of_activity_exchanges: bool = True,
            write_LCIA_process_contribution: bool = True,
            write_LCIA_emission_contribution: bool = True,
            write_characterization_factors: bool = False,
            cutoff_process: (int | float) = 0.001,
            cutoff_emission: (int | float) = 0.001,
            write_results_to_file: bool = True,
            local_output_path: (pathlib.Path | None) = None,
            filename_without_ending: (str | None) = None,
            use_timestamp_in_filename: bool = True,
            print_progress_bar: bool = True):
    
    """ A function that does a fast and efficient LCA calculation in Brightway2
    Only the list of LCI activities and the LCIA methods need to be supplied to the function.
    'write' parameters state whether certain additional information from impact assessment should be provided or not.
    

    Parameters
    ----------
    activities : list
        A list of LCI inventories (in Brightway2 format, as Pewee object) for which impacts should be calculated.
        
    methods : list
        A list of LCIA methods (in Brightway2 format, as tuple) which should be used for impact assessment of the inventories.
    
    write_LCI_exchanges : bool, optional
        If True, the inputs (technosphere) and emissions (biosphere) for each of the activities is returned. The default is True.
    
    write_LCI_exchanges_as_emissions : bool, optional
        If True, the (biosphere) emissions for each of the activities is returned. The technosphere inputs are reduced to the respective biosphere emissions first. The default is True.
    
    write_LCIA_impacts_of_activity_exchanges : bool, optional
        If True, the impacts of all direct exchanges (biosphere and technosphere) of an activity are calculated. The default is True.
        
    write_LCIA_process_contribution : bool, optional
        If True, the process contribution (= all processes contributing to an LCA) is returned. The default is True.
        
    write_LCIA_emission_contribution : bool, optional
        If True, the emission contribution (= all emissions contributing to an LCA) is returned. The default is True.
        
    write_characterization_factors : bool, optional
        If True, returns a list of all characterization factors of used 'methods'. The default is False.
        
    cutoff_process : int | float, optional
        If specified and 'write_LCIA_process_contribution' is True, will cut down the list of processes and only show the processes which contribute more than the (impact * cut-off) to the overall impact. The default value is 0.001.
        
    cutoff_emission : int | float, optional
        If specified and 'write_LCIA_process_contribution' is True, will cut down the list of processes and only show the processes which contribute more than the (impact * cut-off) to the overall impact. The default value is 0.001.
    
    write_results_to_file : bool, optional
        Specifies whether the calculated results should be written to CSV or XLSX file. The default is True (a file is written).
    
    local_output_path : pathlib.Path | None, optional
        Specifies the path, where the output file should be written to. The default is None. If no path is specified (= None), the local Brightway2 folder will be used to save results.
        
    filename_without_ending : str, optional
        The filename(s) of the Excel or CSV files where results will be written to. The default is None. If not specified (= None), the timestamp plus an additional name fragment '_LCA' will be used as name.
        
    use_timestamp_in_filename : bool, optional
        Mark whether a timestamp should be used in the resulting filename. The default is True.
    
    print_progress_bar : bool, optional
        If True, progress bars will be printed which show the status of the calculation. The default is True.

    save_csv_for_visual_tool : bool, optional
        If True, will save a csv per activity in 'activities' in local_output_path suitable to be loaded in the visual tool 
        
    Returns
    -------
    results_final : dict
        A dictionary with all the results of the LCIA. Additionally, the Pandas Dataframe of the results is written, either as a XLSX or as a CSV (depending on the row size).

    """
    
    # Check function input type
    hp.check_function_input_type(run_LCA, locals())
    
    # Check if activities are all Pewee objects
    check_activities = [m for m in activities if not isinstance(m, bw2data.backends.peewee.proxies.Activity)]
    if check_activities != []:
        raise ValueError("Input variable 'activities' needs to be a list of only Pewee objects.")
    
    # Check if the cutoff is between 0 and 1. If not, the input variable is wrong and an error is raised.
    if cutoff_process < 0 or cutoff_process > 1:
        raise ValueError("The input variable 'cutoff_process' has a wrong value. Allowed are values between 0 and 1")
    
    # Check if the cutoff is between 0 and 1. If not, the input variable is wrong and an error is raised.
    if cutoff_emission < 0 or cutoff_emission > 1:
        raise ValueError("The input variable 'cutoff_emission' has a wrong value. Allowed are values between 0 and 1")
    
    # Create a path if not provided by the function
    # If not provided, the local Brightway2 folder will be used to save results
    if local_output_path is None:
        local_output_path = pathlib.Path(bw.projects.output_dir)
    
    # Extract current time
    current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    # Create filename if not provided
    if filename_without_ending is None:
        
        # Either use or do not use the timestamp in the filename
        # depending on how specified in the function input
        if use_timestamp_in_filename:
            
            # Give file name
            filename = current_time + "_LCA"
            
        else:
            # Give file name
            filename = "LCA"
    
    else:
        # The same as above
        if use_timestamp_in_filename:
            
            # Give file name
            filename = current_time + "_" + filename_without_ending
            
        else:
            # Give filename
            filename = filename_without_ending
    
    # Initialise an empty dictionary where results from the LCIA calculation will be stored
    results = {
        "LCI_exchanges": [],
        "LCI_exchanges_reduced_to_biosphere_flows": [],
        "LCIA_activity_scores": [],
        "LCIA_exchanges_scores": [],
        "LCIA_contribution_of_top_processes": [],
        "LCIA_contribution_of_top_emissions": []
        }
    
    # The functional unit is defined as 1
    functional_unit = 1
    
    # Extract the UUID's (= keys) from all activities
    activities_keys = [m.key for m in activities]
    
    # Extract the UUID's (= keys) from all exchanges (inputs/emissions)
    # The amount of the exchange is adjusted to the functional unit amount specified before
    if write_LCI_exchanges or write_LCIA_impacts_of_activity_exchanges:
        
        # Initialie list
        all_exchanges_keys = []
        
        # Loop through all activities to extract all exchanges
        for act in activities:
            
            # Initialise list
            exchange_info = []
            
            # Extract production amount directly from the inventory
            prod_amount = act.get("production amount")
            
            # If we fail to find the production amount from the inventory, go to the production exchange and extract it from there
            if prod_amount is None:
                
                # Extract all production amounts from the production exchanges
                prod_amounts = [n["amount"] for n in act.exchanges() if n["type"] == "production"]
                
                # Raise error if no production exchange was identified
                assert len(prod_amounts) == 1, str(len(prod_amounts)) + " production exchange(s) found. Valid is to have exactly one production exchange."
                
                # Use the only production exchange amount
                prod_amount = prod_amounts[0]
            
            # Raise error if we have not found an production amount
            assert prod_amount is not None, "No production exchange(s) found. Valid is to have exactly one production exchange."
            
            # Loop through all exchanges and adapt amounts
            for exc in act.exchanges():
                
                # Go on, if the current exchange is of type production. In that case we don't want to add it.
                if exc["type"] == "production":
                    continue
                
                else:
                    # Adapt amount with the production exchange amount and add to list
                    exchange_info += [{"type": exc["type"],
                                       "amount": exc["amount"] / prod_amount if prod_amount != 0 else 0,
                                       "input": exc["input"]}]
            
            # Add to variable
            all_exchanges_keys += [exchange_info]
                    
        
    # Write the exchanges to the results dictionary
    # But only if specified
    if write_LCI_exchanges:
        results["LCI_exchanges"] = [[{"Activity_Input": activities_keys[idx],
                                      "Input": n["input"],
                                      "Flow_type": n["type"], 
                                      "Flow_amount": n["amount"],
                                      "Method": None} for n in m] for idx, m in enumerate(all_exchanges_keys)]
        
    
    # Extract the number/length of the activities
    # This information will only be used for printing information to the console
    act_length = len(activities_keys)
    
    ###############################
    ##### Block 2 ##### (start) ###
    # -> Preload all data needed for the LCIA calculation (matrices, activity and biosphere mapping dictionaries and characterization factors)
    
    # Print statement
    print("\n-----\nInitializing calculation")
    
    # Extract all the databases that are used in the inventories
    # all_databases_used = list(set([m[0] for m in activities_keys + [nn[2] for n in all_exchanges_keys for nn in n]]))
    all_databases_used = list(bw.databases)
    
    # Initialise empty dictionaries to be filled with preloaded data
    characterization_matrices = {}
    activity_dicts = {}
    biosphere_dicts = {}
    characterization_factors = {}
    lca_objects = {}
    
    # Loop through each database used
    for database in all_databases_used:
        
        # Initialise new dictionaries in already existing dictionaries
        characterization_matrices[database] = {m: {} for m in methods}
        activity_dicts[database] = {m: {} for m in methods}
        biosphere_dicts[database] = {m: {} for m in methods}
        characterization_factors[database] = {m: {} for m in methods}
        
        try:
            # LCA object generation will fail for biosphere databases
            # However, we do not need an LCA object for biosphere databases but only the characterization factors
                        
            # Create the Brightway2 LCA object from the first activity and method.
            # This takes a little bit long, and therefore this step is only done once. The object will be reused in the calculation            
            lca_object = bw.LCA({[m.key for m in bw.Database(database)][0]: 1}, method = methods[0])
            
            # Calculate inventory once to load all database data
            lca_object.lci()
            
            # Keep the LU factorized matrices for faster calculations
            lca_object.decompose_technosphere()
            
            # Load method data 
            lca_object.lcia()
            
            # Append to dictionary
            lca_objects[database] = lca_object
        
        # Raise error if the database is not square
        except NonsquareTechnosphere:
            
            # Export inventory, where error appeared
            # Used for displaying additional information in the error statement
            inv_for_error = dict([m for m in bw.Database(database)][0].as_dict(), **{"exchanges": [n.as_dict() for n in [nn for nn in bw.Database(database)][0].exchanges()]})
            
            # Raise error
            raise NonsquareTechnosphere("'code' and 'database' parameter of inventory (= 'ds') probably don't match the parameter 'input' of the production exchange (= 'exc'):\n\ninventory code: " + inv_for_error.get("code", "code parameter not existent") + "\ninventory database: " + inv_for_error.get("database", "database parameter not existent") + "\n\nproduction exchange input (first item = database, second item = code): " + str([m for m in inv_for_error.get("exchanges", {"input": "no production exchange available", "__error__": True, "type": "nothing"}) if m.get("__error__") is not None and m["type"] == "production"][0].get("input")))
        
        # If the biosphere is empty, that means that an error has occured during setting up the inventories
        # An error is raised
        except EmptyBiosphere:
            
            # Export inventory, where error appeared
            # Used for displaying additional information in the error statement
            inv_for_error = dict([m for m in bw.Database(database)][0].as_dict(), **{"exchanges": [n.as_dict() for n in [nn for nn in bw.Database(database)][0].exchanges()]})
            
            # Raise error
            raise EmptyBiosphere("LCA can not be done if no biosphere flows are attributed to the inventory. Problematic inventory (... and possibly others):\n\ncode: " + inv_for_error["code"] + "\ndatabase: " + inv_for_error["database"] + "\nname: " + inv_for_error["name"] + "\nlocation: " + inv_for_error["location"] + "\nunit: " + inv_for_error["unit"])
        
        # If all arrays are empty, that means that the current database is a biosphere database
        # Therefore, we do not need to build a LCA object but only save the characterization factors
        except AllArraysEmpty:
            
            # Extract the characterization factors for all methods
            characterization_factors[database] = {m: {n[0]: n[1] for n in bw.Method(m).load()} for m in methods}
            continue
        
        # Loop through each method
        for method in methods:
            
            # Use the same LCA object but switch the method
            lca_object.switch_method(method)
            
            # Extract the characterized matrices
            characterization_matrices[database][method] = lca_object.characterization_matrix.copy()
            
            # Extract the mapping dictionaries
            # ... for the activities (technosphere)
            activity_dicts[database][method] = [k for k, v in lca_object.activity_dict.items()]
            
            # ... for the emissions (biosphere)
            biosphere_dicts[database][method] = [k for k, v in lca_object.biosphere_dict.items()]
            
            # Extract the characterization factors of the current method
            characterization_factors[database][method] = {m[0]: m[1] for m in bw.Method(method).load()}
        
    ##### Block 2 ##### (end) #####
    ###############################
    
    # Print statement
    print("\n-----\nCalculating results")
    
    ###############################
    ##### Block 3 ##### (start) ###
    # -> For each activity, run the impact assessment
    
    # Save the time at the beginning
    time_act = datetime.datetime.now()
    
    # To speed up the calculation, save already calculated results in a temporary dictionary
    # Those results can then be reused.
    # For that, initialise an empty dictionary
    temporary_saved_scores = {}
    
    # Prepare iterator, either with or without progress bar
    # Depending on what is specified
    if print_progress_bar:
        
        # Wrap progress bar around iterator
        i_activities_keys = hp.progressbar(activities_keys, prefix = "\nCalculate LCIA of activities ...")
    else:
        # Do nothing
        i_activities_keys = activities_keys
    
    # Loop through each activity individually
    for first, activity in enumerate(i_activities_keys):
        
        # Initialise lists for temporary results
        pack_I = []
        pack_II = []
        pack_III = []
        
        # Extract the database of the exchange from the key (= first item of the key is always the database)
        database = activity[0]
        
        # Extract the lca object
        lca_activity = lca_objects[database]
        
        # For each activity, at the beginning, the LCI calculation has to be redone
        lca_activity.redo_lci({activity: functional_unit})
        
        # Write the (reduced) biosphere emissions to the results dictionary
        # But only if specified
        if write_LCI_exchanges_as_emissions:
            results["LCI_exchanges_reduced_to_biosphere_flows"] += [[dict(m, **{"Activity_Input": activity}) for m in reduce_inventory_to_biosphere_emissions(lca_activity.inventory, biosphere_dicts[database][methods[0]])]]
        
        # Loop through each method individually
        for second, (method, matrix) in enumerate(characterization_matrices[database].items()):
            
            # Characterize the current matrix of the LCI with the matrix of the method
            characterized_matrix = matrix * lca_activity.inventory
            
            # Check whether the current activity and method combination has already been calculated
            # If yes, the result has already been saved in the 'temporary_saved_scores' dictionary
            if temporary_saved_scores.get((method, activity)) is not None:
                
                # If yes, simply save/add the already existing result
                pack_I += [{"Method": method, "Activity_Input": activity, "Score": temporary_saved_scores[(method, activity)]}]
            else:
                # Otherwise, sum up the characterized matrix (= total score)
                characterized_matrix_sum = characterized_matrix.sum()
                
                # Simply, save/add the newly calculated result
                pack_I += [{"Method": method, "Activity_Input": activity, "Score": characterized_matrix_sum}]
                
                # Write the newly calculated result to the temporary dictionary
                temporary_saved_scores[(method, activity)] = characterized_matrix_sum
              
            # Calculate the process contribution. Use the predefined function together with the characterized matrix
            # Use the activity dict for the mapping
            # But only if specified
            if write_LCIA_process_contribution:
                pack_II += [[dict(m, **{"Method": method, "Activity_Input": activity}) for m in top_processes_by_name(characterized_matrix, activity_dicts[database][method])]]
            
            # Calculate the emission contribution. Use the predefined function together with the characterized matrix
            # Use the biosphere dict for the mapping
            # But only if specified
            if write_LCIA_emission_contribution:
                pack_III += [[dict(m, **{"Method": method, "Activity_Input": activity}) for m in top_emissions_by_name(characterized_matrix, biosphere_dicts[database][method])]]
                        
        # Add the current results to the results dictionary
        results["LCIA_activity_scores"] += [pack_I]
        
        # Write but only if specified
        if write_LCIA_process_contribution:
            results["LCIA_contribution_of_top_processes"] += [pack_II]
        
        # Write but only if specified
        if write_LCIA_emission_contribution:
            results["LCIA_contribution_of_top_emissions"] += [pack_III]
            
        ##### Block 3 ##### (end) #####
        ###############################
    
    # Print additional progress bar statement to console, if specified
    if print_progress_bar:
        
        # Summary of the amount of calculated impacts
        print("    Calculated " + str(act_length * len(methods)) + " impacts (from " + str(act_length) + " inventories, " + str(len(methods)) + " methods)")
        
        # Summary of the calculation time
        print("    Calculation time: " + str(round((datetime.datetime.now() - time_act).seconds, 0)) + " sec")

    
    ###############################
    ##### Block 4 ##### (start) ###
    # -> For each immediate exchange of an activity, run the impact assessment
    
    
    # Only calculate if specified
    if write_LCIA_impacts_of_activity_exchanges:
        
        # Save the time at the beginning
        time_exc = datetime.datetime.now()
        
        # Prepare iterator, either with or without progress bar
        # Depending on what is specified
        if print_progress_bar:
            
            # Wrap progress bar around iterator
            i_all_exchanges_keys = hp.progressbar(all_exchanges_keys, prefix = "\nCalculate LCIA of immediate exchanges ...")
        else:
            # Do nothing
            i_all_exchanges_keys = all_exchanges_keys
        
        # Loop through all exchanges
        for first, exchanges in enumerate(i_all_exchanges_keys):
            
            # Initialise a list, where calculated impacts of the exchanges are stored
            immediate_exchange_scores = []

            # Loop through each exchange of the current activity
            # for exchange_type, exchange_amount, exchange in exchanges:
            for exc in exchanges:
                
                # Write to variables
                exchange_type, exchange_amount, exchange = exc["type"], exc["amount"], exc["input"]
                
                # Extract the database of the exchange from the key (= first item of the key is always the database)
                database = exchange[0]
                
                # If the exchange is of type technosphere, impacts need to be calculated through the Brigtway2 LCA object
                if exchange_type in ["technosphere", "substitution"]:
                    
                    # Extract the lca object
                    lca_activity = lca_objects[database]
                    
                    # Re do the LCI calculation using the new exchange
                    lca_activity.redo_lci({exchange: exchange_amount})
                
                # Loop through each method and calculate the impacts
                for second, (method, matrix) in enumerate(characterization_matrices[database].items()):
                    
                    # Calculation differs depending on the type of the exchange
                    # Either 'technosphere'/'substitution' or 'biosphere'
                    if exchange_type in ["technosphere", "substitution"]:
                        
                        # Use the LCA object to calculate impacts for technosphere exchanges
                        # Apply the same logic as above
                        # If impacts have already been calculated, use already calculated impact results
                        if temporary_saved_scores.get((method, exchange)) is not None:
                            immediate_exchange_scores += [{"Flow_type": exchange_type, "Activity_Input": activities_keys[first], "Method": method, "Input": exchange, "Score": temporary_saved_scores[(method, exchange)] * exchange_amount, "Flow_amount": exchange_amount}]
                        else:
                            # Otherwise, make the the characterized matrix and sum it up (= total score)
                            exchange_score = (matrix * lca_activity.inventory).sum()
                            immediate_exchange_scores += [{"Flow_type": exchange_type, "Activity_Input": activities_keys[first], "Method": method, "Input": exchange, "Score": exchange_score, "Flow_amount": exchange_amount}]
                            
                            if exchange_amount != 0:
                                temporary_saved_scores[(method, exchange)] = functional_unit / exchange_amount * exchange_score
                    
                    # If the type is 'biosphere'
                    elif exchange_type == "biosphere":
                        
                        # Use the results if already provided
                        if temporary_saved_scores.get((method, exchange)) is not None:
                            immediate_exchange_scores += [{"Flow_type": exchange_type, "Activity_Input": activities_keys[first], "Method": method, "Input": exchange, "Score": temporary_saved_scores[(method, exchange)] * exchange_amount, "Flow_amount": exchange_amount}]
                        else:
                            # Otherwise, calculate the results using the characterization factors
                            # Load the characterization factors
                            characterization_factors_loaded = characterization_factors[database][method]
                            
                            # Calculate the impact by multliplying the amount with the factor
                            # If no factor is found, use 0
                            exchange_score = characterization_factors_loaded.get(exchange, 0) * exchange_amount
                            
                            # Add the results to the final list
                            immediate_exchange_scores += [{"Flow_type": exchange_type, "Activity_Input": activities_keys[first], "Method": method, "Input": exchange, "Score": exchange_score, "Flow_amount": exchange_amount}]
                            
                            # Temporarily, save the results
                            # temporary_saved_scores[(method, exchange)] = functional_unit / exchange_amount * exchange_score
                            temporary_saved_scores[(method, exchange)] = characterization_factors_loaded.get(exchange, 0)
                    
                    else:
                        # If the type is other than 'biosphere' or 'technosphere', raise an error
                        # Note: this should not be the case because we only consider exchanges of this types
                        raise TypeError("Current exchange type " + str(exchange_type) + " not evaluated for impact assessment.")
            
            # Add the results to the results dictionary
            results["LCIA_exchanges_scores"] += [immediate_exchange_scores]
            
            ##### Block 4 ##### (end) #####
            ###############################
        
        # Print additional progress bar statement to console, if specified
        if print_progress_bar:
            
            # Summary of the amount of calculated impacts
            print("    Calculated " + str(len(hp.flatten(all_exchanges_keys)) * len(methods)) + " impacts (from " + str(len(hp.flatten(all_exchanges_keys))) + " exchanges, " + str(len(methods)) + " methods)")
            
            # Summary of the calculation time
            print("    Calculation time: " + str(round((datetime.datetime.now() - time_exc).seconds, 0)) + " sec")
        
    
    # Make a short validation. The list lengths of the produced 'results' dictionary need to be of the same size
    # Otherwise, an error occurred
    # Extract the list lengths
    all_lengths = list(set([len(v) for k, v in results.items() if v != []]))
    
    # Check whether different list lengths have been found
    if len(all_lengths) > 1:
        
        # If yes, an error is raised
        raise ValueError("Different lengths for key/value pairs were found in the 'results' variable.")
    
    
    ###############################
    ##### Block 5 ##### (start) ###
    # -> Depending on the results, it may be that list of lists of list ... have been generated
    # -> We need to flatten those lists, in order to properly work with them
    
    # Print statement
    print("\n-----\nPreparing results")
    
    # Initialise an empty dictionary, where flattened lists will be written to as values
    results_flattened = {}
    
    # Loop through each key (= type of result) and value (= list of values of results) of the produced results from beforehand
    for k1, v1 in results.items():
        
        # Go on if the current list (= v1) is empty
        # That means, that no data was added
        if v1 == []:
            continue
        
        # Flatten the list using the helper function 'flatten'
        v1_flattened = [m for m in hp.flatten(v1)]
        
        # Check if there is the need to further flatten.
        # We need to further flatten if after the flattening, there are still lists available
        all_instances_are_lists = [True if isinstance(m, list) else False for m in v1_flattened]
        
        # Check if all instances are still lists.
        if all(all_instances_are_lists):
            
            # Here, we further flatten the lists
            # Additionally, if needed, the 'apply_cutoff' function is used to reduce the amount of data points
            # Cutoff function can only be applied to the emission and/or process contribution
            # Otherwise, lists will only be flattened
            
            # Process contribution
            if k1 == "LCIA_contribution_of_top_processes":
                results_flattened[k1] = apply_cutoff(v1, results["LCIA_activity_scores"], cutoff_process)
            
            # Emission contribution
            elif k1 == "LCIA_contribution_of_top_emissions":
                results_flattened[k1] = apply_cutoff(v1, results["LCIA_activity_scores"], cutoff_emission)
            
            # All other results
            else:
                results_flattened[k1] = [o for idx_I, m in enumerate(v1) for idx_II, n in enumerate(m) for o in n]
        
        else:
            # Otherwise, make a double loop to flatten
            results_flattened[k1] = [n for idx, m in enumerate(v1) for n in m]
            
        ##### Block 5 ##### (end) #####
        ###############################
    
    
    # Extract the database names which are used by the results dictionary
    all_database_names_used = list(set([m.get("Input", [None])[0] for k, v in results_flattened.items() for m in v if isinstance(m.get("Input"), tuple)] + [m.get("Activity_Input", [None])[0] for k, v in results_flattened.items() for m in v if isinstance(m.get("Activity_Input"), tuple)]))
    
    # Load the data from the databases in order to map
    # Load the mapping data for activities and for emissions (= flow)
    mapping_activity, mapping_flow = load_mappings(all_database_names_used, progress_bar = print_progress_bar)
    
    # Load the mapping for the method units
    mapping_method_unit = {m: bw.Method(m).metadata.get("unit", "unit could not be extracted") for m in methods}
    
    ###############################
    ##### Block 6 ##### (start) ###
    # -> We have just used a minimum amout of data so far
    # -> To understand the data that we generated, we need to append/map additional information
    
    # Initialise an empty dictionary
    results_final = {}
    
    # Loop through all the result key/value pairs in the flattened results dictionary
    for k, v in results_flattened.items():
        
        # Initialise an empty list
        final = []
        
        # Loop through each item in the current results list
        for m in v:
            
            # Each item is built from scratch. Therefore, we need to initialise an empty dictionary at each iteration
            item = {}
            
            # Loop through each key/value pair of the current item
            for k_m, v_m in m.items():
                
                # Different actions are required depending on the key name
                
                # If the key is 'Input', it is generally mapped with the help of the mapping dictionary which was created beforehand
                if k_m == "Input":
                    
                    # However, if the value is 'Rest' (emerging from applying a cutoff)
                    # we don't need to add any other information from the mapping
                    if v_m == "Rest":
                        
                        # Just add the flow name directly which will remain as 'Rest'
                        # The number 13 specifieds the location of the column at the end in the pandas Dataframe
                        item["13_Flow_name"] = "Rest"
                    else:
                        # Otherwise, use the flow mapping to extract all other relevant information which will be added to the new dictionary
                        item |= mapping_flow[v_m]
                
                # The same is done for the activity mapping, using the activity instead of the flow mapping
                # However, here we do not have an exception such as 'Rest'
                elif k_m == "Activity_Input":
                    
                    # Add the additional information extracted through the activity mapping to the current dictionary
                    item |= mapping_activity[v_m]
                
                # If the key is 'Method', simply add it to the new dict
                # Also extract and append the method's unit
                elif k_m == "Method":
                    
                    # If no method was specified (that means no impacts were generated, e.g. for exchanges), just go on
                    if v_m is None:
                        continue
                    
                    # Extract the method from the method mapping and add
                    item["1000_Score_unit"] = mapping_method_unit[v_m]
                    
                    # Append the method
                    item["1001_" + k_m] = v_m
                
                # If the key is 'Score', simply append the existing information
                elif k_m == "Score":
                    item["100_" + k_m] = v_m
                
                # If the key is 'Flow_amount', simply append the existing information
                elif k_m == "Flow_amount":
                    item["18_" + k_m] = v_m
                    
                # If the key is 'Flow_type', simply append the existing information
                elif k_m == "Flow_type":
                    item["12_" + k_m] = v_m
                
                # If the current key is None of the keys specified in this if/else statement,
                # an error will be raised
                else:
                    raise ValueError("Key '" + str(k_m) + "' is currently not handled in Block 6. Check!")
            
            # Add the activity amount (specified as functional unit) to the newly created dictionary
            # and add the dictionary to the final results list
            final += [dict(item, **{"8_Activity_amount": functional_unit})]
        
        # Add the new list with the existing key to the dictionary
        results_final[k] = final 
        
        ##### Block 6 ##### (end) #####
        ###############################
        
    
    ###############################
    ##### Block 7 ##### (start) ###
    # -> Convert dictionary data into Pandas DataFrames
    # -> Write results to XLSX or CSV
    
    # Initialise an empty dictionary where dataframes will be temporarily saved to
    dataframes = {}
    
    # Write results of the simple LCIA scores to dataframe
    df_summary_orig = pd.DataFrame(results_final["LCIA_activity_scores"])
    
    # Rearrange and rename the columns in the dataframe
    df_summary = df_summary_orig[rearrange_columns(list(df_summary_orig.columns))].rename(columns = rename_columns(list(df_summary_orig.columns)))
    
    # Add to the new dictionary
    dataframes["LCIA_summary"] = df_summary
    
    # Initialize a documentation variable
    documentation = {"LCIA_summary": "This dataframe contains the result of the LCA calculation and shows the total environmental impact of all inventories (LCI) and methods (LCIA) combinations."}
    
    # Write and add Pandas DataFrame for each type of results depending on whether the results have been produced or not
    # ... LCI exchanges
    if write_LCI_exchanges:
        
        # Append to documentation
        documentation["LCI_exchanges"] = "This dataframe contains detailed information about the inventories. It shows the inputs and emissions (biosphere and technosphere) for the first layer/tier. It is equivalent to what you see when you open the inventory in SimaPro."
        
        # Convert to dataframe
        df_exchanges_orig = pd.DataFrame(results_final["LCI_exchanges"])
        
        # Rearrange and rename columns and save to dictionary
        dataframes["LCI_exchanges"] = df_exchanges_orig[rearrange_columns(list(df_exchanges_orig.columns))].rename(columns = rename_columns(list(df_exchanges_orig.columns)))
    
    # ... LCI exchanges as biosphere emissions
    if write_LCI_exchanges_as_emissions:
        
        # Append to documentation
        documentation["LCI_exchanges_as_emissions"] = "This dataframe contains detailed information about the inventories. It shows the summarized biosphere emissions from the whole supply chain (inventory matrix) and not only from the first layer/tier. Note: it shows only the uncharacterized emissions and therefore is not connected to any LCIA method."     
        
        # Convert to dataframe
        df_exchanges_biosphere_orig = pd.DataFrame(results_final["LCI_exchanges_reduced_to_biosphere_flows"])
        
        # Rearrange and rename columns and save to dictionary
        dataframes["LCI_exchanges_as_biosphere_emissions"] = df_exchanges_biosphere_orig[rearrange_columns(list(df_exchanges_biosphere_orig.columns))].rename(columns = rename_columns(list(df_exchanges_biosphere_orig.columns)))
    
    # ... LCIA impacts of immediate activity exchanges
    if write_LCIA_impacts_of_activity_exchanges:
        
        # Append to documentation
        documentation["LCIA_impacts_of_activity_exchanges"] = "This dataframe shows the contribution of the individual biosphere and technosphere flows to the overall impact of an inventory. It is similarly to 'LCI_exchanges' with the difference, that for each biosphere and technosphere flow, the contribution to the environmental impact is shown."
        
        # Convert to dataframe
        df_exchanges_impacts_orig = pd.DataFrame(results_final["LCIA_exchanges_scores"])
        
        # Rearrange and rename columns and save to dictionary
        dataframes["LCIA_impacts_of_activity_exchanges"] = df_exchanges_impacts_orig[rearrange_columns(list(df_exchanges_impacts_orig.columns))].rename(columns = rename_columns(list(df_exchanges_impacts_orig.columns))).sort_values(by = ["Method", "Activity_code"])
    
    # ... LCIA emission contribution
    if write_LCIA_emission_contribution:
        
        # Append to documentation
        documentation["LCIA_emission_contribution"] = "This dataframe shows the contribution of all elementary flows/biosphere flows (e.g., carbon dioxide) from the whole supply chain of the inventory to the overall environmental impact of the inventory."
        
        # Convert to dataframe
        df_emissions_orig = pd.DataFrame(results_final["LCIA_contribution_of_top_emissions"])
        
        # Rearrange and rename columns and save to dictionary
        dataframes["LCIA_emission_contribution"] = df_emissions_orig[rearrange_columns(list(df_emissions_orig.columns))].rename(columns = rename_columns(list(df_emissions_orig.columns)))
     
    # ... LCIA process contribution
    if write_LCIA_process_contribution:
        
        # Append to documentation
        documentation["LCIA_process_contribution"] = "This dataframe shows the contribution of all activities/processes (e.g., electricity) from the whole supply chain of the inventory to the overall environmental impact of the inventory."
        
        # Convert to dataframe
        df_processes_orig = pd.DataFrame(results_final["LCIA_contribution_of_top_processes"])
        
        # Rearrange and rename columns and save to dictionary
        dataframes["LCIA_process_contribution"] = df_processes_orig[rearrange_columns(list(df_processes_orig.columns))].rename(columns = rename_columns(list(df_processes_orig.columns)))
    
    # ... Characterization factors
    if write_characterization_factors:
        
        # Append to documentation
        documentation["Characterization_factors"] = "This dataframe shows all characterization factors of all LCIA methods that were used for LCA calculation."
        
        # Extract characterization factors directly to dataframe
        df_factors = pd.DataFrame(extract_CF(methods))
        
        # Save to dictionary
        dataframes["Characterization_factors"] = df_factors
    
    # Clean the keys --> remove the numbers from them again
    results_final_keys_cleaned = {k: [{"_".join(k2.split("_")[1:]): v2 for k2, v2 in m.items()} for m in v] for k, v in results_final.items()}
    
    # Return dictionary and do not write result as a file
    if not write_results_to_file:
        return results_final_keys_cleaned
        
    # Check, if row length is above or beyond the maximum number of rows set by Excel
    # If it is beyond the limit, an Excel file is written
    # Otherwise, CSV files are written
    write_CSV_bool = any([True if len(v) > 1048000 else False for k, v in dataframes.items()])
    
    # Append documentation to dataframes
    dataframes["Documentation"] = pd.DataFrame([{"Sheet" if not write_CSV_bool else "File": "'" + sheet_name + "'" if not write_CSV_bool else "'" + filename + "_" + sheet_name + ".csv'",
                                                 "Description": doc} for sheet_name, doc in documentation.items()])
    
    # Prepare iterator, either with or without progress bar
    # Depending on what is specified
    if print_progress_bar:
        
        # Wrap progress bar around iterator
        i_dataframes = hp.progressbar(dataframes.items(), prefix = "\nWrite data ...")
    else:
        # Do nothing
        i_dataframes = dataframes.items()
    
    # Decide, whether to write CSV files or XLSX file
    if write_CSV_bool:
        
        # Print statement
        print("-----\nSaving CSV files")
        
        # Loop through all dataframes and write
        for idx, (df_name, df_data) in enumerate(i_dataframes):
            
            # Write data
            df_data.to_csv(local_output_path / str(filename + "_" + df_name + ".csv"))
            
        # Print statement saying where CSV files were stored
        print("-----\nLCA results saved to:\n" + os.path.join(str(local_output_path)))


    else:
        # Write data to XLSX
        # Initialise the writer variable
        writer = pd.ExcelWriter(local_output_path / str(filename + str(".xlsx")))
        
        # Print statement
        print("\n-----\nSaving XLSX file")
        
        # Loop through all dataframes and write
        for idx, (df_name, df_data) in enumerate(i_dataframes):
            
            # Write data
            df_data.to_excel(writer, sheet_name = df_name[:30])
        
        # Print statement
        print("\n-----\nClose file")
        
        # Save the XLSX
        writer.close()
        
        # Print statement saying where XLSX file was stored
        print("\n-----\nLCA results saved to:\n" + str(local_output_path / str(filename + ".xlsx")))
    
        

    ##### Block 7 ##### (end) #####
    ###############################    
    
    # Return dictionary
    return results_final_keys_cleaned


