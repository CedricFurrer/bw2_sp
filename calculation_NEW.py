import pathlib
import os

if __name__ == "__main__":
    os.chdir(pathlib.Path(__file__).parent)

import re
import copy
import scipy
import numpy
import pathlib
import datetime
import bw2calc
import bw2data
import numpy as np
import pandas as pd
from bw2calc.errors import NonsquareTechnosphere, EmptyBiosphere, AllArraysEmpty
import helper as hp


#%%
here: pathlib.Path = pathlib.Path(__file__).parent
from utils import change_brightway_project_directory

change_brightway_project_directory(str(here / "notebook" / "Brightway2_projects"))
project_name: str = "Food databases"
bw2data.projects.set_current(project_name)

acts: list = [m for m in bw2data.Database("AgriFootprint v6.3 - SimaPro")][0:1000]
mets: list[tuple] = [m for m in bw2data.methods if "SALCA" in m[0]]

#%%

class LCA_Calculation():
    
    def __init__(self,
                 activities: list,
                 methods: list,
                 functional_amount: (float | int) = 1,
                 cut_off_percentage: (float | int | None) = None,
                 exchange_level: int = 1,
                 print_progress_bar: bool = True) -> None:
        
        
        # Check function input type
        hp.check_function_input_type(self.__init__, locals())
        
        # Check if activities are all Brightway activity objects
        check_activities = [m for m in activities if not isinstance(m, bw2data.backends.peewee.proxies.Activity)]
        
        # Check and raise error
        if check_activities != []:
            raise ValueError("Input variable 'activities' needs to be a list of only Activity objects.")
            
        # Check if methods are all tuples
        assert all([isinstance(m, tuple) for m in methods]), "At least one method is not of type <tuple>."
        
        # Extract all methods that are not registered in the Brightway background
        check_methods = [str(m) for m in methods if m not in bw2data.methods]
        
        # Check if all methods are registered in the Brightway background
        if check_methods != []:
            raise ValueError("They following methods are not registered:\n - " + "\n - ".join(check_methods))
        
        # Raise error if level is smaller than 1
        if exchange_level < 1:
            raise ValueError("Input variable 'exchange_level' needs to be greater than 1 but is currently '" + str(exchange_level) + "'.")
        
        # Raise error if the cut off is not between 0 and 1
        if cut_off_percentage is not None and (cut_off_percentage < 0 or cut_off_percentage > 1):
            raise ValueError("Input variable 'cut_off_percentage' needs to be between 0 and 1 but is currently '" + str(cut_off_percentage) + "'.")
        
        # Loop through each activity and construct the key tuple if not yet existing
        for activity in activities:
            
            # Check if the key is already existing
            if not hasattr(activity, "key"):
                
                # Extract key components, database and code
                database: (str | None) = activity["database"]
                code: (str | None) = activity["code"]
                
                # Raise error if they were not found
                if database is None or code is None:
                    raise ValueError("Activity key tuple could not be constructed.")
                
                # Add key attribute
                activity.key: tuple[str, str] = (database, code)
        
        # The functional unit is defined as 1
        self.functional_amount: (int | float) = functional_amount
        
        # Add to object
        self.activities: list[bw2data.backends.peewee.proxies.Activity] = activities
        self.methods: list[tuple] = methods
        self.cut_off_percentage: (int | float) = cut_off_percentage
        self.exchange_level: int = exchange_level
        self.progress_bar: bool = print_progress_bar
        
        # Create a list of tuples with all the activity - method - combinations that should be calculated
        # This will be used further below when calculating (as iterator for the progressbar)
        self.activity_method_combinations: list = [(n, m) for m in self.methods for n in self.activities]
        
        # Definition of names to be used for the different results
        self.name_LCIA_scores: str = "LCIA_scores"
        self.name_LCI_emission_contributions: str = "LCI_emission_contribution"
        self.name_LCI_process_contributions: str = "LCI_process_contribution"
        self.name_LCIA_emission_contributions: str = "LCIA_emission_contribution"
        self.name_LCIA_process_contributions: str = "LCIA_process_contribution"
        self.name_characterization_factors: str = "Characterization_factors"
        
        self.lca_objects: dict = {} # LCIA score
        self.characterization_matrices: dict = {} # LCIA score, LCIA emission contr.
        self.biosphere_dicts_as_arrays: dict = {} # LCIA emission contr.
        self.element_arrays: dict = {} # LCIA emission contr.
        self.structured_arrays_for_LCIA_emission_contribution: dict = {} # LCIA emission contr.
    
    
    def _get_LCA_object(self, database: str) -> bw2calc.lca.LCA:
        
        if database in self.lca_objects:
            return self.lca_objects[database]
        
        # Extract first inventory and method to then initialise lca object
        _act: bw2data.backends.peewee.proxies.Activity = bw2data.Database(database).random()
        _met: tuple = self.methods[0]
        
        # LCA object generation will fail for biosphere databases
        # However, we do not need an LCA object for biosphere databases but only the characterization factors
        
        # Create the Brightway2 LCA object from the first activity and method.
        # This takes a little bit long, and therefore this step is only done once. The object will be reused in the calculation            
        lca_object: bw2calc.lca.LCA = bw2calc.LCA({_act.key: self.functional_amount}, method = _met)
        
        # Calculate inventory once to load all database data
        lca_object.lci()
        
        # Load method data 
        lca_object.lcia()
        
        # Append to dictionary and initialize a dictionary to store the characterized matrices to
        self.lca_objects[database]: bw2calc.lca.LCA = lca_object
        self.characterization_matrices[database]: dict = {}
        
        # Loop through each method to build the characterization matrices
        for met in self.methods:
            
            # Switch to new method
            lca_object.switch_method(met)
            
            # Extract the characterization matrix and write to dictionary
            self.characterization_matrices[database][met] = lca_object.characterization_matrix.copy()
        
        # Return the lca object
        return self.lca_objects[database]
    
    
    def _get_characterization_matrix(self, database: str, method: str):
        
        # Simply return if already existing
        if database in self.characterization_matrices:
            return self.characterization_matrices[database][method]

        # Otherwise, create BW object and matrices
        self._get_LCA_object(database = database)
        
        # Return the matrix
        return self.characterization_matrices[database][method]
    
    
    
    def _get_biosphere_dict_as_array(self, database: str) -> np.array:
        
        # Simply return if already existing
        if self.biosphere_dicts_as_arrays.get(database) is not None:
            return self.biosphere_dicts_as_arrays[database]
        
        # Retrieve the lca object
        lca_object: bw2calc.lca.LCA = self._get_LCA_object(database = database)
        
        # Write the biosphere dict as array
        biosphere_array: np.array = np.array(list(lca_object.biosphere_dict.keys()))
        
        # Temporarily store
        self.biosphere_dicts_as_arrays[database]: np.array = biosphere_array
        
        # Return the matrix
        return biosphere_array
    
    
    
    def _get_element_array(self, element: (str | None | tuple), length: int) -> np.array:
        
        # Simply return if already existing
        if self.element_arrays.get((element, length)) is not None:
            return self.element_arrays[(element, length)]
        
        # Construct an array with only elements and length indicated
        element_array: np.array = np.array([element] * length)
        
        # Add the matrix to the temporary dict
        self.element_arrays[(element, length)]: np.array = element_array
        
        # Return the matrix
        return element_array
    
    
    def _apply_cut_off_to_structured_array(self, structured_array: np.array) -> np.array:
        
        array: np.array = structured_array[structured_array["value"] != 0]
        last: tuple = tuple(array[-1:])
        
        mask_under_0 = array["value"] < 0
        mask_over_0 = array["value"] > 0
        
        under_0 = array[mask_under_0]
        over_0 = array[mask_over_0]
        
        min_treshold = sum(under_0["value"]) * self.cut_off_percentage
        max_treshold = sum(over_0["value"]) * self.cut_off_percentage
        
        mask_min_treshold = array["value"] < min_treshold
        mask_max_treshold = array["value"] > max_treshold

        mask_rest = (~mask_min_treshold) & (~mask_max_treshold)
        rest_amount = sum(array["value"][mask_rest])
        
        updated_last: tuple = last[:2] + (rest_amount,) + last[3:]
        rest: np.array = np.array([updated_last[0]], dtype = structured_array.dtype)
        
        constructed = np.concatenate((over_0, under_0, rest), axis = 0)
        
        return constructed
    
    
    def _get_structured_arrays_for_LCIA_emission_contribution(self, database: str) -> None:
        
        if database in self.structured_arrays_for_LCIA_emission_contribution:
            return
        
        biosphere_array: np.array = self._get_biosphere_dict_as_array(database = database)
        biosphere_array_dtype = biosphere_array.dtype
        length_biosphere_array: int = biosphere_array.shape[0]
        width_biosphere_array: int = biosphere_array.shape[1]
        
        none_array: np.array = self._get_element_array(element = None, length = length_biosphere_array)
        none_array_dtype = none_array.dtype

        functional_amount_array: np.array = self._get_element_array(element = self.functional_amount, length = length_biosphere_array)
        functional_amount_array_dtype = functional_amount_array.dtype
        
        activity_key_array: np.array = self._get_element_array(element = self.activities[0].key, length = length_biosphere_array)
        activity_key_array_dtype = activity_key_array.dtype
        width_activity_key_array = activity_key_array.shape[1]
        
        self.structured_arrays_for_LCIA_emission_contribution[database]: dict = {}
        
        for met in self.methods:
            
            method_array: np.array = self._get_element_array(element = met, length = length_biosphere_array)
            method_array_dtype = method_array.dtype
            width_method_array = method_array.shape[1]
            
            inventory_dtype = self._get_LCA_object(database = database).inventory.dtype
            
            dtype: list[tuple] = [("activity_key", activity_key_array_dtype, (width_activity_key_array,)),
                                  ("functional_amount", functional_amount_array_dtype),
                                  ("flow_key", biosphere_array_dtype, (width_biosphere_array,)),
                                  ("flow_amount", none_array_dtype),
                                  ("value", inventory_dtype),
                                  ("method", method_array_dtype, (width_method_array,))
                                  ]
            
            self.structured_arrays_for_LCIA_emission_contribution[database][met]: np.array = np.core.records.fromarrays([activity_key_array,
                                                                                                                         functional_amount_array,
                                                                                                                         biosphere_array,
                                                                                                                         none_array,
                                                                                                                         np.zeros(length_biosphere_array, dtype = inventory_dtype),
                                                                                                                         method_array
                                                                                                                         ], dtype = dtype)
    
    
    def calculate(self,
                  calculate_LCIA_scores: bool = True,
                  extract_LCI_exchanges: bool = False,
                  extract_LCI_emission_contribution: bool = False,
                  extract_LCI_process_contribution: bool = False,
                  calculate_LCIA_scores_of_exchanges: bool = False,
                  calculate_LCIA_emission_contribution: bool = False,
                  calculate_LCIA_process_contribution: bool = False):
        
        # Add an instance where results will be saved to
        self.results: dict = {self.name_LCIA_scores: [],
                              self.name_LCIA_emission_contributions: [],
                              self.name_LCIA_process_contributions: []
                              }
        
        # Save time when calculation starts
        if self.progress_bar:
            start: datetime.datetime = datetime.datetime.now()
        
        # Retrieve the combinations that should be calculated
        activities: list = self.activities
        
        # Check if progress bar should be printed to console
        if self.progress_bar:
            
            # Wrap progress bar around iterator
            activities: list = hp.progressbar(activities, prefix = "\nCalculate ...")
        
        # Loop through all activities
        for act in activities:
            
            # Extract the activity key
            activity_key: tuple[str, str] = act.key
            
            # First get the database the activity belongs to
            database: str = activity_key[0]

            # This block is needed for any calculation
            # We at least need
            # ... to get 1) the BW object,
            lca_object: bw2calc.lca.LCA = self._get_LCA_object(database = database)

            # ... to redo 2) the inventory matrix (which takes quite some time)
            lca_object.redo_lci({act: self.functional_amount})
            
            # We then extract the inventory matrix
            inventory: np.matrix = lca_object.inventory

            # If we need to do any LCIA calculation, we need to do the following block
            # This however will only be done for LCIA score calculation, LCIA emission contribution and LCIA process contribution
            if any([calculate_LCIA_scores, calculate_LCIA_emission_contribution, calculate_LCIA_process_contribution]):
                
                # Initialize a temporary dictionary to store the characterized matrices to
                characterized_matrices: dict = {}
                
                # Loop through each method and create the characterized matrices by multiplying the inventory matrix with the 
                for met in self.methods:
                    
                    # Build or simply get the matrix
                    matrix = self._get_characterization_matrix(database = database, method = met)
                    
                    # Multiply the matrices
                    characterized_matrices[met] = (matrix * inventory)
            
            # If LCIA scores are calculated, go on
            if calculate_LCIA_scores:
                
                # Loop through each characterized matrix and method to get the sum/LCIA score
                for met, characterized_matrix in characterized_matrices.items():
                    
                    # The ID tuple is always of length 5!
                    ID: tuple = (
                        act.key, # Key of the current activity
                        self.functional_amount, # The amount that was calculated
                        None, # Flow key, not used here
                        None, # Flow amount, not used here
                        characterized_matrix.sum(), # The calculated impact assessment result
                        met # The method we calculated the result for
                    ) 
                    
                    # Add to list in the result dictionary
                    self.results[self.name_LCIA_scores] += [ID]
            
            # Prepare or retrieve structured arrays for LCIA emission contribution
            if calculate_LCIA_emission_contribution:
                self._get_structured_arrays_for_LCIA_emission_contribution(database = database)
            
            if any((calculate_LCIA_emission_contribution, calculate_LCIA_process_contribution)):
                
                # Loop through each characterized matrix and method to get the sum/LCIA score
                for met, characterized_matrix in characterized_matrices.items():
                    
                    if calculate_LCIA_emission_contribution:
                        
                        # time: tuple = ()
                        # time += (datetime.datetime.now(),) # !!!
                        # print([(time[m] - time[m-1]).total_seconds() for m in list(range(len(time)))[1:]])

                        # Extract the emission contribution as the sum of the characterized matrix
                        LCIA_emission_contribution_array: np.array = np.array(characterized_matrix.sum(axis = 1))[:, 0]
                        
                        structured_array: np.array = self.structured_arrays_for_LCIA_emission_contribution[database][met]
                        structured_array["value"] = LCIA_emission_contribution_array
                        
                        structured_array_cutoff: np.array = self._apply_cut_off_to_structured_array(structured_array = structured_array)
                        structued_array_cutoff_as_list: list[tuple] = structured_array_cutoff.tolist()
                        self.results[self.name_LCIA_emission_contributions] += [structued_array_cutoff_as_list]

                        
                        
        # Print summary statement(s)
        if self.progress_bar:
            print("  - Calculation /extraction time: {}".format(self.convert_timedelta(datetime.datetime.now() - start)))
            
            if calculate_LCIA_scores:
                print("      - {} LCIA score(s) from {} activity/ies & {} methods were calculated".format(len(self.activities)*len(self.methods), len(self.activities), len(self.methods)))
    

    
    # Function to convert time delta in a readable, nice string
    def convert_timedelta(self, timedelta: datetime.timedelta) -> str:
        
        # Check function input type
        hp.check_function_input_type(self.convert_timedelta, locals())
        
        # Get total days and total seconds
        days, seconds = timedelta.days, timedelta.seconds
        
        # Convert to hours, minutes and seconds
        hours = days * 24 + seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = (seconds % 60)
        
        # Create string
        if hours > 0:
            string: str = str(hours) + " hour(s) and " + str(minutes) + " minute(s)"
            
        elif minutes > 0:
            string: str = str(minutes) + " minute(s) and " + str(seconds) + " second(s)"
            
        elif seconds > 0:
            string: str = str(seconds) + " second(s)"
            
        else:
            string: str = "less than a second"
            
        # Return string
        return string


lca_calculation: LCA_Calculation = LCA_Calculation(activities = acts,
                                                   methods = mets,
                                                   cut_off_percentage = 0.01)
lca_calculation.calculate(calculate_LCIA_scores = True,
                          extract_LCI_exchanges = False,
                          extract_LCI_emission_contribution = False,
                          extract_LCI_process_contribution = False,
                          calculate_LCIA_scores_of_exchanges = False,
                          calculate_LCIA_emission_contribution = True,
                          calculate_LCIA_process_contribution = False)
results: list[tuple] = lca_calculation.results
print(tuple(results["LCIA_emission_contribution"][0][0][2]))

#%%

class LCA_Calculation():
    
    def __init__(self,
                 activities: list,
                 methods: list,
                 functional_amount: (float | int) = 1,
                 cut_off_percentage: (float | int | None) = None,
                 exchange_level: int = 1,
                 print_progress_bar: bool = True) -> None:
        
        """ A class that provides functions to do fast and efficient LCA calculations in Brightway2
        Only the list of LCI activities and the LCIA methods need to be supplied.
        
        
        Parameters
        ----------
        activities : list
            A list of activities (LCI) (in Brightway format, as activity objects retrieved from background databases) for which impacts should be calculated.
            
        methods : list
            A list of LCIA methods (in Brightway format, as tuple) which should be used for impact assessment of the inventories.
            Methods need to be registered in the Brightway background in order to work.
            
        functional_amount : (float | int)
            Indicates the reference unit of the calculated environmental impacts. Refers to how much of an activity should be calculated.
            With value of 1, it will calculate/retrieve information for 1 unit of the activity.
            The default is 1.
        
        cut_off_percentage : (float | int | None)
            Will only be applied in case LCIA emission contribution and LCIA process contribution are calculated. The value needs to be between 0 and 1.
            When a percentage is indicated (not None), the list of contributing emissions or processes will be shortened.
            The higher the percentage, the higher the cut off for scores and therefore the shorter the list.
            Impacts that are cut will be summarised into a 'Rest' value.
            The default is None (no cut off applied).
            
        exchange_level : int
            Needs to be >= 1 in order to work. Specifies at which level/tier the exchanges of an activity should be extracted from.
            The higher the level, the more exchanges and the longer the calculation will take.
            Important: for emissions, there are no subsequent levels. It would be wrong to just avoid them. That's why for emissions the level might be smaller than what is specified here.
            The default is 1.
        
        print_progress_bar : bool
            Specifies whether to print a progress bar to the console indicating the progress of the calculation
            
        """
        
        # Check function input type
        hp.check_function_input_type(self.__init__, locals())
        
        # Check if activities are all Brightway activity objects
        check_activities = [m for m in activities if not isinstance(m, bw2data.backends.peewee.proxies.Activity)]
        
        # Check and raise error
        if check_activities != []:
            raise ValueError("Input variable 'activities' needs to be a list of only Activity objects.")
            
        # Check if methods are all tuples
        assert all([isinstance(m, tuple) for m in methods]), "At least one method is not of type <tuple>."
        
        # Extract all methods that are not registered in the Brightway background
        check_methods = [str(m) for m in methods if m not in bw2data.methods]
        
        # Check if all methods are registered in the Brightway background
        if check_methods != []:
            raise ValueError("They following methods are not registered:\n - " + "\n - ".join(check_methods))
        
        # Raise error if level is smaller than 1
        if exchange_level < 1:
            raise ValueError("Input variable 'exchange_level' needs to be greater than 1 but is currently '" + str(exchange_level) + "'.")
        
        # Raise error if the cut off is not between 0 and 1
        if cut_off_percentage is not None and (cut_off_percentage < 0 or cut_off_percentage > 1):
            raise ValueError("Input variable 'cut_off_percentage' needs to be between 0 and 1 but is currently '" + str(cut_off_percentage) + "'.")
        
        # Loop through each activity and construct the key tuple if not yet existing
        for activity in activities:
            
            # Check if the key is already existing
            if not hasattr(activity, "key"):
                
                # Extract key components, database and code
                database: (str | None) = activity["database"]
                code: (str | None) = activity["code"]
                
                # Raise error if they were not found
                if database is None or code is None:
                    raise ValueError("Activity key tuple could not be constructed.")
                
                # Add key attribute
                activity.key: tuple[str, str] = (database, code)
        
        # The functional unit is defined as 1
        self.functional_amount: (int | float) = functional_amount
        
        # Add to object
        self.activities: list[bw2data.backends.peewee.proxies.Activity] = activities
        self.methods: list[tuple] = methods
        self.cut_off_percentage: (int | float) = cut_off_percentage
        self.exchange_level: int = exchange_level
        self.progress_bar: bool = print_progress_bar
        
        # Get the length of the activities and methods, as well as the total
        # Create string to use for progress bar
        self.len_act: str = str(len(self.activities))
        self.len_met: str = str(len(self.methods))
        self.tot_act_met: str = str(len(self.activities) * len(self.methods))
                
        # Default booleans to say that certain functions have already been done
        # Default is False. This attributes will change if certain functions will be run.
        self.calculation_initialized: bool = False
        self.Brightway_mappings_loaded: bool = False
        self.CFs_retrieved: bool = False
        self.characterized_contribution_dicts_created: bool = False
        self.uncharacterized_contribution_dicts_created: bool = False
        
        # Initialize a new mapping instance for the production amount
        # Those mappings serve as temporary storage of already retrieved values
        # Can be accessed by the 'get...' functions
        self._temporary_key_to_production_amount_mapping: dict = {}
        
        # Definition of names to be used for the different results
        self.name_LCIA_scores: str = "LCIA_scores"
        self.name_LCIA_immediate_scores: str = "LCIA_scores_of_exchanges"
        self.name_LCI_exchanges: str = "LCI_exchanges"
        self.name_LCI_emission_contributions: str = "LCI_emission_contribution"
        self.name_LCI_process_contributions: str = "LCI_process_contribution"
        self.name_LCIA_emission_contributions: str = "LCIA_emission_contribution"
        self.name_LCIA_process_contributions: str = "LCIA_process_contribution"
        self.name_characterization_factors: str = "Characterization_factors"
        
        # Definition of names to be used for activities and flows (e.g. shown in results)
        self.key_name_flow: str = "flow"
        self.key_name_flow_name: str = self.key_name_flow + "_name"
        self.key_name_flow_unit: str = self.key_name_flow + "_unit"
        self.key_name_method: str = "method"
        self.key_name_score: str = "score"
        self.key_name_score_unit: str = self.key_name_score + "_unit"
        self.key_name_level: str = "level"
        self.key_name_activity: str = "activity"
        self.key_name_activity_database: str = self.key_name_activity + "_database"
        self.key_name_production_amount: str = "production_amount"
        self.key_name_functional_amount: str = "functional_amount"
        self.key_name_characterization_factor: str = "cf"
        self.key_name_characterization_unit: str = "cf_unit"
        
        # Create a list of tuples with all the activity - method - combinations that should be calculated
        # This will be used further below when calculating (as iterator for the progressbar)
        self.activity_method_combinations: list = [(n, m) for m in self.methods for n in self.activities]
        
        # Add an instance where results will be saved to
        self.results: dict = {}
    
    
    # Read all Brightway background data and construct mappings
    def load_Brightway_mappings(self) -> None:
        
        # Initialize empty dictionaries
        self.key_to_id_mapping: dict = {}
        self.id_to_key_mapping: dict = {}
        self.key_to_dict_mapping: dict = {}
        self.id_to_dict_mapping: dict = {}
        self.key_to_activity_mapping: dict = {}
        self.id_to_activity_mapping: dict = {}
        self.method_tuple_to_method_unit_mapping: dict = {}

        # Loop through each registered database in the Brightway background
        for db_name in bw2data.databases:
            
            # Loop through each instance in the database
            for m in bw2data.Database(db_name):
                
                # Check if the key is already existing
                if not hasattr(m, "key"):
                    
                    # Extract key components, database and code
                    database: (str | None) = m["database"]
                    code: (str | None) = m["code"]
                    
                    # Raise error if they were not found
                    if database is None or code is None:
                        raise ValueError("Activity key tuple could not be constructed.")
                    
                    # Add key attribute
                    m.key: tuple[str, str] = (database, code)
                
                # Get ID, UUID and the instance as dictionary
                _key: tuple = tuple(m.key)
                _instance: dict = m.as_dict()
                self.key_to_dict_mapping[_key]: dict = _instance
                self.key_to_activity_mapping[_key]: bw2data.backends.peewee.proxies.Activity = m
        
        # Loop through each method
        for method_name in bw2data.methods:
            
             # Extract the unit
             unit: str = bw2data.Method(method_name).metadata["unit"]

             # Write to mapping
             self.method_tuple_to_method_unit_mapping[method_name]: str = unit
            
        # Change to True because we have loaded all Brightway mappings
        self.Brightway_mappings_loaded: bool = True
    
    
    
    # Function to retrieve the production amount of an activity
    def get_production_amount(self,
                              activity: bw2data.backends.peewee.proxies.Activity) -> float:
        
        # Check function input type
        hp.check_function_input_type(self.get_production_amount, locals())
        
        # Try to retrieve first from temporary results and if successful directly return
        if self._temporary_key_to_production_amount_mapping.get(activity.key) is not None:
            
            # Directly return
            return self._temporary_key_to_production_amount_mapping[activity.key]
        
        # Directly receive the production amount from the activity, if possible
        production_amount_I: (float | int | None) = activity.get("production amount")
        
        # Check if a value has been found with the first method
        if production_amount_I is not None:
            
            # Add to temporary dictionary
            self._temporary_key_to_production_amount_mapping[activity.key]: float = float(production_amount_I)
            
            # Return
            return float(production_amount_I)
        
        # Extract the production exchange
        production_exchange: list[dict] = self.extract_production_exchange(activity)
        
        # If the second method yields an empty list, the activity is of type biosphere
        # Check if the list is empty
        if production_exchange == []:
            
            # We save the production amount as 1 value to the temporary dictionary
            self._temporary_key_to_production_amount_mapping[activity.key]: (float | int) = float(1)
            
            # We return a 1
            return float(1)
        
        else:
            # Otherwise we can only have one item in the list which is the production exchange
            # We extract the production amount from the dictionary
            production_amount_II: (float | int | None) = production_exchange[0].get("amount")
            
            # We extract the allocation
            allocation: (float | int) = production_exchange[0].get("allocation")
            
            # Check if the retrieved amount is None
            if production_amount_II is not None and allocation is not None:
                
                # If not, we were successful and write the amount to the temporary dictionary
                self._temporary_key_to_production_amount_mapping[activity.key]: float = float(production_amount_II) * float(allocation)
                
                # And we return the value
                return float(production_amount_II)
            
            else:
                # If we were unable to retrieve the amount (e.g. because there is no amount key)
                # we need to raise an error
                if production_amount_II is None:
                    raise ValueError("Production amount of activity '" + str(activity.key) + "' could not be retrieved.")
                    
                # If we were unable to retrieve the alloation (e.g. because there is no allocation key)
                # we need to raise an error
                if allocation is None:
                    raise ValueError("Allocation of activity '" + str(activity.key) + "' could not be retrieved.")
            
            
    
    
    
    # Function to extract the non production exchanges of an activity
    def extract_non_production_exchanges(self,
                                         activity: bw2data.backends.peewee.proxies.Activity) -> list[dict]:
        
        # Check function input type
        hp.check_function_input_type(self.extract_non_production_exchanges, locals())
        
        # Load BW mapping if not yet specified
        if not self.Brightway_mappings_loaded:
            self.load_Brightway_mappings()
        
        # Extract all exchanges as dictionary
        non_production_exchanges_orig: list[dict] = [n.as_dict() for n in activity.exchanges() if n["type"] != "production"]
        
        # Overwrite existing information with information from the mapping
        non_production_exchanges: list[dict] = [{**n,
                                                 **self.key_to_dict_mapping[n["input"]]} for n in non_production_exchanges_orig]
        
        # Return
        return non_production_exchanges


    
    # Function to extract production exchanges of an activity
    def extract_production_exchange(self,
                                    activity: bw2data.backends.peewee.proxies.Activity) -> list[dict]:
        
        # Check function input type
        hp.check_function_input_type(self.extract_production_exchange, locals())
        
        # Load BW mapping if not yet specified
        if not self.Brightway_mappings_loaded:
            self.load_Brightway_mappings()
            
        # Extract all exchanges as dictionary
        production_exchanges: list = [n.as_dict() for n in activity.production()]

        
        # If the list is empty, that means the activity is a biosphere flow
        # In that case we return an empty list
        if production_exchanges == []:
            return []
        
        # Raise error if more than one production exchange was identified.
        # This should never be the case!
        assert len(production_exchanges) <= 1, str(len(production_exchanges)) + " production exchange(s) found. Valid is to have exactly one production exchange."
        
        # Extract the only production exchange from the list
        production_exchange_orig: dict = production_exchanges[0]
        
        # Merge all relevant information together in a dictionary to be returned
        production_exchange: dict = {"input": activity.key,
                                     **production_exchange_orig,
                                     **self.key_to_dict_mapping[activity.key]}
        
        # Return
        return [production_exchange]
        
    
    # Function to return a dictionary of the key information of an activity
    def retrieve_activity_key_data_as_dictionary(self, activity: bw2data.backends.peewee.proxies.Activity) -> dict:
        
        # Create dictionary with the activity key information
        retrieved: dict = {"uuid": activity.key[1],
                           "database": activity.key[0]}
        
        # Add string to the keys
        retrieved_str_added: dict = self.add_str_to_dict_keys(retrieved, self.key_name_activity)
        
        # Return the dictionary
        return retrieved_str_added
    
    
    
    # !!! Newly added since the previous version was erroneous
    # Support function to extract exchanges of an activity based on a specific level
    def extract_LCI_exchanges_of_activity_at_certain_level(self,
                                                           activity: bw2data.backends.peewee.proxies.Activity,
                                                           level: int) -> list[dict]:
        
        # Check function input type
        hp.check_function_input_type(self.extract_LCI_exchanges_of_activity_at_certain_level, locals())
        
        # Load mappings if not yet done
        if not self.Brightway_mappings_loaded:
            self.load_Brightway_mappings()
        
        # Define the starting level
        current_level: int = 0

        # Initialize a new key to write exchange keys to that should be calculated in the next round
        exchanges_at_level: dict = {current_level: ((self.key_to_activity_mapping[activity.key], activity.key, self.functional_amount, 1),)}
        
        # Extract exchanges as long/as many times as there are levels
        while current_level < level:

            # Extract all the relevant keys at the current level
            current_exchanges: tuple[tuple, float] = exchanges_at_level[current_level]
            
            # Initialize a new list for the current level
            exchanges_at_level[current_level + 1]: tuple = ()
            
            # Loop through each current exchange key and amount
            for current_exchange, current_key, current_amount, respective_level in current_exchanges:
                
                # Check if the level of the current exchange matches the current level where we are, otherwise we don't need to go on and can simply move the exchange to the next round
                if respective_level < current_level:
                    
                    # Simply move the exchange to the next round without searching
                    exchanges_at_level[current_level + 1] += ((current_exchange, current_key, current_amount, respective_level),)
                    continue
                
                # First get the activity corresponding to the current exchange
                current_exchange_as_obj: bw2data.backends.peewee.proxies.Activity = self.key_to_activity_mapping[current_key]
                
                # Get the current production amount
                current_production_amount: float = float(self.get_production_amount(current_exchange_as_obj))
                
                # Extract all exchanges that come next for the current exchange
                next_exchanges: list[dict] = self.extract_non_production_exchanges(current_exchange_as_obj)
                
                # Check if we have found next exchanges
                if next_exchanges == []:
                    
                    # If we have not found any next exchanges, we simply append the current exchange to the next level to keep it
                    exchanges_at_level[current_level + 1] += ((current_exchange, current_key, current_amount, respective_level),)
                else:
                    # Append the keys and the amounts for the next level
                    exchanges_at_level[current_level + 1] += tuple([(m,
                                                                     m["input"],
                                                                     (current_amount / current_production_amount * m["amount"]) if current_production_amount != 0 else 0,
                                                                     current_level + 1) for m in next_exchanges])
                
            # Check if we arrived at the end
            if current_level >= level:
                break
                
            else:
                # Raise iterator
                current_level += 1
        
        at_level: list[dict] = [{**self.add_str_to_dict_keys(exc_dict, self.key_name_flow),
                                 self.key_name_flow + "_amount": exc_amount,
                                 self.key_name_level: exc_level} for exc_dict, exc_key, exc_amount, exc_level in exchanges_at_level[level]]
        
        # Retrieve the activity information
        activity_info: dict = self.key_to_dict_mapping[activity.key]
        
        # Add string 'activity' to the beginning of the dictionary keys
        activity_info_str_added: dict = self.add_str_to_dict_keys({**activity_info,
                                                                   "amount": self.functional_amount}, self.key_name_activity)
        
        # Merge all relevant information together in a dictionary to be returned
        compiled: list[dict] = [{**activity_info_str_added,
                                 **m} for m in at_level]
        
        return compiled
    
    
    
    
    
    # Function that does the heavy lifting. Needs only to be done once!
    # Calculates the matrices for each database and method to be reapplied further down the chain
    def _initialize_calculation(self) -> None:
        
        # Initialize new attributes to save things like lca objects to
        self.lca_objects: dict = {}
        self.characterized_inventory: dict = {}
        self.uncharacterized_inventory: dict = {}
        self.activities_per_database: dict = {}
        
        # Loop through each database used
        for database in bw2data.databases:
            
            # Extract first inventory and method to then initialise lca object
            _act: bw2data.backends.peewee.proxies.Activity = [m for m in bw2data.Database(database)][0]
            _met: tuple = self.methods[0]
            
            # Export inventory, where error appeared
            # Used for displaying additional information in the error statement
            inv_for_error = dict(_act.as_dict(), **{"exchanges": [n.as_dict() for n in [nn for nn in _act.exchanges()]]})
            
            # Filter all activities that correspond to the current database
            self.activities_per_database[database]: list = [m for m in self.activities if m["database"] == database]
            
            try:
                # LCA object generation will fail for biosphere databases
                # However, we do not need an LCA object for biosphere databases but only the characterization factors
                
                # Create the Brightway2 LCA object from the first activity and method.
                # This takes a little bit long, and therefore this step is only done once. The object will be reused in the calculation            
                lca_object = bw2calc.LCA({_act.key: self.functional_amount}, method = _met)
                
                # Calculate inventory once to load all database data
                lca_object.lci()
                
                # Keep the LU factorized matrices for faster calculations
                lca_object.decompose_technosphere()
                
                # Load method data 
                lca_object.lcia()
                
                # Append to dictionary
                self.lca_objects[database]: bw2calc.lca.LCA = lca_object
            
            # Raise error if the database is not square
            except NonsquareTechnosphere:
                
                # Raise error
                raise NonsquareTechnosphere("'code' and 'database' parameter of inventory (= 'ds') probably don't match the parameter 'input' of the production exchange (= 'exc'):\n\ninventory code: " + inv_for_error.get("code", "code parameter not existent") + "\ninventory database: " + inv_for_error.get("database", "database parameter not existent") + "\n\nproduction exchange input (first item = database, second item = code): " + str([m for m in inv_for_error.get("exchanges", {"input": "no production exchange available", "__error__": True, "type": "nothing"}) if m.get("__error__") is not None and m["type"] == "production"][0].get("input")))
            
            # If the biosphere is empty, that means that an error has occured during setting up the inventories
            # An error is raised
            except EmptyBiosphere:
                
                # Raise error
                raise EmptyBiosphere("LCA can not be done if no biosphere flows are attributed to the inventory. Problematic inventory (... and possibly others):\n\ncode: " + inv_for_error["code"] + "\ndatabase: " + inv_for_error["database"] + "\nname: " + inv_for_error["name"] + "\nlocation: " + inv_for_error["location"] + "\nunit: " + inv_for_error["unit"])
            
            # If all arrays are empty, that means that the current database is a biosphere database
            # Therefore, we do not need to build a LCA object but only save the characterization factors
            except AllArraysEmpty:
                
                # Extract the characterization factors for the used methods
                # But only if not already retrieved
                if not self.CFs_retrieved:
                    self.retrieve_CFs()
                
                # Go on
                continue
            
        # Initialize a dictionary to save calculated scores temporarily
        self._temporary_saved_scores: dict = {}
        
        # Change boolean to state that the calculation has already been initialized
        self.calculation_initialized: bool = True
    
    
    
    # Get the characterized activity and biosphere dictionaries
    # Important to calculate the LCIA process and emission contribution
    def _get_characterized_contribution_dicts(self) -> None:
        
        # Initialize calculation if not yet done
        if not self.calculation_initialized:
            self._initialize_calculation()
        
        # Initialize new attributes
        self.characterized_activity_dicts: dict = {}
        self.characterized_biosphere_dicts: dict = {}
        self.characterized_activity_series: dict = {}
        self.characterized_biosphere_series: dict = {}
        
        # Loop through each database used
        for database in bw2data.databases:
            
            # Loop through each method
            for method in self.methods:
                
                # For the biosphere, it will fail to retrieve the lca object because there won't be any
                # However, that is not problematic. If we fail to retrieve, we go on
                try:
                    # Retrieve already prebuilt lca object
                    lca_object: bw2calc.lca.LCA = self.lca_objects[database]
                    
                    # Use the same LCA object but switch the method
                    lca_object.switch_method(method)
                    
                    # Extract the biosphere and activity dictionaries
                    self.characterized_activity_dicts[(database, method)]: bw2calc.dictionary_manager.ReversibleRemappableDictionary = copy.deepcopy(lca_object.activity_dict)
                    self.characterized_biosphere_dicts[(database, method)]: bw2calc.dictionary_manager.ReversibleRemappableDictionary = copy.deepcopy(lca_object.biosphere_dict)
                    
                    # Create a pandas Series of the mapped dicts
                    self.characterized_activity_series[(database, method)]: pd.Series = pd.Series(self.characterized_activity_dicts[(database, method)].keys(), index = range(len(self.characterized_activity_dicts[(database, method)])), name = "flows").map(self.key_to_dict_mapping)
                    self.characterized_biosphere_series[(database, method)]: pd.Series = pd.Series(self.characterized_biosphere_dicts[(database, method)].keys(), index = range(len(self.characterized_biosphere_dicts[(database, method)])), name = "flows").map(self.key_to_dict_mapping)
                    
                    
                # If we encounter a key error, it is not problematic. We just go to the next method
                except KeyError:
                    continue
                
                # However, if there are other errors, we need to catch them
                except Exception as e:
                    raise ValueError("An error ocurred while loading the characterized contribution dictionaries for database '" + str(database) + "' and method '" + str(method) + "'. The following error message appeared:\n\n'" + str(e) + "'")
                    
        # Change boolean to True, indicating that we have created the contribution dictionaries
        self.characterized_contribution_dicts_created: bool = True
    
    
    
    
    # Get the uncharacterized activity and biosphere dictionaries
    # Important to calculate the LCI process and emission contribution
    def _get_uncharacterized_contribution_dicts(self) -> None:
        
        # Initialize calculation if not yet done
        if not self.calculation_initialized:
            self._initialize_calculation()
        
        # Initialize new attributes
        self.uncharacterized_activity_dicts: dict = {}
        self.uncharacterized_biosphere_dicts: dict = {}
        self.uncharacterized_activity_series: dict = {}
        self.uncharacterized_biosphere_series: dict = {}
        
        # Loop through each database used
        for database in bw2data.databases:
                
                # For the biosphere, it will fail to retrieve the lca object because there won't be any
                # However, that is not problematic. If we fail to retrieve, we go on
                try:
                    # Retrieve already prebuilt lca object
                    lca_object: bw2calc.lca.LCA = self.lca_objects[database]
                    
                    # Extract the biosphere and activity dictionaries
                    self.uncharacterized_activity_dicts[database]: bw2calc.dictionary_manager.ReversibleRemappableDictionary = copy.deepcopy(lca_object.activity_dict)
                    self.uncharacterized_biosphere_dicts[database]: bw2calc.dictionary_manager.ReversibleRemappableDictionary = copy.deepcopy(lca_object.biosphere_dict)
                    
                    # Create a pandas Series of the mapped dicts
                    self.uncharacterized_activity_series[database]: pd.Series = pd.Series(self.uncharacterized_activity_dicts[database].keys(), index = range(len(self.uncharacterized_activity_dicts[database])), name = "flows").map(self.key_to_dict_mapping)
                    self.uncharacterized_biosphere_series[database]: pd.Series = pd.Series(self.uncharacterized_biosphere_dicts[database].keys(), index = range(len(self.uncharacterized_biosphere_dicts[database])), name = "flows").map(self.key_to_dict_mapping)
                    
                # If we encounter a key error, it is not problematic. We just go to the next method
                except KeyError:
                    continue
                
                # However, if there are other errors, we need to catch them
                except Exception as e:
                    raise ValueError("An error ocurred while loading the uncharacterized contribution dictionaries for database '" + str(database) + "'. The following error message appeared:\n\n'" + str(e) + "'")
                    
        # Change boolean to True, indicating that we have created the contribution dictionaries
        self.uncharacterized_contribution_dicts_created: bool = True
            
        

    # Function to get the characterized inventory matrices
    # Important to calculate the LCI process and emission contribution
    def _get_characterized_inventory_matrices(self, activity: bw2data.backends.peewee.proxies.Activity) -> None:
        
        # Check function input type
        hp.check_function_input_type(self._get_characterized_inventory_matrices, locals())
        
        # Initialize calculation if not yet done
        if not self.calculation_initialized:
            self._initialize_calculation()
        
        # Return the activity key data in dictionary format
        activity_key_data: dict = self.retrieve_activity_key_data_as_dictionary(activity)
        
        # Retrieve the activity database
        database: str = activity_key_data[self.key_name_activity_database]
        
        # Loop through each method
        for method in self.methods:
            
            # Check if matrix has already been built
            if self.characterized_inventory.get((activity.key, method)) is None:
                
                # If not, calculate and add
                # Redo the life cycle inventory and the life cycle impact assessment for the current method and activity
                _redo = self.lca_objects[database]
                _redo.redo_lci({activity.key: self.functional_amount})
                _redo.switch_method(method) # !!! IMPORTANT, was forgotten previously, is this step efficient? Or should I do redo_lcia?
                _redo.lcia()
                
                # Extract the characterized matrix and save to the attribute
                self.characterized_inventory[(activity.key, method)]: scipy.sparse._csr.csr_matrix = _redo.characterized_inventory.copy()
    
    
    
    
    # Function to get the uncharacterized inventory matrices
    # Important to calculate the LCI process and emission contribution
    def _get_uncharacterized_inventory_matrices(self, activity: bw2data.backends.peewee.proxies.Activity) -> None:
        
        # Check function input type
        hp.check_function_input_type(self._get_uncharacterized_inventory_matrices, locals())
        
        # Initialize calculation if not yet done
        if not self.calculation_initialized:
            self._initialize_calculation()
        
        # Return the activity key data in dictionary format
        activity_key_data: dict = self.retrieve_activity_key_data_as_dictionary(activity)
        
        # Retrieve the activity database
        database: str = activity_key_data[self.key_name_activity_database]

        # Check if matrix has already been built
        if self.uncharacterized_inventory.get(activity.key) is None:
                
            # If not, calculate and add
            # Redo the life cycle inventory and the life cycle impact assessment for the activity
            _redo = self.lca_objects[database]
            _redo.redo_lci({activity.key: self.functional_amount})
                
            # Extract the uncharacterized matrix and save to the attribute
            self.uncharacterized_inventory[activity.key]: scipy.sparse._csr.csr_matrix = _redo.inventory.copy()
    
    
    
    
    # Function to calculate the LCIA score of a method - activity combination
    def calculate_LCIA_score(self,
                             activity_key: tuple,
                             method: tuple,
                             amount: (int | float)) -> dict:
        
        # Check function input type
        hp.check_function_input_type(self.calculate_LCIA_score, locals())
        
        # Load BW mapping if not yet specified
        if not self.Brightway_mappings_loaded:
            self.load_Brightway_mappings()
            
        # Retrieve CF's if not yet retrieved
        if not self.CFs_retrieved:
            self.retrieve_CFs()
        
        # Initialize calculation if not yet done
        if not self.calculation_initialized:
            self._initialize_calculation()
        
        # Retrieve the activity
        activity: bw2data.backends.peewee.proxies.Activity = self.key_to_activity_mapping[activity_key]
        
        # Check if score has already been calculated
        if self._temporary_saved_scores.get((activity_key, method)) is not None:
            
            # Simply retrieve score
            score_per_1_amount: (int | float) = self._temporary_saved_scores[(activity_key, method)]
            
        else:
            # Otherwise calculate freshly
            # First, we try to calculate via redoing the lci. This works for activities.
            try:
                
                # Make sure that matrices are available
                self._get_characterized_inventory_matrices(activity)
                
                # The score is the matrix sum of the characterized inventory
                score_per_functional_amount: (int | float) = self.characterized_inventory[(activity_key, method)].sum()
                
                # Since the calculation is always referencing to the functional amount, we need to specify a multiplier so that we have the score for 1 amount
                multiplier_to_get_score_at_1_amount: (int | float) = (1 / self.functional_amount) if self.functional_amount !=  0 else 0
                
                # Get score per 1 amount
                score_per_1_amount: (int | float) = score_per_functional_amount * multiplier_to_get_score_at_1_amount
                
                
            # If there is no characterized inventory available, that means the current activity is of type biosphere
            # As a result from redoing the lci, we will get a key error. If that happens, we retrieve the CF and calculate manually
            except KeyError:
                
                # Get the characterization factor of the biosphere flow of the current method
                # In that case, the cf is also the score, that's why we call it score here
                # If the cf is not specified for the method, that means the biosphere flow is not characterized by the method. In that case we return a 0.
                score_per_1_amount: (int | float) = self.method_and_key_to_CF_mapping[method].get(activity_key, 0)
                
            except:
                # If another error is raised, we need to abort since the calculation can not be done
                # Not sure what could be reasons for that?
                raise ValueError("Calculation of score for " + str(activity.key) + " and " + str(method) + " failed.")
                
            # Add calculated score to temporary dictionary in case the result should be used again (for faster calculation)
            # Important: the score is always valid for the amount of 1
            self._temporary_saved_scores[(activity_key, method)]: (float | int) = score_per_1_amount
        
        # Retrieve the activity information
        activity_info: dict = self.key_to_dict_mapping[activity_key]
        
        # Add string 'activity' to the beginning of the dictionary keys
        activity_info_str_added: dict = self.add_str_to_dict_keys(activity_info, self.key_name_activity)
        
        # Merge all relevant information together in a dictionary to be returned
        compiled: dict = {**self.retrieve_activity_key_data_as_dictionary(activity),
                          self.key_name_method: method,
                          self.key_name_score: score_per_1_amount * amount,
                          self.key_name_score_unit: self.method_tuple_to_method_unit_mapping[method],
                          self.key_name_functional_amount: self.functional_amount,
                          **activity_info_str_added}
        # Return
        return compiled
    
    
    
        
    # Calculate all the LCIA scores for the inventories and methods
    def calculate_LCIA_scores(self) -> None:
        
        # Save time when calculation starts
        if self.progress_bar:
            start: datetime.datetime = datetime.datetime.now()
        
        # Retrieve the combinations that should be calculated
        activity_method_combinations: list[tuple] = self.activity_method_combinations
        
        # Check if progress bar should be printed to console
        if self.progress_bar:
            
            # Wrap progress bar around iterator
            activity_method_combinations: list[tuple] = hp.progressbar(activity_method_combinations, prefix = "\nCalculate LCIA scores of activities ...")
        
        # Initialize new dictionary to save the LCIA score results to
        self.results[self.name_LCIA_scores]: list = []
        
        # Loop through each method and activity and calculate the LCIA scores
        for activity, method in activity_method_combinations:
        
            # Apply function to calculate the LCIA score of the current activity-method combination
            score_dict: dict = self.calculate_LCIA_score(activity.key, method, self.functional_amount)
            
            # Add to results
            self.results[self.name_LCIA_scores] += [score_dict]
            
        # Print summary statement
        if self.progress_bar:
            print("  - Calculation of " + self.tot_act_met + " LCIA score(s) (from " + self.len_act + " activity/ies & " + self.len_met + " method(s)) took " + self.convert_timedelta(datetime.datetime.now() - start))
    

    

    # Function to extract the exchanges for each of the activity
    def extract_LCI_exchanges(self, exchange_level: (int | None) = None, _return: bool = False):
        
        # Check function input type
        hp.check_function_input_type(self.extract_LCI_exchanges, locals())
        
        # Save time when calculation starts
        if self.progress_bar:
            start: datetime.datetime = datetime.datetime.now()
        
        # Select the exchange level, either directly provided by this function or from the init
        self.level_of_LCI_exchanges: int = self.exchange_level if exchange_level is None else exchange_level
        
        # Raise error if level is smaller than 1
        if self.level_of_LCI_exchanges < 1:
            raise ValueError("Input variable 'exchange_level' needs to be greater than 1 but is currently '" + str(self.level_of_LCI_exchanges) + "'.")
        
        # Initialize new dictionary to save the LCIA score results to
        extracted_LCI_exchanges: list = []
        
        # Get activities where exchanges should be extracted from and wrap progress bar around it
        activities: list = hp.progressbar(self.activities, prefix = "\nExtract LCI exchanges (level " + str(self.level_of_LCI_exchanges) + ") ...") if self.progress_bar else self.activities
        
        # Loop through each activity
        for act in activities:
            
            # Extract the exchanges at a certain level
            exchanges_of_activity_at_certain_level: list[dict] = self.extract_LCI_exchanges_of_activity_at_certain_level(act, self.level_of_LCI_exchanges)
            
            # Write to results dictionary
            extracted_LCI_exchanges += exchanges_of_activity_at_certain_level
          
        # Print summary statement
        if self.progress_bar:
            print("  - Extraction of " + str(len(extracted_LCI_exchanges)) + " LCI exchange(s) at level " + str(self.level_of_LCI_exchanges) + " (from " + self.len_act + " activity/ies) took " + self.convert_timedelta(datetime.datetime.now() - start))
    
        # Check if the results should be returned
        if not _return:
            
            # Write to or update the result attribute
            self.results[self.name_LCI_exchanges] = extracted_LCI_exchanges
            
            # Return a None
            return None
        
        else:
            # If we simply want to return, we do return the list with the dictionaries
            # In that case, we do not update the results attribute!
            return extracted_LCI_exchanges
        
        
    
    
    # Function to calculate the emission contribution of activities
    def extract_LCI_emission_contribution(self) -> None:
        
        # Check function input type
        hp.check_function_input_type(self.extract_LCI_emission_contribution, locals())
        
        # Save time when calculation starts
        if self.progress_bar:
            start: datetime.datetime = datetime.datetime.now()
        
        # Write results to results instance
        self.results[self.name_LCI_emission_contributions]: list = []
        
        # Get activities where emission contribution should be extracted from and wrap progress bar around it
        activities: list = hp.progressbar(self.activities, prefix = "\nExtract LCI emission contribution ...") if self.progress_bar else self.activities
        
        # Loop through each activity
        for act in activities:
                
            # Calculate the emission contribution
            emission_contributions: list[dict] = self.extract_biosphere_contribution(act.key)
                
            # Write results to results instance
            self.results[self.name_LCI_emission_contributions] += emission_contributions
            
        # Print summary statement
        if self.progress_bar:
            print("  - Extraction of " + str(len(self.results[self.name_LCI_emission_contributions])) + " LCI emission contribution(s) (from " + self.len_act + " activity/ies) took " + self.convert_timedelta(datetime.datetime.now() - start))

    
    
    
    # Function to calculate the process contribution of activities
    def extract_LCI_process_contribution(self) -> None:
        
        # Check function input type
        hp.check_function_input_type(self.extract_LCI_process_contribution, locals())
        
        # Save time when calculation starts
        if self.progress_bar:
            start: datetime.datetime = datetime.datetime.now()
        
        # Write results to results instance
        self.results[self.name_LCI_process_contributions]: list = []
        
        # Get activities where process contribution should be extracted from and wrap progress bar around it
        activities: list = hp.progressbar(self.activities, prefix = "\nExtract LCI process contribution ...") if self.progress_bar else self.activities
        
        # Loop through each activity
        for act in activities:
                
            # Calculate the process contribution
            process_contributions: list[dict] = self.extract_activity_contribution(act.key)
                
            # Write results to results instance
            self.results[self.name_LCI_process_contributions] += process_contributions
            
        # Print summary statement
        if self.progress_bar:
            print("  - Extraction of " + str(len(self.results[self.name_LCI_process_contributions])) + " LCI process contribution(s) (from " + self.len_act + " activity/ies) took " + self.convert_timedelta(datetime.datetime.now() - start))

    
    
    
    # Calculate the LCIA scores of exchanges of activities
    def calculate_LCIA_scores_of_exchanges(self, exchange_level: (int | None) = None) -> None:
        
        # Check function input type
        hp.check_function_input_type(self.calculate_LCIA_scores_of_exchanges, locals())
        
        # Save time when calculation starts
        if self.progress_bar:
            start: datetime.datetime = datetime.datetime.now()
        
        # Select the exchange level, either directly provided by this function or from the init
        self.level_of_LCIA_exchanges: int = self.exchange_level if exchange_level is None else exchange_level
        
        # Raise error if level is smaller than 1
        if self.level_of_LCIA_exchanges < 1:
            raise ValueError("Input variable 'exchange_level' needs to be greater than 1 but is currently '" + str(self.level_of_LCIA_exchanges) + "'.")
        
        # If exchanges have not yet been extracted (= None), extract them first at the current level
        if self.results.get(self.name_LCI_exchanges) is None:
            extracted_LCI_exchanges: list[dict] = self.extract_LCI_exchanges(exchange_level = self.level_of_LCIA_exchanges,
                                                                             _return = True)
            
        elif self.results.get(self.name_LCI_exchanges) is not None and (self.level_of_LCIA_exchanges == self.level_of_LCI_exchanges):
            extracted_LCI_exchanges: list[dict] = copy.deepcopy(self.results[self.name_LCI_exchanges])
            
        else:
            extracted_LCI_exchanges: list[dict] = self.extract_LCI_exchanges(exchange_level = self.level_of_LCIA_exchanges,
                                                                             _return = True)

        # Get all the combinations that should be calculated
        method_exc_combinations: list = [(m, n) for m in self.methods for n in extracted_LCI_exchanges]
        
        # Check if progress bar should be printed to console
        if self.progress_bar:
            
            # Wrap progress bar around iterator
            method_exc_combinations: list[dict] = hp.progressbar(method_exc_combinations, prefix = "\nExtract LCIA scores of exchanges (level " + str(self.level_of_LCIA_exchanges) + ") ...")
          
        # Initialize new results list
        self.results[self.name_LCIA_immediate_scores]: list = []
        
        # Loop through each method, exchange key and exchange amount combination that should be calculated
        for method, exc in method_exc_combinations:
            
            # Calculate the score of the exchange
            score_dict: dict = self.calculate_LCIA_score(exc[self.key_name_flow + "_input"],
                                                         method,
                                                         exc[self.key_name_flow + "_amount"])
            # Add to results
            self.results[self.name_LCIA_immediate_scores] += [{**exc,
                                                              self.key_name_score: score_dict[self.key_name_score],
                                                              self.key_name_score_unit: score_dict[self.key_name_score_unit],
                                                              self.key_name_method: method}]
            
        # Print summary statement
        if self.progress_bar:
            print("  - Calculation of " + str(len(self.results[self.name_LCIA_immediate_scores])) + " LCIA score(s) (from " + str(len(extracted_LCI_exchanges)) + " exchange(s) at level " + str(self.level_of_LCIA_exchanges) + " & " + self.len_met + " method(s)) took " + self.convert_timedelta(datetime.datetime.now() - start))
        
    
    
    
    # Function to calculate the emission contribution of activities
    def calculate_LCIA_emission_contribution(self, cut_off_percentage: (None | int | float) = None) -> None:
        
        # Check function input type
        hp.check_function_input_type(self.calculate_LCIA_emission_contribution, locals())
        
        # Save time when calculation starts
        if self.progress_bar:
            start: datetime.datetime = datetime.datetime.now()
        
        # Retrieve the combinations that should be calculated
        activity_method_combinations: list[tuple] = self.activity_method_combinations
        
        # Check if progress bar should be printed to console
        if self.progress_bar:
            
            # Wrap progress bar around iterator
            activity_method_combinations: list[tuple] = hp.progressbar(activity_method_combinations, prefix = "\nCalculate LCIA emission contribution ...")
        
        # Write results to results instance
        self.results[self.name_LCIA_emission_contributions]: list = []
        
        # Loop through each method and activity for which the emission contribution should be calculated
        for act, method in activity_method_combinations:
                
            # Calculate the emission contribution
            emission_contributions: list[dict] = self.calculate_emission_contribution(act.key, method, cut_off_percentage)
                
            # Write results to results instance
            self.results[self.name_LCIA_emission_contributions] += emission_contributions
            
        # Print summary statement
        if self.progress_bar:
            print("  - Calculation of " + str(len(self.results[self.name_LCIA_emission_contributions])) + " LCIA emission contribution(s) (from " + self.len_act + " activity/ies & " + self.len_met + " method(s)) took " + self.convert_timedelta(datetime.datetime.now() - start))

    


    # Function to calculate the process contribution of activities
    def calculate_LCIA_process_contribution(self, cut_off_percentage: (None | int | float) = None) -> None:
        
        # Check function input type
        hp.check_function_input_type(self.calculate_LCIA_process_contribution, locals())
        
        # Save time when calculation starts
        if self.progress_bar:
            start: datetime.datetime = datetime.datetime.now()
        
        # Retrieve the combinations that should be calculated
        activity_method_combinations: list[tuple] = self.activity_method_combinations
        
        # Check if progress bar should be printed to console
        if self.progress_bar:
            
            # Wrap progress bar around iterator
            activity_method_combinations: list[tuple] = hp.progressbar(activity_method_combinations, prefix = "\nCalculate LCIA process contribution ...")
        
        # Write results to results instance
        self.results[self.name_LCIA_process_contributions]: list = []
        
        # Loop through each method and activity for which the process contribution should be calculated
        for act, method in activity_method_combinations:
                
            # Calculate the process contribution
            process_contributions: list[dict] = self.calculate_process_contribution(act.key, method, cut_off_percentage)
                
            # Write results to results instance
            self.results[self.name_LCIA_process_contributions] += process_contributions
            
        # Print summary statement
        if self.progress_bar:
            print("  - Calculation of " + str(len(self.results[self.name_LCIA_process_contributions])) + " LCIA process contribution(s) (from " + self.len_act + " activity/ies & " + self.len_met + " method(s)) took " + self.convert_timedelta(datetime.datetime.now() - start))




    # Function to convert time delta in a readable, nice string
    def convert_timedelta(self, timedelta: datetime.timedelta) -> str:
        
        # Check function input type
        hp.check_function_input_type(self.convert_timedelta, locals())
        
        # Get total days and total seconds
        days, seconds = timedelta.days, timedelta.seconds
        
        # Convert to hours, minutes and seconds
        hours = days * 24 + seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = (seconds % 60)
        
        # Create string
        if hours > 0:
            string: str = str(hours) + " hour(s) and " + str(minutes) + " minute(s)"
            
        elif minutes > 0:
            string: str = str(minutes) + " minute(s) and " + str(seconds) + " second(s)"
            
        elif seconds > 0:
            string: str = str(seconds) + " second(s)"
            
        else:
            string: str = "less than a second"
            
        # Return string
        return string
                
    
    
    # Function to apply a string to the beginning of dictionary keys
    def add_str_to_dict_keys(self, dct: dict, str_to_add: str) -> dict:
        
        # Check function input type
        hp.check_function_input_type(self.add_str_to_dict_keys, locals())
        
        # Check if all keys are of type string, otherwise it is not possible to add strings
        all_dct_keys_are_strings: bool = all([isinstance(m, str) for m in dct.keys()])
        
        # Raise error if one or all keys are not of type string
        if not all_dct_keys_are_strings:
            raise ValueError("Can not add string '" + str_to_add + "' to the beginning of dictionary keys of dictionary\n\n" + str(dct) + "\n\n ... because not all dictionary keys are strings.")
        
        # Add strings
        dct_added: dict = {(str_to_add + "_" + k): v for k, v in dct.items()}
        
        # Return
        return dct_added
    
    
    
    # Function to retrieve characterization factors from Brighway background
    def retrieve_CFs(self) -> None:
        
        # Load the methods metadata
        metadata: dict = {(k, v["unit"]): v for k, v in bw2data.methods.deserialize().items() if k in self.methods}
        loaded_methods: dict = {(m[0], m[1]): [(n[0], n[1]) for n in bw2data.Method(m[0]).load()] for m in list(metadata.keys())}
        
        # Initialize list to store the cf's to
        self.cfs_list: list[dict] = []
        self.method_and_key_to_CF_mapping: dict = {}
        
        # Load BW mapping if not yet specified
        if not self.Brightway_mappings_loaded:
            self.load_Brightway_mappings()
        
        # Loop through each method
        for (method_name, method_unit), flow_list in loaded_methods.items():
            
            # Loop through each flow and extract the information
            for flow_key, cf in flow_list:
                
                # Map the UUID to the dictionary containing information about the flow
                flow_data: dict = self.key_to_dict_mapping[flow_key]
                
                # Construct the characterization unit from the method and flow unit
                cf_unit: str = method_unit + " / " + flow_data["unit"]
                
                # Add characterization factor to the list
                self.cfs_list += [{**self.add_str_to_dict_keys(flow_data, self.key_name_flow),
                                   **{self.key_name_flow + "_uuid": flow_key[1],
                                      self.key_name_flow + "_database": flow_key[0],
                                      self.key_name_characterization_factor: cf,
                                      self.key_name_characterization_unit: cf_unit,
                                      self.key_name_method: method_name}}]
                
                # Construct mapping
                # Fist check, if method name has already been registered to store the CF's to
                if method_name not in self.method_and_key_to_CF_mapping:
                    
                    # Initialize new key with the method name if not yet there
                    self.method_and_key_to_CF_mapping[method_name]: dict = {}
                    
                # Add uuid to CF mapping
                self.method_and_key_to_CF_mapping[method_name][flow_key]: dict = cf
                
        # Change to True because we have retrieved the CF's
        self.CFs_retrieved: bool = True
    
    
    
    
    
    # Function to write the characterization factors to the results
    def extract_characterization_factors(self) -> None:
        
        # Retrieve characterization factors if not yet retrieved
        if not self.CFs_retrieved:
            self.retrieve_CFs()
        
        # Add list to the results
        self.results[self.name_characterization_factors]: list[dict] = self.cfs_list
    
    
    
    
    # A function that removes rows from a dataframe and summarizes them into a "rest" row
    def apply_cut_off(self,
                      df: pd.DataFrame,
                      column_name_rest: str,
                      column_name_for_filtering: str,
                      cut_off_percentage: (float | None) = None) -> pd.DataFrame:
        
        # Check if cut off is provided
        if cut_off_percentage is not None:
            
            # Cut off percentage needs to be between 0 and 1. If this is not the case, raise error
            if cut_off_percentage > 1 or cut_off_percentage < 0:
                raise ValueError("Cut-off percentage provided '" + str(cut_off_percentage) + "' is invalid. It needs to be between 0 and 1.")
        
            # First identify the value, which defines whether data should be cut off or not. We do this individually for negative and positive values
            min_cut_off_value: float = float(sum(df[df[column_name_for_filtering] < 0][column_name_for_filtering])) * cut_off_percentage
            max_cut_off_value: float = float(sum(df[df[column_name_for_filtering] > 0][column_name_for_filtering])) * cut_off_percentage
            
            # Filter and sort the emissions -> everything exceeding the cut off will be removed
            df_contributions_min: pd.DataFrame = df[df[column_name_for_filtering] < min_cut_off_value].sort_values([column_name_for_filtering])
            df_contributions_max: pd.DataFrame = df[df[column_name_for_filtering] > max_cut_off_value].sort_values([column_name_for_filtering])
            
            # Compile the rest quantity that was excluded -> sum all the amounts that were filtered out before and combine to one entry
            rest_quantity: float = float(sum(df[(df[column_name_for_filtering] >= min_cut_off_value) & (df[column_name_for_filtering] <= max_cut_off_value)][column_name_for_filtering]))
            rest: pd.DataFrame = pd.DataFrame({column_name_rest: ["Rest"], column_name_for_filtering: [rest_quantity]}) if cut_off_percentage < 1 and cut_off_percentage > 0 else pd.DataFrame({})
        
            # Merge the three dataframes together
            df_contributions_merged: pd.DataFrame = pd.concat([df_contributions_max, df_contributions_min, rest], axis = 0)
            
            # Return dataframe with applied cut off
            return df_contributions_merged
        
        else:
            # Return initial dataframe without any changes
            return df
        
    
    
    
    
    # A default function acting as supporter for the contribution functions
    def _default_calculate_contribution_function(self,
                                                 activity_key: tuple,
                                                 method: (None | tuple),
                                                 calculated: numpy.matrix,
                                                 biosphere_or_activity_series: pd.Series,
                                                 cut_off_percentage: (float | None)) -> list[dict]:
        
        # Check function input type
        hp.check_function_input_type(self._default_calculate_contribution_function, locals())

        # Retrieve the activity
        activity: bw2data.backends.peewee.proxies.Activity = self.key_to_activity_mapping[activity_key]
        
        # Retrieve the activity as dictionary
        activity_as_dict: dict = self.key_to_dict_mapping[activity_key]
        
        # Add string to activity dictionary keys
        activity_as_dict_str_added: dict = self.add_str_to_dict_keys(activity_as_dict, self.key_name_activity)

        # Extract all results from the characterized matrix and sum all the columns to get only one column with the total result
        contribution_results_orig: pd.Series = pd.Series(pd.DataFrame(calculated)[0], name = self.key_name_score)
        
        # Merge the results with the mapped series
        contribution_results_merged: pd.DataFrame = pd.concat([contribution_results_orig, biosphere_or_activity_series], axis = 1)

        # Remove the entries which have a 0 as an amount
        contribution_results_0_removed: pd.DataFrame = contribution_results_merged[(contribution_results_merged.score > 0) | (contribution_results_merged.score < 0)]

        # Check if a cut off should be applied
        if cut_off_percentage is None:
            
            # If no cut-off should be applied, simply use the orig dataframe and convert to list of dictionaries
            contribution_results: list[dict] = contribution_results_0_removed.to_dict("records")
            
        else:
            # Apply the cut off function to remove lines that exceed the cut off
            contribution_results_cutoff_applied: pd.DataFrame = self.apply_cut_off(df = contribution_results_0_removed,
                                                                                   column_name_rest = self.key_name_flow_name,
                                                                                   column_name_for_filtering = self.key_name_score,
                                                                                   cut_off_percentage = cut_off_percentage)
            
            # If a cutoff is applied, a 'rest' will be calculated that does have a NaN for 'flows'
            # We need to fill that NaN with an string
            contribution_results_cutoff_applied_cleaned: pd.DataFrame = contribution_results_cutoff_applied.fillna("")
            
            # Convert the dataframe to a list of dictionaries
            contribution_results: list[dict] = contribution_results_cutoff_applied_cleaned.to_dict("records")
        
        # Summarise all the different data into one dictionary
        contribution_results_added: list[dict] = [{(self.key_name_score_unit if method is not None else self.key_name_flow_unit): self.method_tuple_to_method_unit_mapping[method] if method is not None else None,
                                                   self.key_name_method: method,
                                                   self.key_name_functional_amount: self.functional_amount,
                                                   self.key_name_flow_name: m.get(self.key_name_flow_name),
                                                   **(self.add_str_to_dict_keys(m["flows"], self.key_name_flow) if m["flows"] != "" else {}),
                                                   (self.key_name_score if method is not None else self.key_name_flow + "_amount"): m[self.key_name_score],
                                                   **activity_as_dict_str_added} for m in contribution_results]
        
        # Return the list of dictionaries
        return contribution_results_added
    




    # Function to export the emission contribution of a characterized inventory
    def calculate_emission_contribution(self,
                                        activity_key: tuple,
                                        method: tuple,
                                        cut_off_percentage: (float | None) = None) -> list[dict]:
        
        # Check function input type
        hp.check_function_input_type(self.calculate_emission_contribution, locals())
        
        # Load Brightway mappings if not yet loaded
        if not self.Brightway_mappings_loaded:
            self.load_Brightway_mappings()
        
        # Get characterized contribution dictionaries if not yet created
        if not self.characterized_contribution_dicts_created:
            self._get_characterized_contribution_dicts()
        
        # Retrieve the activity
        activity: bw2data.backends.peewee.proxies.Activity = self.key_to_activity_mapping[activity_key]
        
        # Check if a characterized inventory already exists for the activity
        if self.characterized_inventory.get((activity_key, method)) is None:

            # Get the inventory matrices in case they have not yet been built
            self._get_characterized_inventory_matrices(activity)
        
        # Retrieve the key data of the dictionary in dictionary format
        activity_key_data: dict = self.retrieve_activity_key_data_as_dictionary(activity)
        
        # Extract the database of the current activity
        database: str = activity_key_data[self.key_name_activity_database]
        
        # Retrieve the characterized inventory
        characterized_inventory: scipy.sparse._csr.csr_matrix = self.characterized_inventory[(activity_key, method)]
        
        # We are only interested in the emissions, therefore we sum all the columns (activities) to have one item per row (emissions)
        calculated: numpy.matrix = characterized_inventory.sum(axis = 1)
        
        # Retrieve the characterized biosphere series
        characterized_biosphere_series: pd.Series = self.characterized_biosphere_series[(database, method)]
        
        # Calculate the emission contribution with the default function
        emission_contribution: list[dict] = self._default_calculate_contribution_function(activity_key = activity_key,
                                                                                          method = method,
                                                                                          calculated = calculated,
                                                                                          biosphere_or_activity_series = characterized_biosphere_series,
                                                                                          cut_off_percentage = cut_off_percentage)
        # Return the list of dictionaries
        return emission_contribution

    



    # Function to export the process contribution of a characterized inventory
    def calculate_process_contribution(self,
                                       activity_key: tuple,
                                       method: tuple,
                                       cut_off_percentage: (float | None) = None) -> list[dict]:
        
        # Check function input type
        hp.check_function_input_type(self.calculate_process_contribution, locals())
        
        ## Load Brightway mappings if not yet loaded
        if not self.Brightway_mappings_loaded:
            self.load_Brightway_mappings()
        
        # Get characterized contribution dictionaries if not yet created
        if not self.characterized_contribution_dicts_created:
            self._get_characterized_contribution_dicts()
        
        # Retrieve the activity
        activity: bw2data.backends.peewee.proxies.Activity = self.key_to_activity_mapping[activity_key]
        
        # Check if a characterized inventory already exists for the activity
        if self.characterized_inventory.get((activity_key, method)) is None:

            # Get the inventory matrices in case they have not yet been built
            self._get_characterized_inventory_matrices(activity)
        
        # Retrieve the key data of the dictionary in dictionary format
        activity_key_data: dict = self.retrieve_activity_key_data_as_dictionary(activity)
        
        # Extract the database of the current activity
        database: str = activity_key_data[self.key_name_activity_database]
        
        # Retrieve the characterized inventory
        characterized_inventory: scipy.sparse._csr.csr_matrix = self.characterized_inventory[(activity_key, method)]
        
        # We are only interested in the characterized processes/activities, therefore we first transpose the matrix (to have all activities in the rows) and then sum all the columns (emissions) to have one item per row (processes)
        calculated: numpy.matrix = characterized_inventory.transpose().sum(axis = 1)
        
        # Retrieve the activity series
        characterized_activity_series: pd.Series = self.characterized_activity_series[(database, method)]
        
        # Calculate the process contribution with the default function
        process_contribution: list[dict] = self._default_calculate_contribution_function(activity_key = activity_key,
                                                                                         method = method,
                                                                                         calculated = calculated,
                                                                                         biosphere_or_activity_series = characterized_activity_series,
                                                                                         cut_off_percentage = cut_off_percentage)
        # Return the list of dictionaries
        return process_contribution
    


    


    # Function to extract the biosphere contribution of an uncharacterized inventory
    def extract_biosphere_contribution(self,
                                       activity_key: tuple) -> list[dict]:
        
        # Check function input type
        hp.check_function_input_type(self.extract_biosphere_contribution, locals())
        
        ## Load Brightway mappings if not yet loaded
        if not self.Brightway_mappings_loaded:
            self.load_Brightway_mappings()
        
        # Get uncharacterized contribution dictionaries if not yet created
        if not self.uncharacterized_contribution_dicts_created:
            self._get_uncharacterized_contribution_dicts()
        
        # Retrieve the activity
        activity: bw2data.backends.peewee.proxies.Activity = self.key_to_activity_mapping[activity_key]
        
        # Check if a uncharacterized inventory already exists for the activity
        if self.uncharacterized_inventory.get(activity_key) is None:

            # Get the uncharacterized inventory matrices in case they have not yet been built
            self._get_uncharacterized_inventory_matrices(activity)
        
        # Retrieve the key data of the dictionary in dictionary format
        activity_key_data: dict = self.retrieve_activity_key_data_as_dictionary(activity)
        
        # Extract the database of the current activity
        database: str = activity_key_data[self.key_name_activity_database]
        
        # Retrieve the uncharacterized inventory
        uncharacterized_inventory: scipy.sparse._csr.csr_matrix = self.uncharacterized_inventory[activity_key]
        
        # We are only interested in the uncharacterized emissions, therefore we sum all the columns (activities) to have one item per row (emissions)
        calculated: numpy.matrix = uncharacterized_inventory.sum(axis = 1)
        
        # Retrieve the biosphere series
        uncharacterized_biosphere_series: pd.Series = self.uncharacterized_biosphere_series[database]
        
        # Calculate the biosphere contribution with the default function
        biosphere_contribution: list[dict] = self._default_calculate_contribution_function(activity_key = activity_key,
                                                                                           method = None,
                                                                                           calculated = calculated,
                                                                                           biosphere_or_activity_series = uncharacterized_biosphere_series,
                                                                                           cut_off_percentage = None)
        # Return the list of dictionaries
        return biosphere_contribution



    


    # Function to extract the activity contribution of an uncharacterized inventory
    def extract_activity_contribution(self,
                                      activity_key: tuple) -> list[dict]:
        
        # Check function input type
        hp.check_function_input_type(self.extract_biosphere_contribution, locals())
        
        ## Load Brightway mappings if not yet loaded
        if not self.Brightway_mappings_loaded:
            self.load_Brightway_mappings()
        
        # Get uncharacterized contribution dictionaries if not yet created
        if not self.uncharacterized_contribution_dicts_created:
            self._get_uncharacterized_contribution_dicts()
        
        # Retrieve the activity
        activity: bw2data.backends.peewee.proxies.Activity = self.key_to_activity_mapping[activity_key]
        
        # Check if a uncharacterized inventory already exists for the activity
        if self.uncharacterized_inventory.get(activity_key) is None:

            # Get the uncharacterized inventory matrices in case they have not yet been built
            self._get_uncharacterized_inventory_matrices(activity)
        
        # Retrieve the key data of the dictionary in dictionary format
        activity_key_data: dict = self.retrieve_activity_key_data_as_dictionary(activity)
        
        # Extract the database of the current activity
        database: str = activity_key_data[self.key_name_activity_database]
        
        # Retrieve the uncharacterized inventory
        uncharacterized_inventory: scipy.sparse._csr.csr_matrix = self.uncharacterized_inventory[activity_key]
        
        # We are only interested in the uncharacterized processes/activities, therefore we first transpose the matrix (to have all activities in the rows) and then sum all the columns (emissions) to have one item per row (processes)
        calculated: numpy.matrix = uncharacterized_inventory.transpose().sum(axis = 1)
        
        # Retrieve the activity series
        uncharacterized_activity_series: pd.Series = self.uncharacterized_activity_series[database]
        
        # Calculate the activity contribution with the default function
        activity_contribution: list[dict] = self._default_calculate_contribution_function(activity_key = activity_key,
                                                                                          method = None,
                                                                                          calculated = calculated,
                                                                                          biosphere_or_activity_series = uncharacterized_activity_series,
                                                                                          cut_off_percentage = None)
        # Return the list of dictionaries
        return activity_contribution
    
    
    
    
    
    # Function to run all calculations
    def calculate_all(self) -> None:
        
        # Run all calculation functions to get the results
        self.calculate_LCIA_scores()
        self.extract_LCI_exchanges(exchange_level = self.exchange_level)
        self.extract_LCI_emission_contribution()
        self.extract_LCI_process_contribution()
        self.calculate_LCIA_scores_of_exchanges(exchange_level = self.exchange_level)
        self.calculate_LCIA_emission_contribution(cut_off_percentage = self.cut_off_percentage)
        self.calculate_LCIA_process_contribution(cut_off_percentage = self.cut_off_percentage)
        self.extract_characterization_factors()

    
    # Function that retrieves and assigned harmonized column names to available columns
    def retrieve_column_mapping_and_order(self, available_column_names: list) -> dict:
        
        # Check function input type
        hp.check_function_input_type(self.retrieve_column_mapping_and_order, locals())
        
        # This dictionary specifies ...
        # 1) keys which will be the column names and values which are list of tuples where each tuple contains regex fragments to search the available column names
        # 2) the order how columns should be arranged in the final dataframe (order of keys in dictionary)
        mapping: dict = {"Activity_database": [(self.key_name_activity_database,), (self.key_name_activity, "database")],
                         "Activity_UUID": [(self.key_name_activity, "uuid"), (self.key_name_activity, "code")],
                         "Activity_name": [(self.key_name_activity + "_name",), (self.key_name_activity, "name")],
                         "Activity_SimaPro_name": [(self.key_name_activity, "simapro", "name")],
                         "Activity_location": [(self.key_name_activity, "location")],
                         "Activity_amount": [(self.key_name_functional_amount,)],
                         "Activity_unit": [(self.key_name_activity, "unit")],
                         "Flow_database": [(self.key_name_flow, "database")],
                         "Flow_UUID": [(self.key_name_flow, "uuid"), (self.key_name_flow, "code")],
                         "Flow_level": [(self.key_name_level,)],
                         "Flow_name": [(self.key_name_flow_name,), (self.key_name_flow, "name")],
                         "Flow_categories": [(self.key_name_flow, "categories")],
                         "Flow_location": [(self.key_name_flow, "location")],
                         "Flow_amount": [(self.key_name_flow + "_amount",)],
                         "Flow_unit": [(self.key_name_flow_unit,), (self.key_name_flow, "unit")],
                         "Score": [(self.key_name_score,)],
                         "Score_unit": [(self.key_name_score_unit,)],
                         "Characterization_factor": [(self.key_name_characterization_factor,)],
                         "Characterization_unit": [(self.key_name_characterization_unit,)],
                         "Method": [(self.key_name_method,)]
                         }
        
        # Variable to store for each new column name, the respective column found from the function input ('available_column_names')
        mapped: dict = {}
    
        # Loop through each new column name and the list of tuples that specify how columns should be identified
        for new_column_name, search in mapping.items():
            
            # Initialize a new list where all possible mapped column names are temporarily stored
            possibilities: list = []
            found: str = ""
            
            # We loop through each provided column name and check if it suits a new column name
            for column_name in available_column_names:
                
                # Loop through each item that should be used to identify column names
                for items in search:
                    
                    # Initialize a variable to temporarily store whether an identifier suits the column name (True/False)
                    in_column_name = []
                    
                    # Loop through each identifier (in the tuple)
                    for item in items:    
                        
                        # Check if we have a perfect match
                        if item.lower() == column_name.lower():
                            
                            # If yes, we found our perfect column
                            found: str = column_name                            
                            
                            # We can break out of the loop
                            break
                            
                        # Evaluate whether the current identifier (item) matches the column name
                        evaluation: bool = bool(re.search(item.lower(), column_name.lower()))
                        
                        # Add
                        in_column_name += [evaluation]
                    
                    # If all identifier are True, that means that the current column name can successfully be mapped to the new column name
                    # That means we can add the column name to the list of possible candidates
                    if all(in_column_name):
                        possibilities += [column_name]
            
            # 1. priority: perfect match
            if found != "":
                mapped[found]: str = new_column_name
            
            # 2. priority: If the list of candidates is not empty, we simply take the first candidate and add to the dictionary
            elif possibilities != []:
                mapped[possibilities[0]]: str = new_column_name
        
        # For each new column name, check whether we have found a suitable candidate.
        order: list = [m for m in list(mapping.keys()) if m in list(mapped.values())]
        
        # Return the mapping as well as the order
        return mapped, order

    


    # Function that converts list of dictionaries into dataframes and cleans the columns, if specified (according to a predefined mapping)
    def _convert_results_to_df(self, clean: bool = True) -> None:
        
        # Check function input type
        hp.check_function_input_type(self._convert_results_to_df, locals())
        
        # Initialize a new attribute to store the results as dataframes to
        self.results_df: dict = {}
        
        # Loop through each result
        for result_key, result_list in self.results.items():
            
            # Convert the list of dictionary into a pandas dataframe
            df: pd.DataFrame = pd.DataFrame(result_list)
            
            # Check if the data frame should be cleaned
            # With clean, it is meant to only keep relevant columns and order them
            if clean:
                
                # Retrieve the column mapping and order based on the function and mapping in there
                column_mapping, column_order = self.retrieve_column_mapping_and_order(list(df.columns))

                # Write the dataframe to the dictionary with mapped and ordered columns
                self.results_df[result_key]: pd.DataFrame = df.rename(columns = column_mapping)[column_order]
                
            else:
                # If the dataframe should not be cleaned, we just convert the list of dictionary to a dataframe and return
                self.results_df[result_key]: pd.DataFrame = df
                


    # Function to return the results as dataframes
    def get_result_dataframes(self, clean: bool = True) -> dict:
        
        # Check function input type
        hp.check_function_input_type(self.get_result_dataframes, locals())
        
        # Convert result dictionaries to dataframes
        self._convert_results_to_df(clean = clean)
        
        # Print message of empty results in case results are empty
        if self.results_df == {}:
            raise ValueError("No results found. Did you calculate results beforehand?")
        
        # Return results as dictionary of dataframes
        return self.results_df
        

        
    # Function to return the results as dictionaries
    def get_result_dictionaries(self, clean: bool = True) -> dict:
        
        # Check function input type
        hp.check_function_input_type(self.get_result_dictionaries, locals())
        
        # Print message of empty results in case results are empty
        if self.results == {}:
            raise ValueError("No results found. Did you calculate results beforehand?")
        
        # Return the raw dictionary if results should not be cleaned beforehand        
        if not clean:
            return self.results
        
        else:
            # Get all unique available columns per result dict
            available_cols: dict = {result_key: list(set([str(m) for n in result_list for m in n.keys()])) for result_key, result_list in self.results.items()}
            
            # Initialize a new attribute to store cleaned list of dictionaries to
            self.results_cleaned: dict = {}
            
            # Loop through each result
            for result_key, result_list in self.results.items():
    
                # Retrieve the column mapping based on the function and mapping in there
                column_mapping, _ = self.retrieve_column_mapping_and_order(available_cols[result_key])
                
                # Add new dictionary key
                self.results_cleaned[result_key]: list = []
                
                # Loop through each dictionary in the list
                for m in result_list:
                    
                    # Initialize a new dictionary which will be the cleaned version
                    cleaned_m: dict = {}
                    
                    # Loop through each key and value of the initial dictionary
                    for k, v in m.items():
                        
                        # Check if the mapping can be applied
                        if column_mapping.get(k) is not None:
                            
                            # If yes, store the value under the new, cleaned key
                            cleaned_m[column_mapping[k]] = v
                    
                    # Add the cleaned dictionary to the already existing list
                    self.results_cleaned[result_key] += [cleaned_m]
                    
            # Return the new cleaned lists of dictionaries
            return self.results_cleaned

    



    # Function to export the results to XLSX/CSV
    def write_results(self,
                      path: (pathlib.Path | str | None),
                      filename: (str | None) = None,
                      use_timestamp_in_filename: bool = True,
                      clean: bool = True) -> None:
        
        # Check function input type
        hp.check_function_input_type(self.write_results, locals())
        
        # Convert results to dataframe if not yet done
        self.get_result_dataframes(clean = clean)
        
        # Create a path if not provided by the function
        # If not provided, the local Brightway2 folder will be used to save results
        if path is None:
            path: pathlib.Path = pathlib.Path(bw2data.projects.output_dir)
        
        # If path is of type string, provide as pathlib path
        elif isinstance(path, str):
            path: pathlib.Path = pathlib.Path(path)
            
        # Check if the path exists
        if not path.is_dir():
            raise ValueError("Path '" + str(path) + "' does not exist")
            
        # Extract current time
        current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        
        # Create filename if not provided
        if filename is None:
            
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
                filename = current_time + "_" + filename
        
        # Initialize a documentation variable
        documentation = {self.name_LCIA_scores: """This dataframe contains the result of the LCA calculation
                         and shows the total environmental impact of all activities (LCI) and methods (LCIA) combinations""",
                         
                         self.name_LCI_exchanges: """This dataframe contains detailed information about the inventories.
                         It shows the biosphere emissions and technosphere activities contributing to an activity (LCI).""",
                         
                         # !!! DO THE DOCUMENTATIONS HERE
                         self.name_LCI_emission_contributions: """This dataframe """,
                         
                         # !!! DO THE DOCUMENTATIONS HERE
                         self.name_LCI_process_contributions: """  """,
                         
                         # !!! DO THE DOCUMENTATIONS HERE
                         self.name_LCIA_immediate_scores: """  """,
                         
                         # !!! DO THE DOCUMENTATIONS HERE
                         self.name_LCI_emission_contributions: """  """,
                         
                         # !!! DO THE DOCUMENTATIONS HERE
                         self.name_LCI_process_contributions: """  """,
                         
                         # !!! DO THE DOCUMENTATIONS HERE
                         self.name_characterization_factors: """  """}
        
        # Write data to XLSX
        # Initialise the writer variable
        writer = pd.ExcelWriter(path / (str(filename) + ".xlsx"))
        
        # Convert the documentation dictionary into a dataframe
        documentation_df: pd.DataFrame = pd.DataFrame([{"Sheet": k, "Description": v} for k, v in documentation.items() if k in self.results_df])
        
        # Add the documentation dataframe to the writer
        documentation_df.to_excel(writer, sheet_name = "Documentation")
        
        # Add each result dataframe to the writer
        for df_name, df in self.results_df.items():
            
            # Check if the length of the dataframe exceeds a million and more rows
            # If not, we are good to go with writing an excel file
            if len(df) < 1048000:
                
                # We simply append the result dataframe to the excel workbook
                df.to_excel(writer, sheet_name = df_name[:30])
                
            else:
                # If there are more than one million rows, we can not add the results in a new sheet.
                # In that case, we write a standalone csv
                df.to_csv((path / str(df_name + ".csv")))

        # Save the XLSX
        writer.close()
        
        # Print filepath if specified
        if self.progress_bar:
            
            # Print statement saying where XLSX file was stored
            print("\n-----\nLCA results saved to the following path:\n" + str(path))
            

