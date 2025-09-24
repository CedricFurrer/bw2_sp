
#%% Import baseline functions
import pandas as pd
import re
# import math
# from itertools import compress
import sys
import types

#%%

# Own type of exception error, to be raised if function inputs are of wrong type (other than specified by the function)
class WrongFunctionInputType(Exception):
    "Raised when the function input value is of wrong type"
    pass

# Function to automatically check the input types given by a function
# Raises error, if wrong types are passed
def check_function_input_type(func, _locals: dict, exclude_from_check: list[str] = []):
    
    """
    Function to automatically compare function annotations (= variable types) with given function inputs. If variable types differ, error is raised
    Should be put at the beginning of a function. Needs the function and local variables as inputs.
    
    Examples:
    ---------
        >>> def f(x: str, y: dict, z: list, a: tuple, b: None, c: set, d: list[int | float], e: pd.DataFrame) -> list | dict:
                check_function_input_type(f, locals(), exclude_from_check = ["d"])
            
            f("k", {"a": "b"}, [1], ("a",), None, {"a", "b"}, [1, 2.0], pd.DataFrame({"a": ["b"]}))
    
    
    Parameters
    ----------
    func : function
        The function to check (usually the same function where it is applied to)
    
    _locals : dict
        A dictionary of all local variables, using 'locals()'
        
    exclude_from_check : list
        Variables to exclude from check. Default is empty list.

    Raises
    ------
    WrongFunctionInputType
        Raised when the function input value is of wrong type.

    Returns
    -------
    Only performs a type check and does not return anything

    """
    
    # Initialize an empty string
    input_variables_with_errors = ""
    
    # Loop through each function variable and given annotation (e.g., str or int)
    for item in func.__annotations__.keys():
        
        # Skip if check should not be done
        if item in exclude_from_check:
            continue
        
        # Assertion error if variable has parametrized type
        any_parametrized_typemarks = any([hasattr(m, "__origin__") for m in func.__annotations__[item].__args__]) if hasattr(func.__annotations__[item], "__args__") else False
        assert not any_parametrized_typemarks, "Parametrized checks not yet supported --> exclude the variable '" + str(item) + "' from being checked"
        
        # An exception for 'result' parameter
        if _locals.get(item) is None:
            continue
        
        # Check the current variable type with the annotation
        if not isinstance(_locals[item], func.__annotations__[item]):
            
            # If type is different from the annotation, add error information to the string
            input_variables_with_errors += "\nVariable '" + str(item) + "': " + str(type(_locals[item]).__name__) + " --> " + str(func.__annotations__[item].__name__ if not isinstance(func.__annotations__[item], types.UnionType) else func.__annotations__[item])
    
    # If string is not empty, an error has to be raised
    if input_variables_with_errors != "":
        raise WrongFunctionInputType("Some input variables passed for function '" + str(func.__name__) + "' have the wrong instance type and need to be checked:\n\n(VARNAME : CURRENT --> SHOULD)" + input_variables_with_errors)


#%% Function to modify elements of a tuple

def format_values(values: tuple | list,
                  case_insensitive: bool = True,
                  strip: bool = True,
                  remove_special_characters: bool = False,
                  pattern: str = "[()\[\],'\"]") -> tuple:
    
    """
    Function to modify the elements of a tuple.
    Depending on how it is specified in the function input, elements of the tuple:
        - will be written to lowercase
        - edging whitespaces will be stripped
        - special characters (specified by the regex pattern) will be removed

    Parameters
    ----------
    values : tuple[str | tuple] | list[str | tuple]
        The values to be modified. Can either be provied as list or tuple of string and tuple values.
        
    case_insensitive : bool, optional
        If True, all elements are converted to lowercase. The default is True.
    
    strip : bool, optional
        If True, tailing whitespaces of strings are stripped. The default is True.
    
    remove_special_characters : bool, optional
        If True, removes special characters which are specified in the regex pattern. The default is False.
        
    pattern : str, optional
        Pattern to use for removing special characters.

    Raises
    ------
    ValueError
        In case tuple elements are not of type 'tuple' or 'string', raises error.

    Returns
    -------
    values_new : tuple
        The initial tuple with the modified elements is returned.

    """
    
    # Make variable check
    check_function_input_type(format_values, locals())
    
    # Compile pattern
    re_sub = re.compile(pattern)
    
    # Initialize new tuple variable
    values_new = ()
    
    # Loop through each element in the tuple
    for m in values:
        
        # If None is provided, add to tuple and go on
        if m is None:
            values_new += (None,)
            
            # Go on
            continue
        
        # Check if tuple element is of type string
        if isinstance(m, str):
            
            # Write to lowercase, if specified
            if case_insensitive:
                m = m.lower()
            
            # Strip tailing whitespaces, if specified
            if strip:
                m = m.strip()
            
            # Remove special characters, if specified
            if remove_special_characters and re_sub is not None:
                m = re_sub.sub("", m)
            
            # Append
            values_new += (m,)
            
            # Go on
            continue
        
        # Check if tuple element is of type tuple
        if isinstance(m, tuple | list):
            
            # Write to lowercase, if specified
            if case_insensitive:
                m = [n.lower() for n in m]
            
            # Strip tailing whitespaces, if specified
            if strip:
                m = [n.strip() for n in m]
            
            # Remove special characters, if specified
            if remove_special_characters and re_sub is not None:
                m = [re_sub("", n) for n in m]
            
            # Append
            values_new += (tuple(m),)
            
            # Go on
            continue
        
        # Print element
        print(m)
        
        # Raise error if the current type of value can not be catched
        raise ValueError("Instance '" + str(type(m)) + "' not yet catched by function 'format_value'.")
        
    return values_new


# #%% Define function 'replace_if_value_is_nan'

# def replace_if_value_is_nan(value, replacement_value):
    
#     """ Replace a NaN value with a given/pre-defined replacement value.
    
#     Examples:
#     ---------
#         >>> replace_if_value_is_nan(float("NaN"), "B")
#         "B"
        
#         >>> replace_if_value_is_nan("A", "B")
#         "A"
        
#     Parameters
#     ----------
#     value : any type of length 1 (e.g. str, float, int, etc.)
#         The value to check and replace if it is NaN.
        
#     replacement_value : str
#         The replacement value to be applied when 'value' is NaN.

#     Returns
#     -------
#     value : str
#         If the initial 'value' was NaN, the 'replacement_value' is returned. Else the initial 'value' is returned
#     """
    
#     # Check if 'value' is of type float
#     if isinstance(value, float):
        
#         # If 'value' is of type float, check if it is NaN. Return the 'replacement_value' if so.
#         if math.isnan(value):
#             return replacement_value
        
#         # Else return the initial 'value'
#         else:
#             return value
#     else:
#         return value

# #%% Define function 'check_if_value_is_nan'

# def check_if_value_is_nan(value):
    
#     """ Check a value if it is NaN.
    
#     Examples:
#     ---------
#         >>> check_if_value_is_nan(float("NaN"))
#         True
        
#         >>> check_if_value_is_nan("A")
#         False
    
#     Parameters
#     ----------
#     value : any type of length 1
#         The value to check if it is NaN.

#     Returns
#     -------
#     bool
#         Returns a True if 'value' is NaN. Else False is returned.
#     """

#     # Check if 'value' is of type float
#     if isinstance(value, float):
        
#         # If 'value' is of type float, check if it is NaN. Return True if so.
#         if math.isnan(value):
#             return True
        
#         # Else return False
#         else:
#             return False
#     else:
#         return False

# #%% Define function 'check_instance'

# def check_instance(variable: any, list_of_instances_to_check: list, activate_error: bool = False, error_msg: str = ""):
    
#     """ Check if a variable is of a certain instance type.
    
#     Examples:
#     ---------
#         >>> check_instance(["A", "B"], [list, float])
#         True
        
#         >>> check_instance(["A", "B"], [str])
#         False
        
#         >>> check_instance(["A", "B"], [str], True, "")
#         TypeError: Variable '['A', 'B']' is of wrong instance type <class 'list'>, but should be of type <class 'str'>.
    
#     Parameters
#     ----------
#     variable : variable
#         Any variable, which should be checked as an instance
    
#     list_of_instances_to_check : list
#         List of types (e.g. str, tuple, dict, int, float, list) for which the variable should be checked for.
    
#     activate_error : bool, optional
#         Indicating whether an error should be thrown, if the instance check fails. The default is set to 'False'.
    
#     error_msg : str, optional
#         Additional error message, which can be added to the general error message. The default is set to ''.
        
#     Returns
#     -------
#     bool
#         If the variable is of one instance type provided as 'list_of_instances_to_check', a True is returned. Otherwise, either a False is returned or an error thrown (if 'activate_error' is set to True) indicating that the variable is not of the instance types given as the input to the function.
#     """
    
#     # Throw a Type Error if the variable 'list_of_instances_to_check' is not a list
#     if not isinstance(list_of_instances_to_check, list):
#         raise TypeError("The parameter 'list_of_instances_to_check' needs to be of type <class list> but is corrently not. Current instance type is " + str(type(list_of_instances_to_check)))
    
#     # Initialize the 'bool_for_validation' variable. If this variable will increase during processing of the function, it indicates that at least one provided
#     # instance from 'list_of_instances_to_check' is present
#     bool_for_validation = 0
    
#     # Check every instance one at a time (loop through)
#     for i in list_of_instances_to_check:
#         if isinstance(variable, i):
#              bool_for_validation = bool_for_validation + 1
    
#     # If no instance could be detected (== 0), either a False is returned or an error (if error is activated)
#     if bool_for_validation == 0:
#         if activate_error:
#             raise TypeError("Variable '" + str(variable) + "' is of wrong instance type " + str(type(variable)) + ", but should be of type " + str(list_of_instances_to_check[0]) + ". " + str(error_msg))
#         else:
#             return False
    
#     # Return True if an instance could be detected    
#     if bool_for_validation > 0:
#         return True
    
#%% Define function 'give_back_correct_cas'

def give_back_correct_cas(cas_value: str, return_None: bool = True):
    
    """ Converts a CAS-Nr. into the correct format
    
    Examples:
    ---------
        >>> give_back_correct_cas("000014-51-7")
        "000014-51-7"
    
        >>> give_back_correct_cas("14-51-7")
        "000014-51-7"
        
        >>> give_back_correct_cas("14-51-7", False)
        "000014-51-7"
        
        >>> give_back_correct_cas("No CAS-Nr.", False)
        "No CAS-Nr."
    
    Parameters
    ----------
    cas_value : str
        Any string, which might be a CAS-Nr. (e.g. '52-41-2' or also 'glyphosate').
    
    return_None : bool, optional
        Indicating, whether the original 'cas_value' should be returned or a None, if conversion is unsuccessful. The default is set to True.
        
    Returns
    -------
    str
        If the specific CAS-Nr. format is detected (xxxxxxx(x)-xx-x), a correct CAS-Nr. is given back. Otherwise, either None or the original 'cas_value' is returned depending on how 'return_None' is set.
    """
    
    # Check if cas_value is None, if yes, None is returned. If No, cas_value will be checked if there is a specific CAS-Nr. format.
    if cas_value is None or pd.isnull(cas_value):
        return None
    
    else:
        # Regex pattern with three groups
        pattern_cas = "^([0-9]{1}|[0-9]{2}|[0-9]{3}|[0-9]{4}|[0-9]{5}|[0-9]{6}|[0-9]{7})-([0-9]{2})-([0-9]{1})$"
        x = re.search(pattern_cas, cas_value)
        
        # If no pattern is detected, None is returned
        if x is not None:
            
            # Extract all 3 groups a separate variables
            x1 = x[1]
            x2 = x[2]
            x3 = x[3]
            
            # Group 1 can be either of length 6 or 7. If it is of such length, the cas_value can be returned as it is
            if len(x1) == 7 or len(x1) == 6:
                return cas_value
            
            # If group 1 is of length 5 or lower, add '0' to the beginning of group 1 (for standardization purpose)
            else:
                add_n_zeros = 6 - len(x1)
                return "0"*add_n_zeros + x1 + "-" + x2 + "-" + x3
        else:
            
            if return_None:
                return None
            
            else:
                return cas_value

# #%% Define function 'combine_list_of_dictionaries_v3'

# def combine_list_of_dictionaries(list_of_dicts: list, replacement_if_missing = None):
    
#     """ Combines a list of dictionaries into a common dictionary. All keys are preserved. Values of the keys will be put into a list. If in one dictionary, the key is missing, a None value will be put into the list.
    
#     Parameters
#     ----------
#     list_of_dicts : list
#         A list of dictionaries to be merged

#     Returns
#     -------
#     dict
#         A comnbined dictionary is returned.
#     """
    
#     return {
#         k: list(d[k] if k in d else replacement_if_missing for d in list_of_dicts)
#         for k in set().union(*(e.keys() for e in list_of_dicts))
#     }


# #%% Define function 'remove_duplicates_from_a_list'

# def remove_duplicates_from_a_list(input_list_or_tuple):
    
#     """ Remove the duplicates of either a list of lists, a list of tuples, a tuple of tuples or a tuple of lists.
    
#     Examples:
#     ---------
#         >>> remove_duplicates_from_a_list([["A", "A"], ["A", "A"], ["A", "B"]])
#         [["A", "A"], ["A", "B"]]
        
#         >>> remove_duplicates_from_a_list((("A", "A"), ("A", "A"), ("A", "B")))
#         [("A", "A"), ("A", "B")]
        
#         >>> remove_duplicates_from_a_list((["A", "A"], ["A", "A"], ["A", "B"]))
#         (["A", "A"], ["A", "B"])
        
#         >>> remove_duplicates_from_a_list((("A", "A"), ["A", "A"], ["A", "B"]))
#         [("A", "A"), ["A", "A"], ["A", "B"]]
    
#     Parameters
#     ----------
#     input_list_or_tuple : list or tuple
#         Input list or tuple, where duplicated objects should be removed.
        
#     Returns
#     -------
#     final : list or tuple
#         Returns a duplicate-free/cleaned list or tuple
#     """
    
#     final = []
#     [final.append(m) for m in input_list_or_tuple if m not in final]       
    
#     return final

# #%% Define function 'convert_dictionary_to_list_of_tuples'

# def convert_dictionary_to_list_of_tuples(dictionary: dict):
    
#     """ Combine each list element for each key in 'dictionary' into a tuple and append to a (large) list.
#     Important: the list of each key in the dictinary needs to be of the same length! Else an error is thrown.
#     This function is the inverse of the function 'convert_list_of_tuples_to_dictionary'
    
#     Examples:
#     ---------
#         >>> convert_dictionary_to_list_of_tuples({"name": ["A", 1], "unit": ["B", "b"], "categories": [1, 2]})
#         {"keys": ("name", "unit", "categories"), "tuples": [("A", "B", 1), (1, "b", 2)]}       
    
#     Parameters
#     ----------
#     dictionary : dict
#         Input dictionary, which should be converted.
        
#     Returns
#     -------
#     dict
#         Returns a dictionary, where the key 'tuples' is the list of tuples and the key 'keys' is a tuple with the name of the objects of the tuples.
#     """
    
#     # Get all the keys of the dictionary as a list
#     keys = dictionary.keys()
#     keys_as_list = list(keys)
    
#     # Transform simple NaN or None (which would throw an error if provided by dictionary) to [NaN] or [None]
#     for k, v in dictionary.items():
        
#         if check_instance(v, [int, float], False, ""): 
#             if pd.isnull(v):
#                 dictionary[k] = [float("NaN")]
        
#         if v is None:
#             dictionary[k] = [None]
    
#     # Get the length of the value object of each key
#     len_keys = [len(dictionary[m]) for m in keys]
    
#     # Throw an error, if lengths are not equal
#     if len(set(len_keys)) > 1:
#         raise ValueError("List content of the keys of dictionary are not of equal length." + str([str(a) + " = " + str(b) for a, b in zip(keys, len_keys)]))
    
#     # Initialize a list of tuples using the value of the first key
#     final = [(m,) for m in dictionary[keys_as_list[0]]]
    
#     # Loop through every key except the key used just before
#     for i in keys_as_list[1:len(keys_as_list)]:
        
#         # Append the list of tuples with the new value information from the current key
#         final = [n + (m,) for n, m in zip(final, dictionary[i])]
        
#     return {"keys": tuple(keys_as_list), "tuples": final}

# #%% Define function 'select_tuples'

# def select_tuples(dictionary: dict, selection_fields: list):
    
#     """ Selecting and/or reordering 'tuples' from a list of tuples. 
    
#     Examples:
#     ---------
#         >>> select_tuples({"keys": ("name", None, "categories"), "tuples": [("A", "B", 1), (1, "b", 2)]}, [None, "name"])
#         {"keys": (None, "name"), "tuples": [("B", "A"), ("b", 1)]}
    
#     Parameters
#     ----------
#     dictionary : dict
#         Input dictionary with list of tuples as key 'keys' and description of tuples as 'tuples' where keys should be selected/removed and/or reordered.
        
#     selection_fields : list
#         A list specifying which keys should remain and in which order. The values in 'selection_fields' need to appear in dictionary["keys"]!
        
#     Returns
#     -------
#     dict
#         Returns a dictionary with reordered and/or selected fields as 'keys' and reordered and/or selected tuples as 'tuples'.

#     """
#     # Check whether the input dictionary is correctly structured. If 'keys' or 'tuples' are not provided as keys, an error is thrown.
#     if "keys" not in dictionary.keys() or "tuples" not in dictionary.keys():
#         raise TypeError("Input 'dictionary' is probably wrong structured. One or both of the keys ('tuples' or 'keys') are missing in the input variable 'dictionary'. Current keys are: " + str(list(dictionary.keys())))
    
#     # Check whether the value of the key 'tuples' is of type list/tuple or not. If not, an error is thrown.
#     if not check_instance(dictionary["tuples"], [list, tuple]):
#         raise TypeError("The key 'tuples' is not of type <class 'list'> or <class 'tuple'> but currently of type " + str(type(dictionary["tuples"])))
    
#     # Check whether the value of the key 'keys' is of type list/tuple or not. If not, an error is thrown.
#     if not check_instance(dictionary["keys"], [list, tuple]):
#         raise TypeError("The key 'keys' is not of type <class 'list'> or <class 'tuple'> but currently of type " + str(type(dictionary["keys"])))
    
#     # Check whether there are two same key names. Theoretically, no error would result if there would exist two keys.
#     # However, values would be overwritten in the dictionary, which is not ideal. Therefore, better to throw an error
#     if len(dictionary["keys"]) != len(set(dictionary["keys"])):
#         raise ValueError("There is at least one duplicated key. Duplicated keys lead to an overwriting of dictionary keys, which is not ideal. Current keys (with possible duplicates) are: " + str(dictionary["keys"]))
    
#     # Get tuples and keys length
#     tuples_length = set([len(m) for m in dictionary["tuples"]])
#     keys_length = set([len(dictionary["keys"])])
    
#     # Check whether tuples lengths are the same as keys length. If not, an error is thrown.
#     if tuples_length != keys_length:
#         raise ValueError("Length of keys (n = " + str(keys_length) + ") and length of tuples (n = " + str(tuples_length) + ") are not the same.")
    
#     # Tuples description names
#     keys_as_list = dictionary["keys"]
    
#     # Check whether all fields from 'selection_fields' appear in dictionary['keys']
#     check_if_available = [0 if m in keys_as_list else 1 for m in selection_fields]
    
#     # If not, a value error is thrown
#     if sum(check_if_available*1) > 0:
#         raise ValueError(str(sum(check_if_available*1)) + " parameter(s) in 'selection_fields' don't appear in 'dictionary['keys']'. 'keys' are " + str(keys_as_list) + ". 'selection_fields' are " + str(selection_fields))
    
#     # Check if 'selection_fields' is of type list
#     check_instance(selection_fields, [list])
    
#     # Get the index of 'selected_fields'
#     selection_fields_as_index = [keys_as_list.index(m) for m in selection_fields]
    
#     # Create a new list with reordered/arranged tuples based on 'selection_fields'
#     tuples_new = [tuple([m[n] for n in selection_fields_as_index]) for m in dictionary["tuples"]]
    
#     return {"keys": tuple(selection_fields), "tuples": tuples_new}

# #%% Define function 'remove_duplicates_from_named_list_by_fields'

# def remove_duplicates_from_named_list_by_fields(dictionary: dict, fields: list = None, return_first_or_last: str = "first"):
    
#     """ Remove duplicates from named tuples from dictionary with key 'tuples'. Tuple elements are described by dictionary 'keys'.
    
#     Examples:
#     ---------
#         >>> remove_duplicates_from_named_list_by_fields({"keys": ("name", None, float("NaN")), "tuples": [("A", "B", 1), ("B", "B", 1), ("C", "B", 1)]}, fields = [float("NaN")], return_first_or_last = "first")
#         {"keys": ("name", None, float("NaN")), "tuples": [("A", "B", 1)]}
        
#         >>> remove_duplicates_from_named_list_by_fields({"keys": ("name", None, float("NaN")), "tuples": [("A", "B", 1), ("B", "B", 1), ("C", "B", 1)]}, fields = [float("NaN")], return_first_or_last = "last")
#         {"keys": ("name", None, float("NaN")), "tuples": [("C", "B", 1)]}
        
#         >>> remove_duplicates_from_named_list_by_fields({"keys": ("name", None, float("NaN")), "tuples": [("A", "B", 1), ("A", "B", 2), ("C", "B", 1)]}, fields = ["name", None])
#         {"keys": ("name", None, float("NaN")), "tuples": [("A", "B", 1), ("C", "B", 1)]}
        
#         >>> remove_duplicates_from_named_list_by_fields({"keys": ("name", None, float("NaN")), "tuples": [("A", "B", 1), ("A", "B", 2), ("C", "B", 1)]})
#         {"keys": ("name", None, float("NaN")), "tuples": [("A", "B", 1), ("A", "B", 2), ("C", "B", 1)]}
    
#     Parameters
#     ----------
#     dictionary : dict
#         Input dictionary with list of tuples as key 'keys' and description of tuples as 'tuples' where duplicates should be removed.
    
#     fields : list, optional
#         List of names contained in 'keys' to where grouping should be applied. The default is None where all information from tuples are used for grouping.
    
#     return_first_or_last : str
#         A string specifying whether the first element of a duplicate ('first') or the last element of a duplicate ('last') should be returned. The default is set to 'first'.

#     Returns
#     -------
#     dict
#         Returns a dictionary in the same structure, where duplicated tuples in the key 'tuples' were removed. 

#     """
#     # Check whether the input dictionary is correctly structured. If 'keys' or 'tuples' are not provided as keys, an error is thrown.
#     if "keys" not in dictionary.keys() or "tuples" not in dictionary.keys():
#         raise TypeError("Input 'dictionary' is probably wrong structured. One or both of the keys ('tuples' or 'keys') are missing in the input variable 'dictionary'. Current keys are: " + str(list(dictionary.keys())))
    
#     # Check whether the value of the key 'tuples' is of type list/tuple or not. If not, an error is thrown.
#     if not check_instance(dictionary["tuples"], [list, tuple]):
#         raise TypeError("The key 'tuples' is not of type <class 'list'> or <class 'tuple'> but currently of type " + str(type(dictionary["tuples"])))
    
#     # Check whether the value of the key 'keys' is of type list/tuple or not. If not, an error is thrown.
#     if not check_instance(dictionary["keys"], [list, tuple]):
#         raise TypeError("The key 'keys' is not of type <class 'list'> or <class 'tuple'> but currently of type " + str(type(dictionary["keys"])))
    
#     # Check whether there are two same key names. Theoretically, no error would result if there would exist two keys.
#     # However, values would be overwritten in the dictionary, which is not ideal. Therefore, better to throw an error
#     if len(dictionary["keys"]) != len(set(dictionary["keys"])):
#         raise ValueError("There is at least one duplicated key. Duplicated keys lead to an overwriting of dictionary keys, which is not ideal. Current keys (with possible duplicates) are: " + str(dictionary["keys"]))
    
#     # Check if 'fields' is of type list
#     check_instance(fields, [list])
        
#     # If an element provided by 'fields' is not contained in 'keys', an error is thrown
#     if fields is not None:
        
#         # Loop through each field element if 'fields' is not None and check individually
#         for y in fields:
            
#             # Check if 'y' is NaN
#             if check_if_value_is_nan(y):
                
#                 # If 'check_nan' is 1, NaN is contained in 'fields'
#                 check_nan = sum([1 if check_if_value_is_nan(m) else 0 for m in dictionary["keys"]])
                
#                 # If 'check_nan' is 0, an error is thrown because NaN is not contained in 'fields'
#                 if check_nan == 0:
#                     raise ValueError("Grouping by field '" + str(y) + "' (provided by 'fields') is not possible because '" + str(y) + "' does not exist in dictionary 'keys'. Current keys are: " + str(list(dictionary["keys"])))
            
#             # else simply raise error if y is not contained
#             elif y not in dictionary["keys"]:
#                 raise ValueError("Grouping by field '" + str(y) + "' (provided by 'fields') is not possible because '" + str(y) + "' does not exist in dictionary 'keys'. Current keys are: " + str(list(dictionary["keys"])))
    
#     # Get tuples and keys length
#     tuples_length = set([len(m) for m in dictionary["tuples"]])
#     keys_length = set([len(dictionary["keys"])])
    
#     # Check whether tuples lengths are the same as keys length. If not, an error is thrown.
#     if tuples_length != keys_length:
#         raise ValueError("Length of keys (n = " + str(keys_length) + ") and length of tuples (n = " + str(tuples_length) + ") are not the same.")
    
#     # Get the index of 'tuples' for which the grouping should be applied. Grouping can be stated in the input variable 'fields'.
#     # If fields is None, the whole 'tuples' will be used for grouping.
#     if fields is not None:
        
#         # Extract the 'tuples' indexes for which the grouping should be applied
#         # Needs to be done individually, for NaN and None because of special characteristics
#         group_rang_other = [dictionary["keys"].index(m) for m in fields if not pd.isnull(m)]
#         group_rang_none = [dictionary["keys"].index(m) for m in fields if m is None]
        
#         # Get index for NaN in keys
#         group_rang_nan_orig = list(compress(list(range(0, len(dictionary["keys"]))), [True if check_if_value_is_nan(m) else False for m in dictionary["keys"]]))
        
#         # Only write the key if NaN in 'fields'
#         group_rang_nan = [group_rang_nan_orig[0] for m in fields if check_if_value_is_nan(m)]
        
#         # Combine the individual indexes together
#         group_rang = group_rang_other + group_rang_none + group_rang_nan
#     else:
#         group_rang = [*range(list(tuples_length)[0])]
    
#     # Depending how specified at the beginning of the function, the first or last duplicated object is returned.
#     if return_first_or_last == "last":
        
#         # Convert tuples to strings for easier processing
#         selected_tuples_as_str = [str([n[m] for m in group_rang]) for n in dictionary["tuples"]]
        
#         # Loop through the list of strings and add the string name to a dictionary as key and the location index as value
#         location_of_non_duplicates_orig = {v: k for k, v in enumerate(selected_tuples_as_str)}
        
#         # Extract the location of the tuples, which should be kept. Those are unique ones, where duplicates have been removed.
#         location_of_non_duplicates = [m for k, m in location_of_non_duplicates_orig.items()]
        
#         # Write new list of tuples and append to new dictionary
#         tuples_new = [dictionary["tuples"][m] for m in location_of_non_duplicates]
        
#     else:
#         # Convert tuples to strings for easier processing
#         selected_tuples_as_str = [str([n[m] for m in group_rang]) for n in dictionary["tuples"][::-1]]
        
#         # Loop through the list of strings and add the string name to a dictionary as key and the location index as value
#         location_of_non_duplicates_orig = {v: k for k, v in enumerate(selected_tuples_as_str)}
        
#         # Extract the location of the tuples, which should be kept. Those are unique ones, where duplicates have been removed.
#         location_of_non_duplicates = [m for k, m in location_of_non_duplicates_orig.items()]
        
#         # Write new list of tuples and append to new dictionary
#         tuples_new = [dictionary["tuples"][::-1][m] for m in location_of_non_duplicates][::-1]
    
#     return {"keys": dictionary["keys"], "tuples": tuples_new}

# #%% Define function 'convert_list_of_tuples_to_dictionary'

# def convert_list_of_tuples_to_dictionary(dictionary: dict):
    
#     """ Convert a list of tuples with a dictionary key descriptor into a key/value pair structured dictionary
#     Important: the length of the tuples in the list need to be the same! Else an error is thrown.
#     This function is the inverse of the function 'convert_dictionary_to_list_of_tuples'.
    
#     Examples:
#     ---------
#         >>> convert_list_of_tuples_to_dictionary({"keys": ("name", None, "categories"), "tuples": [("A", "B", 1), (1, "b", 2)]})
#         {"name": ["A", 1], None: ["B", "b"], "categories": [1, 2]}
    
#     Parameters
#     ----------
#     dictionary : dict
#         Input dictionary with list of tuples as key 'keys' and description of tuples as 'tuples' which should be converted.
        
#     Returns
#     -------
#     final : dict
#         Returns a dictionary with key/value pairs. Values are of type list and of equal length.
#     """
    
#     # Check whether the input dictionary is correctly structured. If 'keys' or 'tuples' are not provided as keys, an error is thrown.
#     if "keys" not in dictionary.keys() or "tuples" not in dictionary.keys():
#         raise TypeError("Input 'dictionary' is probably wrong structured. One or both of the keys ('tuples' or 'keys') are missing in the input variable 'dictionary'. Current keys are: " + str(list(dictionary.keys())))
    
#     # Check whether the value of the key 'tuples' is of type list/tuple or not. If not, an error is thrown.
#     if not check_instance(dictionary["tuples"], [list, tuple]):
#         raise TypeError("The key 'tuples' is not of type <class 'list'> or <class 'tuple'> but currently of type " + str(type(dictionary["tuples"])))
    
#     # Check whether the value of the key 'keys' is of type list/tuple or not. If not, an error is thrown.
#     if not check_instance(dictionary["keys"], [list, tuple]):
#         raise TypeError("The key 'keys' is not of type <class 'list'> or <class 'tuple'> but currently of type " + str(type(dictionary["keys"])))
    
#     # Check whether there are two same key names. Theoretically, no error would result if there would exist two keys.
#     # However, values would be overwritten in the dictionary, which is not ideal. Therefore, better to throw an error
#     if len(dictionary["keys"]) != len(set(dictionary["keys"])):
#         raise ValueError("There is at least one duplicated key. Duplicated keys lead to an overwriting of dictionary keys, which is not ideal. Current keys (with possible duplicates) are: " + str(dictionary["keys"]))
    
#     # Get tuples and keys length
#     tuples_length = set([len(m) for m in dictionary["tuples"]])
#     keys_length = set([len(dictionary["keys"])])
    
#     # Check whether tuples lengths are the same as keys length. If not, an error is thrown.
#     if tuples_length != keys_length:
#         raise ValueError("Length of keys (n = " + str(keys_length) + ") and length of tuples (n = " + str(tuples_length) + ") are not the same.")
        
#     # Key names which should be written to the dictionary
#     keys_as_list = dictionary["keys"]
    
#     # Initialize dictionary which should be returned
#     final = {}
    
#     # Loop through the range of each key name and write key/value pair to 'final'
#     for i in range(0, len(keys_as_list)):
#         final[keys_as_list[i]] = [m[i] for m in dictionary["tuples"]]
    
#     return final

# #%% Define function 'differences_of_two_dictionaries'

# def differences_of_two_dictionaries(dict1: dict, dict2: dict):
    
#     """ Extract the key/value pairs which are different from two dictionaries. A key with name 'example' from 'dict1' will be named as 'FROM_example', one from 'dict2' as 'TO_example'. The values are extracted from either dictionary. If a key is missing, a None value is returned for that key in the 'diff' dictionary. 
    
#     Examples:
#     ---------
#         >>> differences_of_two_dictionaries({"name": "A"}, {"name": "B", "strategy": "something"})
#         {"FROM_name": "A", "TO_name": "B", "FROM_strategy": None, "TO_strategy": "something"}

#     Parameters
#     ----------
#     dict1 : dict
#         Dictionary 1 to compare.
        
#     dict2 : dict
#         Dictionary 2 to compare

#     Returns
#     -------
#     diff : dict
#         Creates a dictionary, where the keys of 'dict1' and 'dict2' are present, if the values of those keys are different. Key names from 'dict1' will be extended with "FROM_". The same applies for 'dict2' with "TO_". 

#     """
    
#     if dict1 is None or dict2 is None:
#         return None
    
#     # Initialize empty dictionary which should returned in the end
#     diff = {}
    
#     # Extract all the unique keys from both dictionaries and loop through them
#     for i in list(set(list(dict1.keys()) + list(dict2.keys()))):
        
#         # For both, 'dict1' and 'dict2', extract the values
#         value_of_key_in_dict1 = dict1.get(i)
#         value_of_key_in_dict2 = dict2.get(i)
        
#         # Write values from 'dict1' as is
#         diff[str("FROM_" + str(i))] = value_of_key_in_dict1
        
#         # Compare the extracted values. If they are different, then write the 'TO' values from 'dict2'
#         if value_of_key_in_dict1 != value_of_key_in_dict2:
#             diff[str("TO_" + str(i))] = value_of_key_in_dict2
        
#         # Else write an empty list
#         else:
#             diff[str("TO_" + str(i))] = []
            
#     return diff

# #%% Define function 'get_dictionary_with_selected_keys'

# def get_dictionary_with_selected_keys(list_of_dictionary: list, list_of_keys_to_be_returned: list = "all"):
    
#     """ Get only a selection of keys from multiple dictionaries from a list.
    
#     Examples:
#     ---------
#         >>> get_dictionary_with_selected_keys([{"name": "A", "categories": "B"}, {"name": "C", "categories": "D"}], ["name"])
#         [{"name": "A"}, {"name": "C"}]

#     Parameters
#     ----------
#     list_of_dictionary : list
#         A list which contains dictionaries.
        
#     list_of_keys_to_be_returned : list
#         The name of the keys which should be returned. The default is set to 'all'. If that is True, all keys are given back.

#     Returns
#     -------
#     end_list : list
#         Returns a list with dictionaries which only contain the keys as specified by the parameter 'list_of_keys_to_be_returned'.

#     """
#     # Initialize a list
#     end_list = []
    
#     # Remove any NaN or None values from the list
#     list_of_dictionary_cleaned = [m for m in list_of_dictionary if m is not None and not pd.isnull(m) and m != []]
    
#     # Get all keys from all dictionaries in the list
#     all_keys_available = list(set([item for sublist in list_of_dictionary for item in sublist]))
    
#     # Use all keys to give back, if 'list_of_keys_to_be_returned' is not defined
#     if list_of_keys_to_be_returned == "all":
#         list_of_keys_to_be_returned = all_keys_available
    
#     # Return None, if 'list_of_dictionary' is None
#     if list_of_dictionary is None:
#         return None
       
#     # Loop through each element in 'list_of_dictionary_cleaned'
#     for i in list_of_dictionary_cleaned:
        
#         # Quickly check if the instance is of type dict.
#         check_instance(i, [dict], True)
        
#         # Initialize an empty dictionary, to write/append data
#         curr = {}
        
#         # Loop through each key which should be returned
#         for ii in list_of_keys_to_be_returned:
            
#             # Get value of the key, if it is there
#             ii_value = i.get(ii)
            
#             # Write the key/value pair to the new dictionary 'curr'
#             if ii in list(i.keys()):
#                 curr[ii] = ii_value
        
#         # Append dictionary to the list to be returned
#         end_list.append(curr)
        
#     return end_list


# #%% Define function 'get_first_list_element_if_regex_pattern_appear'

# def get_n_list_element_if_regex_pattern_appear(orig_list: list,
#                                                regex_pattern: str,
#                                                n : list = [0],
#                                                return_orig_list_if_pattern_not_found: bool = False,
#                                                return_non_matching_elements: bool = False,
#                                                print_warning_statements: bool = True):
    
#     """ Extract the first (n = [0]), the second ([1]) or the first and second ([0, 1]) and so on (...) list element(s) for which a given regex pattern could be identified.
    
#     Depending on how the following variables are set, different values are returned:
        
#         'return_orig_list_if_pattern_not_found': If set to False (default), the first n list element(s) which include(s) the 'regex_pattern' is/are returned. If no 'regex_pattern' is found in list elements, an empty list ([]) is returned. If set to True, the 'orig_list' is returned in case no list element contained the 'regex_pattern'.
#         'return_non_matching_elements': If set to False (default), the first n list element(s) which include(s) the 'regex_pattern' is/are returned. If set to True, the other list element(s) (if any) not containing the 'regex_pattern' will also be returned.
            
#     Examples
#     --------
#         >>> get_n_list_element_if_regex_pattern_appear(["A001", "A002", "A003", "B001", None, [], float("NaN")], regex_pattern = "^A", n = [0], return_orig_list_if_pattern_not_found = False, return_non_matching_elements = False, print_warning_statement = False)
#         ["A001"]
            
#         >>> get_n_list_element_if_regex_pattern_appear(["A001", "A002", "A003", "B001", None, [], float("NaN")], regex_pattern = "^A", n = [0, 2], return_orig_list_if_pattern_not_found = False, return_non_matching_elements = False, print_warning_statement = False)
#         ["A001", "A003"]

#         >>> get_n_list_element_if_regex_pattern_appear(["A001", "A002", "A003", "B001", None, [], float("NaN")], regex_pattern = "^A", n = [0, 2], return_orig_list_if_pattern_not_found = True, return_non_matching_elements = True, print_warning_statement = False)
#         ["A001", "A003", "B001", None, [], nan]

#         >>> get_n_list_element_if_regex_pattern_appear(["A001", "A002", "A003", "B001", None, [], float("NaN")], regex_pattern = "^A", n = [0, 2, 1000], return_orig_list_if_pattern_not_found = True, return_non_matching_elements = True, print_warning_statement = False)
#         ["A001", "A003", "B001", None, [], nan]

    
#     Parameters
#     ----------
#     orig_list : list
#         The list element(s) which can be returned, if the 'regex_pattern' appears.
        
#     regex_pattern : str
#         The regex pattern on which list elements are selected.
    
#     n : list
#         List of integers specifying which element (in order) should be given back. [0] = only first element, [1] = only second element, [0, 1] = first and second element, and so on... The default is set to [0], returning the first matching element.
    
#     return_orig_list_if_pattern_not_found : bool
#         Specify, if an empty list or the 'orig_list' should be returned, in case the 'regex_pattern' is not found in any of the list elements of 'orig_list'. The default is set to False, returning an empty list if no list element contains the 'regex_pattern'.
        
#     return_non_matching_elements : bool
#         Specify, if other list elements which do not contain the 'regex_pattern' should additionally be returned or not. The default is set to False, indicating that other list elements will not be returned.
    
#     print_warning_statements : bool
#         Specify, if warning statements should be printed regarding the input parameter 'n'. The default is set to True, printing warning statements.
    
#     Returns
#     -------
#     y : list
#         Output is always a list. If the pattern is found in one or multiple list element(s), the first of this element(s) is returned. Else either an empty list or the 'orig_list' is returned with or without additional list elements, depending on how 'return_orig_list_if_pattern_not_found' and 'return_non_matching_elements' were specified.

#     """
#     # Check if the initial list ('orig_list') really is of type 'list'.
#     check_instance(orig_list, [list], True, "")
    
#     # Check if 'n' really is of type 'list'.
#     check_instance(n, [list], True, "")
    
#     # Check if all elements of 'n' are integers
#     [check_instance(m, [int], True, "One of the list (" + str(m) + ") elements of 'n' is not of type <class int>.") for m in n]
    
#     # Remove None's and NaN's
#     orig_list_cleaned = [m if not check_if_value_is_nan(m) and m is not None and m != [] else [] for m in orig_list]
    
#     # If there was no string element found in 'orig_list_cleaned', return None because the screening with the 'regex_pattern' on the 'orig_list' is not possible.
#     if orig_list_cleaned != []:
        
#         # Give back the indexes of the list elements, where the pattern appears
#         x = [index for index, m in enumerate(orig_list_cleaned) if bool(re.search(regex_pattern, str(m)))]
        
#         # If an index was found, because one or multiple list element contained the 'regex_pattern', give back the first element. Otherwise, return a None
#         if x != []:
            
#             # If the max index of 'n' is greater than provided by 'x', cut-off/remove the ones in 'n' which do not fit
#             if max(n) > max(x):
                
#                 # Print warning statement
#                 if print_warning_statements:
#                     print("Index value(s) " + str([m for m in n.copy() if m > max(x)]) + " as specified by 'n' " + str(n) + " has/have been removed from 'n' because it/they would be out of range")
            
#                 # Remove the indexes which would be out of range from 'n'
#                 n = [m for m in n.copy() if m <= max(x)]
            
#             # Overwrite the indexes specified in 'n' if there are not enough pieces specified in 'x'.
#             if len(n) > len(x):
                
#                 # Instead, all pieces in 'x' are given back
#                 n = list(range(len(x)))
            
#             # Return either all other pieces which do not fit the 'regex_pattern' as well (True) or not (False)
#             if return_non_matching_elements:
                
#                 # Specify the elements which fit the 'regex_pattern' but which are not in the index as specified by 'n'. Those should be removed.
#                 elements_to_remove = [orig_list[index] for index, m in enumerate(x) if index not in n]
                
#                 # Remove the pieces                
#                 y = [m for m in orig_list if m not in elements_to_remove]
            
#             else:
#                 # Else only write the pieces based on the index provided by 'n'
#                 y = [orig_list[m] for m in n]
#         else:
#             y = []
        
#     else:
#         y = []
    
    
#     # If no element could be identified, which includes the 'regex_pattern' and it was specified to give back the 'orig_list' in 'return_orig_list_if_pattern_not_found', return the original list.
#     if y == [] and return_orig_list_if_pattern_not_found:
#         return orig_list
    
#     else:
#         # Else return the first list element or a None
#         return y



# #%% Define function 'flatten'

# def flatten(orig_list: list):
    
#     """ Function to flatten a nested list or a nested tuple

#     Parameters
#     ----------
#     orig_list : list or tuple
#         DA nested list or a nested tuple which should be flattened

#     Returns
#     -------
#     x : list
#         Returns a flattened list

#     """
#     # Raise an error, if the input 'orig_list' is not of type list or type tuple
#     check_instance(orig_list, [list, tuple], True)
    
#     # Make a flat list
#     x = [item for sublist in orig_list for item in sublist]
    
#     return x


# #%% Define function 'default_dict'

# def default_dict(keys: list,
#                  existing_dict: dict = {},
#                  overwrite_keys_from_existing: bool = False,
#                  print_overwrite_keys_warning: bool = True):
    
#     """
#     A function which creates a dictionary with given 'keys' and empty lists as value. If there is a dictionary provided ('existing_dict'), given keys ('keys') with empty lists as value will be added. If keys already exist, they either will be overwritten or kept (depending on how 'overwrite_keys_from_existing' is set). A warning can be printed.

#     Parameters
#     ----------
#     keys : list
#         List of string which should either be defined as keys in the default dict or appended to the 'existing_dict'.
        
#     existing_dict : dict, optional
#         Dictionary to which keys with empty lists should be appended. The default is set to None. In that case, a dictionary will be created where keys are added to.
        
#     overwrite_keys_from_existing : bool, optional
#         A bool which defines if overlapping keys should be taken from 'exsting_dict' (= False) or if the overlapping keys should be overwritten with empty lists (= True). The default is set to False.
        
#     print_overwrite_keys_warning : bool, optional
#         A bool which defines whether a warning statement should be printed (= True) if there are overlapping keys or no warning should be printed. The default is set to True.

#     Returns
#     -------
#     dct : dict
#         Returns a dictionary with keys defined as in 'keys' which have empty list as value. Either a new dictionary is created or they are appended to an existing dictionary if provided.

#     """ 
        
#     # Initialize a dictionary to where the results are added to the same keys
#     dct = dict.fromkeys(keys, [])
    
#     # Append new keys to an empty dictionary 'dct'
#     for k, _ in dct.items():
#         dct[k] = []
    
#     # Check if 'existing_dict' is indicated
#     if existing_dict != {}:
        
#         # Check if there are overlapping keys from both dicts
#         overlapping_keys = [key for key in dct.keys() if key in existing_dict.keys()]
        
#         # If there are overlapping keys and warning should be printed, print warning
#         if print_overwrite_keys_warning and overlapping_keys != []:
            
#             # Print warning depending on how the variable 'overwrite_keys_from_existing' is set
#             if overwrite_keys_from_existing:
#                 print("Warning: Overlapping keys are currently overwritten with empty lists ('[]').\n\nOverlapping keys are:\n" + str(overlapping_keys))
#             else:
#                 print("Warning: Overlapping keys are currently taken from dictionary 'existing_dict'.\n\nOverlapping keys are:\n" + str(overlapping_keys))
        
#         # Return new dictionary
#         if overwrite_keys_from_existing:
#             existing_dict.update(dct)
#             return existing_dict
#         else:
#             dct.update(existing_dict)
#             return dct
#     else:
#         return dct

# #%% Define function 'import_json'

# def import_json(path_to_file):
    
#     """ Imports a JSON file as a dictionary
    
#     Parameters
#     ----------
#     path_to_file : str
#         A filepath to the JSON file which should be read in.

#     Returns
#     -------
#     data : dict
#         A dictionary with the data of the JSON file
    
#     """
#     import json
    
#     # Opening JSON file
#     f = open(path_to_file)
  
#     # Returns JSON object as a dictionary. NOTE: tuples will be converted to lists!
#     data = json.load(f)

#     # Closing file
#     f.close()
    
#     return data

# #%% Define function 'time_string'

# def time_string(timedelta):
    
#     """ The function reformats a timedelta (from package 'datetime') to seconds, minutes or hours
    
#     Examples
#     --------
#         >>> time_string(datetime.timedelta(seconds = 60))
#         1.0 min

#     Parameters
#     ----------
#     timedelta : datetime
#         The difference of two timestamps by 'datetime'

#     Returns
#     -------
#     str
#         Returns a reformatted string indicating the timedelta in seconds, minutes or hours

#     """
    
#     # Extract the total seconds of the timedelta
#     time = timedelta.total_seconds()
    
#     # Select whether to return seconds, minutes or hours, depending on the total seconds of the timedelta
#     # Return seconds, if total seconds is lower than 60
#     if time < 60:
#         return str(round(time, 0)) + " sec"
    
#     # Return minutes, if total seconds is lower than 3600
#     elif time < 3600:
#         return str(round(time/60, 1)) + " min"
    
#     # Else return hours
#     else:
#         return str(round(time/3600, 1)) + " hours"
    
# #%% Define function 'defragmentize'

# def defragmentize(text: str, character_to_split: list, strip_whitespaces: bool = True):
    
#     """ The function 'defragmentize' splits a string (= 'text') into various fragments. Where to split can be defined in 'character_to_split'.
#     Important: the function will not split characters inside brackets!
    
#     Examples
#     --------
#         >>> defragmentize(text = "A, B (C, D); E", character_to_split = [",", ";"], strip_whitespaces = True)
#         [A, B (C, D), E]
    
#     Parameters
#     ----------
#     text : str
#         The original text which should be split
    
#     character_to_split : list
#         A list with the characters where splitting should be applied.
    
#     strip_whitespaces : bool, optional
#         A boolean defining whether whitespaces at the start and end of produced fragments should be trimmed. The default is True.

#     Returns
#     -------
#     list
#         Returns a list with the fragments from the splitted 'text' variable.

#     """
    
#     # Evaluation whether the function input variables are of correct variable type
#     # Evaluation for 'character_to_split'
#     if not isinstance(character_to_split, list):
#         raise TypeError("Function input variable 'character_to_split' is not of type 'list'.")
    
#     # Evaluation for 'text'
#     if not isinstance(text, str):
#         raise TypeError("Function input variable 'text' is not of type 'str'.")
    
#     # Evaluation for 'strip_whitespaces'
#     if not isinstance(strip_whitespaces, bool):
#         raise TypeError("Function input variable 'strip_whitespaces' is not of type 'bool'.")
    
#     # Additionally, check if the characters provided in 'character_to_split' are only of length 1. If not, raise error
#     characters_to_split_with_length_greater_than_1 = [m for m in character_to_split if len(m) != 1]
#     if characters_to_split_with_length_greater_than_1 != []:
#         raise TypeError("One or more characters defined in 'character_to_split' have length other than 1. The function however only accepts characters with length 1. Problematic characters are: " + str(characters_to_split_with_length_greater_than_1))
    
#     # Initialies a list with the index 0 as the location to start the fragmentation in the text variable 'text'.
#     # The variable 'places_to_split' will gather all the locations (as an integer) where patterns stated in the variable 'character_to_split'
#     # appear in the text string to split (= variable 'text')
#     locations_to_split = [0]
    
#     # Initialies a boolean. The boolean variable 'do_not_split' indicates, whether the location of the pattern should be written to 'locations_to_split' or not.
#     # In the beginning, splitting should be done, so the variable is set to False. However, the variable can change through the iteration.
#     do_not_split = False
    
#     # Iterate through each letter in a string
#     for idx, letter in enumerate(text):
        
#         # If the letter does not appear in the character list that we want to split, go on
#         if letter not in character_to_split:
#             continue
        
#         # If the letter identifies an opening bracket, change the variable do_not_split to True. This indicates for the next iteration, that if a pattern appears that we want to split, it will not be splitted
#         if letter in ["(", "{", "["]:
#             locations_to_split.append(idx)
#             do_not_split = True
#             continue
        
#         # If the letter identifies a closing bracket, change the variable do_not_split again to False. Now patterns will be splitted again in the next iterations.
#         if letter in [")", "}", "]"]:
#             do_not_split = False
        
#         # If the variable not_to_split indicates that we don't want to split, go on.            
#         if do_not_split:
#             continue
        
#         # Otherwise, write the position of the letter to be split to the variable 'places_to_split'
#         else:
#             locations_to_split.append(idx)
    
#     # Add the number of the last location of a string
#     locations_to_split.append(len(text))
    
#     # Make a paired list from where to where text fragments should be split
#     locations_to_split_paired = [(locations_to_split[idx], locations_to_split[idx + 1]) for idx, m in enumerate(locations_to_split) if idx != len(locations_to_split)-1]
    
#     # Split the text according to the locations identified
#     text_split_orig = [text[m[0]:m[1]] if idx == 0 or idx == len(locations_to_split_paired) else text[(m[0] + 1):m[1]] for idx, m in enumerate(locations_to_split_paired)]
        
#     # Return the fragments. Trim the whitespaces if stated in the function header
#     if strip_whitespaces:
#         return [m.strip() for m in text_split_orig if m != '' and m != ' ']
#     else:
#         return [m for m in text_split_orig if m != '' and m != ' ']

#%% Define function 'progressbar'

def progressbar(iterable, prefix: str = "", size: int = 40, out = sys.stdout):
    
    """ Generates a generator object which prints a progress bar to the console.
    This function can be wrapped around other functions which are iterables in order to show the progress of the iterables.
    
    Parameters
    ----------
    iterable : iter
        Any type of iterable variable, e.g., list or tuple or other.

    prefix : str, optional
        Any text that should be printed at the beginning of the progress bar. The default is an empty string ("").

    size : (int | float), optional
        The length of the progress bar. In detail, how many '#' are shown. The default is 40.

    out : optional
        The default is sys.stdout.

    Yields
    ------
    item
        Yields item which is used to show progress bar.

    """
    
    # Length of the list to be iterated over
    count = len(iterable)
    
    # start = time.time()
    
    # Define what should be shown --> generating the progress bar
    def show(j):
        
        x = int(size * j / count)
        perc = str(round(j / count * 100, 0)) + "%"
        print(f"\r[{u'#'*x}{(' '*(size-x))}] {perc}", end = '\r', file = out, flush = True)
        
        # # The following lines could be used to also show the elapsed time
        # remaining = ((time.time() - start) / j) * (count - j)
        # mins, sec = divmod(remaining, 60)
        # time_str = f"{int(mins):02}:{sec:05.2f}"
        # print(f"\r{prefix}[{u'█'*x}{('.'*(size-x))}] {j}/{count} Est wait {time_str}", end='\r', file=out, flush=True)
    
    # Loopt through the iterable            
    for i, item in enumerate(iterable):
        
        # Print the prefix, if specified
        if i == 0 and prefix != "":
            print(prefix)
        
        yield item
        show(i + 1)
    
    # Flush console
    print("", flush = True, file = out)

    
