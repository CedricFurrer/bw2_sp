import pathlib
import os

if __name__ == "__main__":
    os.chdir(pathlib.Path(__file__).parent)

import uuid
import copy
import torch
import bw2io
import bw2data
import pathlib
import collections
import pandas as pd
from functools import partial
from sentence_transformers import SentenceTransformer, util
import helper as hp

starting: str = "------------"


#%% Change Brightway project directory path

def change_brightway_project_directory(BRIGHTWAY2_DIR: (pathlib.Path | str),
                                       verbose: bool = True):
    
    # Make variable check
    hp.check_function_input_type(change_brightway_project_directory, locals())
    
    # Print statement
    if verbose:
        print(starting + "BRIGHTWAY2_DIR is set to the following path:\n" + str(BRIGHTWAY2_DIR) + "\n")
    
    # Those lines are directly taken from Brightway
    # It constructs some fields new and sets the path according to the one given by ourselves
    bw2data.projects._base_data_dir = str(BRIGHTWAY2_DIR)
    bw2data.projects._base_logs_dir = os.path.join(str(BRIGHTWAY2_DIR), "logs")
    bw2data.projects.db = bw2data.sqlite.SubstitutableDatabase(os.path.join(str(BRIGHTWAY2_DIR), "projects.db"), [bw2data.project.ProjectDataset])
    bw2data.projects.set_current("default", update = False)

#%% Return the amount of unlinked flows from a list of datasets by type

def linking_summary_dictionary(db_var):
    
    # Taken from 'db.statistics()' from Brightway
    # Make variable check
    hp.check_function_input_type(linking_summary_dictionary, locals())
    
    # Initialize empty dictionary
    unique_unlinked = collections.defaultdict(set)
    
    # Loop through the database
    for ds in db_var:
        
        # Loop through all unlinked exchanges
        for exc in (e for e in ds.get("exchanges", []) if not e.get("input")):
            
            # Add 
            if exc["type"] == "biosphere":
                unique_unlinked[exc.get("type")].add(hp.format_values(tuple([exc[m] for m in ("name", "top_category", "sub_category", "location", "unit")]),
                                                                      case_insensitive = True,
                                                                      strip = True,
                                                                      remove_special_characters = False))
                
            else:
                unique_unlinked[exc.get("type")].add(hp.format_values(tuple([exc[m] for m in ("name", "unit", "location")]),
                                                                      case_insensitive = True,
                                                                      strip = True,
                                                                      remove_special_characters = False))
    
    # For each exchange type, get the length of unlinked exchanges 
    unique_unlinked = {k: len(v) for k, v in unique_unlinked.items()}
    
    return unique_unlinked


#%% Define function to add unlinked biosphere flows to a (new) database

def add_unlinked_flows_to_biosphere_database(db: bw2io.importers.base_lci.LCIImporter,
                                             biosphere_db_name_unlinked: str,
                                             biosphere_db_name: str,
                                             add_to_existing_database: bool = True,
                                             verbose: bool = True):
    
    # Make variable check
    hp.check_function_input_type(add_unlinked_flows_to_biosphere_database, locals())

    # Extract available datasets in the unlinked biosphere database
    if add_to_existing_database:
        
        # Load all flows from the background to a dictionary
        biosphere_unlinked_loaded = {(m["database"], m["code"]): dict(m.as_dict(), **{"exchanges": []}) for m in copy.deepcopy(bw2data.Database(biosphere_db_name_unlinked))}
        
        # Write list of existing biosphere flows, but only the fields extracted
        existing = [hp.format_values(tuple([v[m] for m in ("name", "top_category", "sub_category", "location", "unit")]),
                                     case_insensitive = True,
                                     strip = True,
                                     remove_special_characters = False) for _, v in biosphere_unlinked_loaded.items()]
    
    else:
        # Use empty lists if we do not want to keep the original biosphere flows
        biosphere_unlinked_loaded = {}
        existing = []
        
        # If we do not want to add to an existing database BUT the database we want to write to already exist, raise Error!
        if biosphere_db_name_unlinked in bw2data.databases:
            raise ValueError("The database '" + biosphere_db_name_unlinked + "' (provided as variable 'biosphere_new_name') is already registered as database. Either use another name or switch variable 'add_to_existing_database' to True")
    
    # Retrieve all unlinked biosphere flows
    unlinked_biosphere_flows = [exc for ds in db for exc in ds["exchanges"] if exc["type"] == "biosphere" and not exc.get("input")]
    
    # Make them unique --> remove all duplicates using the unique key feature of dicionaries
    # Also, write the keys to lowercase and strip whitespaces in order to remove the duplicates in the next step
    unique_unlinked_biosphere_flows = {hp.format_values(tuple([exc[m] for m in ("name", "top_category", "sub_category", "location", "unit")]),
                                                        case_insensitive = True,
                                                        strip = True,
                                                        remove_special_characters = False): exc for exc in unlinked_biosphere_flows}
    
    # Remove the unlinked biosphere flows which are already in the database
    unique_unlinked_biosphere_flows_dup_removed = {k: v for k, v in unique_unlinked_biosphere_flows.items() if k not in existing}
    
    # Raise error, if the biosphere database is empty or not found
    if len(list(bw2data.Database(biosphere_db_name))) == 0:
        raise ValueError("Biosphere database '" + str(biosphere_db_name) + "' not existing or empty")
    
    # Extract all keys that should be kept based on an already existing, random biosphere flow
    keep_keys = list(bw2data.Database(biosphere_db_name).random().as_dict().keys())
    
    # Clean the biosphere flow dictionaries so that only fields are kept which are also kept in the existing biosphere
    unlinked_biosphere_flows_cleaned = [{k: v for k, v in exc.items() if k in keep_keys} for _, exc in unique_unlinked_biosphere_flows_dup_removed.items()]
    
    # Adapt or add some fields, e.g. such as the database or the code
    unlinked_biosphere_flows_adapted = [dict(exc, **{"code": str(uuid.uuid4()),
                                                     "type": "natural resource" if exc["categories"][0].lower() in ["raw", "natural resource"] else "emission",
                                                     "database": biosphere_db_name_unlinked,
                                                     "exchanges": []}) for exc in unlinked_biosphere_flows_cleaned]
    
    # The final dictionary structure with tuple code and database as keys and the whole dictionary as value
    new = {(exc["database"], exc["code"]): exc for exc in unlinked_biosphere_flows_adapted}
    
    # Merge both, already existing and new biosphere flows
    biosphere_new_data = {**biosphere_unlinked_loaded, **new}
    
    # Delete database, if it exists
    # Reason: we need to delete it first and write it afterwards again
    if biosphere_db_name_unlinked in bw2data.databases:
        
        # Print statement
        if verbose:
            print(starting + "Delete database first: " + biosphere_db_name_unlinked)
        
        # Delete database
        del bw2data.databases[biosphere_db_name_unlinked]
        
        # Add line to console
        if verbose:
            print("")
    
    
    # Print statement
    if verbose:
        print(starting + "Write database: " + biosphere_db_name_unlinked)
    
    # Register the new database
    bw2data.Database(biosphere_db_name_unlinked).register()
        
    # Write the new flows to the new biosphere
    bw2data.Database(biosphere_db_name_unlinked).write(biosphere_new_data)
    
    # ... and link again
    db.apply_strategy(partial(bw2io.utils.ExchangeLinker.link_iterable_by_fields,
                              other = (m for m in bw2data.Database(biosphere_db_name_unlinked)),
                              fields = ("name", "top_category", "sub_category", "location", "unit"),
                              internal = False,
                              kind = {"biosphere"},
                              relink = False
                              ), verbose = verbose)
    
    return biosphere_new_data    


#%% A function that uses the SBERT model to find the best match from a list

def map_using_SBERT(items_to_map: tuple, items_to_map_to: tuple, max_number: int = 1):
    
    # Check function input type
    hp.check_function_input_type(map_using_SBERT, locals())
    
    # Path to where model might lay
    path: pathlib.Path = pathlib.Path(__file__).parent / "defaults" / "all-MiniLM-L6-v2"
    
    # Check if path exists
    if path.exists():
        model = SentenceTransformer(str(path))
    
    else:
        # model = SentenceTransformer("all-mpnet-base-v2")
        model = SentenceTransformer("all-MiniLM-L6-v2")

    # Compute embedding for both lists
    embeddings1 = model.encode(items_to_map, convert_to_tensor=True)
    embeddings2 = model.encode(items_to_map_to, convert_to_tensor=True)

    # Compute cosine-similarities
    cosine_scores = util.cos_sim(embeddings1, embeddings2)

    # Initialize variable
    data = []

    # Loop through each item from 'items_to_map' and extract the elements that were mapped to it with its respective cosine score
    for idx_I, scores_tensor in enumerate(cosine_scores):
        
        # Extract the scores (values) and its respective location (indice)
        values, indices = torch.sort(scores_tensor, descending = True)
        
        # Add the n (max_number) mapped items with the highest cosine score to the list
        data += [{"orig": items_to_map[idx_I],
                  "mapped": items_to_map_to[indice],
                  "score": float(value),
                  "ranking": idx} for idx, value, indice in zip(reversed(range(1, max_number + 1)), values[0:max_number], indices[0:max_number])]

    return pd.DataFrame(data)

    
