import json
import os
import pandas as pd

def json_files_to_dataframes(directory):
    '''
    Returning a Dictionary of all pandas.DataFrames from one directory
    '''
    print(f"Loading data from this directory: {directory}")
    dataframes = {}
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            json_name = os.path.splitext(filename)[0]
            file_path = os.path.join(directory, filename)
            with open(file_path, 'r') as file:
                data = json.load(file)
                dataframes[json_name] = pd.DataFrame(data)

    keys = list(dataframes.keys())
    
    new_bescha = dataframes[keys[0]] if len(keys) > 0 else None
    new_ted = dataframes[keys[1]] if len(keys) > 1 else None
    return new_bescha, new_ted