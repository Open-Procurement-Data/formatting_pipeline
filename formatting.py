import json
import os
import re
import pandas as pd
import sys
import time
from datetime import datetime

DATA_DIR = None
OUTPUT_DIR = None
PRINTING = False

def check_paths():
    '''
    Beginning Logs for more info.
    Checking if the given paths are correct.
    '''
    # check for output dir / create one if it does not exist
    if OUTPUT_DIR is not None:
        try:
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            if PRINTING:
                print(f"Output directory created or already exists: {OUTPUT_DIR}")
        except Exception as e:
            if PRINTING:
                print(f"Error creating output directory: {e}")
            sys.exit(1)

    # check input dir
    try:
        data_dir = DATA_DIR
        if not os.path.isdir(data_dir):
            raise ValueError(f"Input directory does not exist: {data_dir}")
        if PRINTING:
            print(f"Input directory exists: {data_dir}")
    except Exception as e:
        if PRINTING:
            print(f"Error accessing input directory: {e}")
        sys.exit(1)

def save_new_files(dataframes):
    '''
    Saving the reformatted pandas.DataFrame to a csv.
    Only if a output directory path was given.
    '''
    if OUTPUT_DIR is None:
        if PRINTING:
            print("No output directory specified. Skipping saving the DataFrame.")
        return
    
    if dataframes is not None and isinstance(dataframes, dict):
        for frame_name, frame in dataframes.items():
            if isinstance(frame, pd.DataFrame):
                current_date = datetime.now().strftime("%Y_%m_%d")
                # CSV
                csv_file_name = f"{current_date}_{frame_name}.csv"
                csv_file_path = os.path.join(OUTPUT_DIR, csv_file_name)
                try:
                    frame.to_csv(csv_file_path, index=False)
                    if PRINTING:
                        print(f"Saved DataFrame to {csv_file_path}")
                except Exception as e:
                    if PRINTING:
                        print(f"Error saving DataFrame to CSV: {e}")
                    sys.exit(1)
                # JSON
                json_file_name = f"{current_date}_{frame_name}.json"
                json_file_path = os.path.join(OUTPUT_DIR, json_file_name)
                try:
                    frame.to_json(json_file_path, orient='records')
                    if PRINTING:
                        print(f"Saved DataFrame to {json_file_path}")
                except Exception as e:
                    if PRINTING:
                        print(f"Error saving DataFrame to JSON: {e}")
                    sys.exit(1)
            else:
                if PRINTING:
                    print(f"Invalid DataFrame for key {frame_name}.")
                sys.exit(1)
    else:
        if PRINTING:
            print("Invalid dictionary provided.")
        sys.exit(1)


def load_from_json():
    '''
    Returning a Dictionary of all pandas.DataFrames from one directory
    '''
    dataframes = {}
    for filename in os.listdir(DATA_DIR):
        if filename.endswith(".json"):
            json_name = os.path.splitext(filename)[0]
            file_path = os.path.join(DATA_DIR, filename)
            with open(file_path, 'r') as file:
                data = json.load(file)
                dataframes[json_name] = pd.DataFrame(data)
    return dataframes


def extract_column(df, column_name):
    '''
    Extracts and normalizes a specified column containing nested JSON data from a given DataFrame.
    '''
    df_copy = df.copy()

    exploded_parties_df = df_copy.explode(column_name)
    
    exploded_parties_df['parent_id'] = exploded_parties_df.index
    
    normalized_parties_df = pd.json_normalize(exploded_parties_df[column_name])

    combined_df = exploded_parties_df.reset_index(drop=True).join(normalized_parties_df.add_suffix('_' + column_name))
    
    combined_df['parent_id'] = exploded_parties_df['parent_id'].values
    
    combined_df.drop(columns=[column_name], inplace=True)
    
    combined_df['suffix'] = combined_df.groupby('parent_id').cumcount() + 1
    
    combined_df.set_index(['parent_id', 'suffix'], inplace=True)

    combined_df = combined_df.unstack().sort_index(level=1, axis=1)
    
    combined_df.columns = [f'{col[0]}_{col[1]}' for col in combined_df.columns]
    
    combined_df.reset_index(inplace=True)
    
    return combined_df

def formatting_bescha(df, list_of_columns):
    '''
    formatting bescha. Getting all the information out of "releases"
    '''
    exploded_df = df.explode('releases')

    result_df = pd.json_normalize(exploded_df['releases'])

    new_list = ['_' + kw for kw in list_of_columns]
    for column in list_of_columns:
        if PRINTING:
            print(f"Starting extraction for column: {column}")

        start_time = time.time()
        result_df = extract_column(result_df, column)
        end_time = time.time()

        elapsed_time = end_time - start_time
        if PRINTING:
            print(f"Time taken for extract_2 with column {column}: {elapsed_time:.2f} seconds")

        pattern = re.compile(r'^_[2-9]|\d_{2,}')

        columns_to_drop = [col for col in result_df.columns if pattern.search(col) and not any(keyword in col for keyword in new_list)]
        
        result_df = result_df.drop(columns=columns_to_drop)

        result_df.columns = [col[:-2] if ('_' + column) not in col else col for col in result_df.columns]

        result_df.columns = [col.rstrip('_') for col in result_df.columns]

        result_df = result_df.loc[:,~result_df.columns.duplicated()].copy()
    
    return result_df

def get_dataframes_from_json(data_dir, output_dir=None, printing=False):
    '''
    Processes data and returns DataFrames.
    '''
    global DATA_DIR, OUTPUT_DIR, PRINTING
    DATA_DIR = data_dir
    OUTPUT_DIR = output_dir
    PRINTING = printing

    if DATA_DIR is None:
        raise ValueError("The 'data_dir' parameter must be given.")

    if not isinstance(PRINTING, bool):
        raise ValueError("The 'printing' parameter must be a boolean value.")
    
    check_paths()

    dataframes = load_from_json()

    columns_to_extract = ['parties', 'awards', 'contracts', 'tender.items', 'tender.lots']

    dataframes["overView_Bescha"] = formatting_bescha(dataframes["overView_Bescha"], columns_to_extract)

    if printing:
        print(f"Bescha shape: {dataframes['overView_Bescha'].shape}")

    save_new_files(dataframes)

    return dataframes 


if __name__ == "__main__":
    get_dataframes_from_json(DATA_DIR, OUTPUT_DIR, PRINTING)