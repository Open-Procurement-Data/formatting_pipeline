import json
import os
import re
import pandas as pd
import sys
import time
from datetime import datetime

def check_dir(directory, printing):
    '''
    Checking if rhe given paths are correct
    '''
    try:
        dir = directory
        if not os.path.isdir(dir):
            raise ValueError(f"Input directory does not exist: {dir}")
        if printing is True:
            print(f"Input directory exists: {dir}")
        return dir
    except Exception as e:
        if printing is True:
            print(f"Error accessing input directory: {e}")
        sys.exit(1)

def start_logs(dir_input, dir_output, printing):
    '''
    Beginning Logs for more info.
    Checking if the given paths are correct.
    Reading the cpv_numbers
    '''
    # check for output dir / create one if it does not exist
    if dir_output is not None:
        try:
            os.makedirs(dir_output, exist_ok=True)
            if printing is True:
                print(f"Output directory created or already exists: {dir_output}")
        except Exception as e:
            if printing is True:
                print(f"Error creating output directory: {e}")
            sys.exit(1)

    # check input dir
    data_dir = check_dir(dir_input, printing)

    return data_dir

def save_new_files(dataframes, dir_output, printing):
    '''
    Saving the reformatted pandas.DataFrame to a csv.
    Only if a output directory path was given.
    '''
    if dir_output is None:
        if printing is True:
            print("No output directory specified. Skipping saving the DataFrame.")
        return
    
    if dataframes is not None and isinstance(dataframes, dict):
        for frame_name, frame in dataframes.items():
            if isinstance(frame, pd.DataFrame):
                current_date = datetime.now().strftime("%Y_%m_%d")
                # CSV
                csv_file_name = f"{current_date}_{frame_name}.csv"
                csv_file_path = os.path.join(dir_output, csv_file_name)
                try:
                    frame.to_csv(csv_file_path, index=False)
                    if printing is True:
                        print(f"Saved DataFrame to {csv_file_path}")
                except Exception as e:
                    if printing is True:
                        print(f"Error saving DataFrame to CSV: {e}")
                    sys.exit(1)
                # JSON
                json_file_name = f"{current_date}_{frame_name}.json"
                json_file_path = os.path.join(dir_output, json_file_name)
                try:
                    frame.to_json(json_file_path, orient='records')
                    if printing is True:
                        print(f"Saved DataFrame to {json_file_path}")
                except Exception as e:
                    if printing is True:
                        print(f"Error saving DataFrame to JSON: {e}")
                    sys.exit(1)
            else:
                if printing is True:
                    print(f"Invalid DataFrame for key {frame_name}.")
                sys.exit(1)
    else:
        if printing is True:
            print("Invalid dictionary provided.")
        sys.exit(1)


def json_files_to_dataframes(directory):
    '''
    Returning a Dictionary of all pandas.DataFrames from one directory
    '''
    dataframes = {}
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            json_name = os.path.splitext(filename)[0]
            file_path = os.path.join(directory, filename)
            with open(file_path, 'r') as file:
                data = json.load(file)
                dataframes[json_name] = pd.DataFrame(data)
    return dataframes


def extract_column(df, column_name):
    '''
    Extracting one specific column
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

def formatting_dataframe(df, list_of_columns, printing):
    '''
    formatting a whole pandas.DataFrame
    '''
    exploded_df = df.explode('releases')

    result_df = pd.json_normalize(exploded_df['releases'])

    new_list = ['_' + kw for kw in list_of_columns]
    for column in list_of_columns:
        if printing is True:
            print(f"Starting extraction for column: {column}")

        start_time = time.time()
        result_df = extract_column(result_df, column)
        end_time = time.time()

        elapsed_time = end_time - start_time
        if printing is True:
            print(f"Time taken for extract_2 with column {column}: {elapsed_time:.2f} seconds")

        pattern = re.compile(r'^_[2-9]|\d_{2,}')

        columns_to_drop = [col for col in result_df.columns if pattern.search(col) and not any(keyword in col for keyword in new_list)]
        
        result_df = result_df.drop(columns=columns_to_drop)

        result_df.columns = [col[:-2] if ('_' + column) not in col else col for col in result_df.columns]

        result_df.columns = [col.rstrip('_') for col in result_df.columns]

        result_df = result_df.loc[:,~result_df.columns.duplicated()].copy()
    
    return result_df


def get_dataframes(data_dir, output_dir=None, printing=False):
    '''
    Processes data and returns DataFrames.
    '''

    if not isinstance(printing, bool):
        raise ValueError("The 'printing' parameter must be a boolean value.")
    
    data_dir = start_logs(data_dir, output_dir, printing)

    dataframes = json_files_to_dataframes(data_dir)

    first_layer_columns = ['parties', 'awards', 'contracts', 'tender.items', 'tender.lots']

    dataframes["overView_Bescha"] = formatting_dataframe(dataframes["overView_Bescha"], first_layer_columns, printing)

    if printing is True:
        print(f"Bescha shape: {dataframes['overView_Bescha'].shape}")

    save_new_files(dataframes, output_dir, printing)

    return dataframes
