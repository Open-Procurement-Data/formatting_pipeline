import os
import pandas as pd
import sys
from datetime import datetime
import time

def get_cpv(cpv_input, dir_output, printing):
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

    try:
        cvp_numbers = pd.read_excel(cpv_input, usecols=['CODE', 'DE'])
        if printing is True:
            print(f"Processed {cpv_input}")
    except Exception as e:
        if printing is True:
            print(f"Error processing file {cpv_input}: {e}")

    return cvp_numbers

def extract_codes(df, code_column): 
    """
    Extract divisions, groups, classes, and categories from the given DataFrame based on the code rules.
    
    Parameters:
    df (pd.DataFrame): Input DataFrame containing the codes and descriptions.
    code_column (str): Column name containing the codes.
    """
    def get_division(code):
        return code[:2]
    
    def get_group(code):
        return code[:3]
    
    def get_class(code):
        return code[:4]
    
    def get_category(code):
        return code[:5]
    
    def get_classification(code):
        code = code.split('-')[0]
        if code.endswith('000000'):
            return 'division'
        elif code.endswith('00000'):
            return 'group'
        elif code.endswith('0000'):
            return 'class'
        elif code.endswith('000'):
            return 'category'
        else:
            return 'subclass'
        
    df['division'] = df[code_column].apply(get_division)
    df['group'] = df[code_column].apply(get_group)
    df['class'] = df[code_column].apply(get_class)
    df['category'] = df[code_column].apply(get_category)
    #definetly needed, the later 4 lines are obsolete (maybe)
    df['classification'] = df[code_column].apply(get_classification) 
    
    return df

def get_classification(cpv_numbers_list, cvp_numbers, classification = "division"):
    kurzel = []
    intermediate_cvp = cvp_numbers[cvp_numbers['classification'] == classification] # or 'division', 'group', 'class', 'category', 'subclass'

    for cpv_number in cpv_numbers_list:
        if pd.notna(cpv_number):
            for i in intermediate_cvp["CODE"]:
                if i[:2] == str(cpv_number)[:2]:
                    division_desc = intermediate_cvp[intermediate_cvp["CODE"] == i]["DE"].values[0]
                    kurzel.append(division_desc)
    return list(set(kurzel))

def save_new_files(dataframe, name, dir_output, printing):
    '''
    Saving the reformatted pandas.DataFrame to a csv.
    Only if a output directory path was given.
    '''
    if dir_output is None:
        if printing is True:
            print("No output directory specified. Skipping saving the DataFrame.")
        return
    
    if isinstance(dataframe, pd.DataFrame):
        current_date = datetime.now().strftime("%Y_%m_%d")
        csv_file_name = f"{current_date}_{name}.csv"
        csv_file_path = os.path.join(dir_output, csv_file_name)
        try:
            dataframe.to_csv(csv_file_path, index=False)
            if printing is True:
                print(f"Saved DataFrame to {csv_file_path}")
        except Exception as e:
            if printing is True:
                print(f"Error saving DataFrame to CSV: {e}")
            sys.exit(1)
    else:
        if printing is True:
            print(f"Invalid DataFrame for key {name}.")
        sys.exit(1)

def extract_text(entry, lang):
    if isinstance(entry, dict) and lang in entry:
        if isinstance(entry[lang], list) and len(entry[lang]) > 0:
            return entry[lang][0] 
        else:
            return entry[lang]
    elif isinstance(entry, list):
            return entry[0]
    return entry

def formatting_ted(dataframe, printing):
    for column in dataframe.keys():
        if printing is True:
            print(f"Starting extraction for column: {column}")

        start_time = time.time()
        if column == "buyer_locality":
            dataframe[column] = dataframe[column].apply(lambda x: extract_text(x, 'mul'))
        else:
            dataframe[column] = dataframe[column].apply(lambda x: extract_text(x, 'deu'))
        end_time = time.time()

        elapsed_time = end_time - start_time
        if printing is True:
            print(f"Time taken for extract_2 with column {column}: {elapsed_time:.2f} seconds")

    return dataframe
        


def get_new_dataframes(dataframes, cpv_input_dir, output_dir=None, printing=False):
    
    if not isinstance(printing, bool):
        raise ValueError("The 'printing' parameter must be a boolean value.")
    
    cvp_numbers = get_cpv(cpv_input_dir, output_dir, printing)

    df = extract_codes(cvp_numbers, 'CODE')

    dataframes["overView_Ted"]["classification"] = dataframes["overView_Ted"]["classification-cpv"].apply(lambda x: get_classification(x, cvp_numbers) if isinstance(x, list) else [])

    bescha_new = pd.DataFrame()
    ted_new = pd.DataFrame()

    # Equal column names for multiple DataFrames:

    bescha_new["tender_title"] = dataframes["overView_Bescha"]["tender.title"].copy()
    ted_new["tender_title"] = dataframes["overView_Ted"]["notice-title"].copy()

    bescha_new["tender_description"] = dataframes["overView_Bescha"]["tender.description"].copy()
    ted_new["tender_description"] = dataframes["overView_Ted"]["description-lot"].copy() 

    bescha_new["tender_cpv_number"] = dataframes["overView_Bescha"]["classification.id_tender.items_1"].copy()
    ted_new["tender_cpv_number"] = dataframes["overView_Ted"]["classification-cpv"].copy() 

    bescha_new["tender_cpv_category"] = dataframes["overView_Bescha"]["tender.mainProcurementCategory"].copy()
    ted_new["tender_cpv_category"] = dataframes["overView_Ted"]["classification"].copy()

    bescha_new["tender_numberOfTenderers"] = dataframes["overView_Bescha"]["tender.numberOfTenderers"].copy()
    ted_new["tender_numberOfTenderers"] = None # No column for that!

    bescha_new["buyer_name"] = dataframes["overView_Bescha"]["tender.procuringEntity.name"].copy()
    ted_new["buyer_name"] = dataframes["overView_Ted"]["organisation-name-buyer"].copy() 

    bescha_new["buyer_locality"] = dataframes["overView_Bescha"]["buyer.address.locality"].copy()
    ted_new["buyer_locality"] = dataframes["overView_Ted"]["buyer-city"].copy() 

    bescha_new["buyer_nut"] = dataframes["overView_Bescha"]["buyer.address.region"].copy() 
    ted_new["buyer_nut"] = dataframes["overView_Ted"]["buyer-country-sub"].copy() 

    bescha_new["contracts_value_amount"] = dataframes["overView_Bescha"]["value.amount_contracts_1"].copy()
    ted_new["contracts_value_amount"] = dataframes["overView_Ted"]["total-value"].copy() 

    bescha_new["contracts_value_amount"] = dataframes["overView_Bescha"]["description_tender.lots_1"].copy()
    ted_new["24_Lot_description"] = dataframes["overView_Ted"]["BT-24-Lot"].copy() 

    bescha_new["publication_number"] = None # There is no publication numbers, only some id. Not sure if its the same
    ted_new["publication_number"] = dataframes["overView_Ted"]["publicationNumber"].copy() 

    bescha_new["BT-05(a)-notice"] = dataframes["overView_Bescha"]["tender.awardPeriod.endDate"].copy() # This has the same format, but I am not sure, if it has the same date
    ted_new["BT-05(a)-notice"] = dataframes["overView_Ted"]["BT-05(a)-notice"].copy() # not sure which date this is.
    
    bescha_new["company_size"] = None # There is no company size in bescha
    ted_new["company_size"] = dataframes["overView_Ted"]["BT-165-Organization-Company"].copy() 

    bescha_new["BT-262-Lot"] = None # There are no other cpv numbers than classification.id_tender.items_1
    ted_new["BT-262-Lot"] = dataframes["overView_Ted"]["BT-262-Lot"].copy() 

    bescha_new["BT-27-Procedure"] = None # There is no estimated value
    ted_new["BT-27-Procedure"] = dataframes["overView_Ted"]["BT-27-Procedure"].copy() # This is estimated-value

    bescha_new["BT-27-Procedure"] = None # There is no winner in Bescha. Only suppliers, that i think are the winner. But tere can be multiple suppliers?
    ted_new["winner_name"] = dataframes["overView_Ted"]["winner-name"].copy()

    bescha_new["BT-27-Procedure"] = None # There is no winner in Bescha.
    ted_new["winner_post_code"] = dataframes["overView_Ted"]["winner-post-code"].copy()

    bescha_new["BT-27-Procedure"] = None # There is no winner in Bescha.
    ted_new["winner_size"] = dataframes["overView_Ted"]["winner-size"].copy()

    if printing is True:
        print(f"bescha_new has following columns: {bescha_new.keys()}")
        print(f"ted_new has following columns: {ted_new.keys()}")

    ted_new = formatting_ted(ted_new, printing)

    save_new_files(bescha_new, "bescha", output_dir, printing)
    save_new_files(ted_new, "ted", output_dir, printing)

    return bescha_new, ted_new, cvp_numbers
