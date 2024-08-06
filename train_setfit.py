import sys
import pandas as pd
import numpy as np
import argparse
import logging
# --- SetFit --- #
from datasets import load_dataset
from setfit import SetFitModel, Trainer, TrainingArguments, sample_dataset
from sklearn.model_selection import train_test_split
from datasets import Dataset

# use case:
# python3 train_setfit.py -i ../new_data -c ../cpv_exel/cpv_2008_ver_2013.xlsx -s ../formatting_pipeline --load --test

def import_scripts(path):
    '''
        Importing the needed scripts based on their input location
    '''
    sys.path.append(path)
    import formatting
    import new_dataframes 
    import read_json
    return formatting, new_dataframes, read_json

def create_test_df(cpv_numbers, df):
    '''
        Creating an extra DataFrame with less entries for technical test purposes only.
        Divisions with more than 2 entries are searched for and the new DataFrame is filled with them.
    '''
    print(f"Starting to create a test DataFrame")
    division_codes = cpv_numbers[cpv_numbers['classification'] == "division"]["CODE"].tolist()
    division_codes = [code[:-2] for code in division_codes]
    divisions_with_more_entries = []

    for code in division_codes:
        count = df[df['tender_cpv_number'] == code].shape[0]
        if count > 2:
            divisions_with_more_entries.append(code)
    print(f"Found {len(divisions_with_more_entries)} divisions with more than 2 entries.")

    test_ted_df = pd.DataFrame
    test_ted_df = pd.DataFrame(columns=df.columns)

    for code in divisions_with_more_entries:
        entries_count = 0
        for index, row in df.iterrows():
            if row['tender_cpv_number'] == code and entries_count <= 4:
                entries_count = entries_count + 1
                test_ted_df = test_ted_df.append(row)

    print(f"Test DataFrame has {test_ted_df.shape[0]} rows")

    return test_ted_df


def main():
    parser = argparse.ArgumentParser(description="Load data and train with SetFit")
    parser.add_argument("-i", "--input", type=str, help="The directory path for the dataset")
    parser.add_argument("-c", "--cpv", type=str, help="The file path for the cpv numbers")
    parser.add_argument("-s", "--scripts", type=str, help="The path for the new_dataframe and formatting scripts")
    parser.add_argument("-l", "--load", action="store_true", help="Set to True to load the already formatted json dataset.", default=False)
    parser.add_argument("-t", "--test", action="store_true", help="Set to True to use a smaler dataset for test purpouses only.", default=False)

    args = parser.parse_args()

    if args.input is None:
        raise ValueError("The 'input' parameter must be given.")
    if args.cpv is None:
        raise ValueError("The 'cpv' parameter must be given.")
    if args.scripts is None:
        raise ValueError("The 'scripts' parameter must be given.")
    if not isinstance(args.test, bool):
        raise ValueError("The 'test' parameter must be a boolean value.")
    
    formatting, new_dataframes, read_json = import_scripts(args.scripts)

    if args.load:
        new_dataframes.CPV_DIR = args.cpv
        new_dataframes.OUTPUT_DIR = None
        new_dataframes.PRINTING = True
        cpv_numbers = new_dataframes.check_dir_get_cpv()

        df = new_dataframes.extract_cpv_codes(cpv_numbers, 'CODE')

        bescha_new, ted_new = read_json.json_files_to_dataframes("output_for_setfit")
    else:
        # formatting both datasets
        dataframes = formatting.get_dataframes_from_json(data_dir=args.input, output_dir=None, printing=True)

        bescha_new, ted_new, cpv_numbers = new_dataframes.get_equal_dataframes(dataframes, args.cpv, output_dir=None, printing=True)

    # get new smaller dataframe
    if args.test:
        test_ted_df = create_test_df(cpv_numbers, ted_new)
    else:
        print("No Test DataFrame is created. Training is running on the original Ted DataFrame")

    # train, val, test split
    train_df = temp_df = val_df = test_df = pd.DataFrame()

    if args.test:
        train_df, temp_df = train_test_split(test_ted_df, test_size=0.4, random_state=42)
    else:
        train_df, temp_df = train_test_split(ted_new, test_size=0.4, random_state=42)

    val_df, test_df = train_test_split(temp_df, test_size=0.5, random_state=42)

    print(f"Train dataset size: {train_df.shape}")
    print(f"Validation dataset size: {val_df.shape}")
    print(f"Test dataset size: {test_df.shape}")

    train_dataset = Dataset.from_pandas(train_df)
    val_dataset = Dataset.from_pandas(val_df)
    test_dataset = Dataset.from_pandas(test_df)

    # setfit model
    labels = cpv_numbers["CODE"].tolist()
    labels = [code[:-2] for code in labels]
    model = SetFitModel.from_pretrained(
        "sentence-transformers/paraphrase-mpnet-base-v2",
        labels=labels,
    )

    setfit_args = TrainingArguments(
        batch_size=16,
        num_epochs=4,
        evaluation_strategy="epoch",
        save_strategy="epoch",
        load_best_model_at_end=True,
    )

    trainer = Trainer(
        model=model,
        args=setfit_args,
        train_dataset=train_dataset,
        eval_dataset=val_dataset,
        metric="accuracy",
        column_mapping={"tender_description": "text", "tender_cpv_number": "label"}  # Map dataset columns to text/label expected by trainer
    )

    trainer.train()
    metrics = trainer.evaluate(test_dataset)
    print(metrics)

    for index, row in test_df.iterrows():
        preds = model.predict(row["tender_description"])
        print(f"Predictions: {preds}")
        print(f"Real Classifications: {row["tender_cpv_number"]}")
        print("----------------------------")


if __name__ == "__main__":
    main()
