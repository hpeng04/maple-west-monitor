import os
import pandas as pd
import re

INPUT_PATH = './Data'
OUTPUT_PATH = './Combined_Data'

class color:
    RED = '\033[91m'
    GREEN = '\033[92m'
    END = '\033[0m'

def natural_sort_key(s):
    return [int(text) if text.isdigit() else text.lower() for text in re.split('(\\d+)', s)]

""" Data combination."""
def combine_csv_files(input_folder):
    # List all CSV files in the input folder
    csv_files = []
    for root, _, files in os.walk(input_folder):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))
            else:
                print('File does not end in .csv:' + file)
    csv_files.sort(key=natural_sort_key)

    ### Filter out November files: Daylight Savings on Nov 2 adds one extra row to the data which is not accomodated for.
    # csv_files = [file for file in csv_files if not file.endswith('11.csv')]
    
    if not csv_files:
        print(f"{color.RED}No CSV files found in {input_folder}.{color.END}")
        return pd.DataFrame()
    
    # Initialize an empty list to store DataFrames
    dfs = []

    # Read each CSV file and append to the dfs list
    for file in csv_files:
        cols = pd.read_csv(file, nrows=1).columns.size
        print("Reading file: ", file)
        df = pd.read_csv(file, on_bad_lines=lambda x: x[:cols], engine='python')
        if df.iloc[0, 0] > df.iloc[1, 0]:
            df = df.iloc[::-1]
        dfs.append(df)
    # Concatenate all DataFrames in the dfs list
    combined_data = pd.concat(dfs, ignore_index=True)
    
    return combined_data

def save_to_csv(df, output_folder, unit_no):
    # Create the output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)
    
    # Save the DataFrame to a new CSV file in the output folder
    output_file_path = os.path.join(output_folder, f'Unit {unit_no} Combined.csv')
    df.to_csv(output_file_path, index=False)
    
    print(f"Combined CSV file saved successfully at {output_file_path}")

def combine_all(input_path, output_path):
    for _, dirs, _ in os.walk(input_path):
        for dir in dirs:
            in_path = os.path.join(input_path, dir)
            out_path = os.path.join(output_path, dir)
            unit_no = dir.split('_')[-1]
            df = combine_csv_files(in_path)
            if not df.empty:
                save_to_csv(df, out_path, unit_no)
                print(f"Unit {unit_no} combined successfully.")
            else:
                print(f"{color.RED}Unit {unit_no} could not be combined.{color.END}")
        break

if __name__ == '__main__':
    combine_all(INPUT_PATH, OUTPUT_PATH)
    # path = input("Enter the path to the folder containing the raw data or enter -1 to combine all: ")
    # if path == '-1':
    #     combine_all(INPUT_PATH, OUTPUT_PATH)
    # else:
    #     unit_no = input("Enter the unit number: ")
    #     df = combine_csv_files(path)
    #     if not df.empty:
    #         save_to_csv(df, OUTPUT_PATH, unit_no)
    #         print(f"Unit {unit_no} combined successfully.")
    #     else:
    #         print(f"{color.RED}Unit {unit_no} could not be combined.{color.END}")