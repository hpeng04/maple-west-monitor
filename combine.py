import os
import pandas as pd
import unit

# combine pandas dataframe csv files
# dir_path: directory path containing csv files
# output: UNIT {unit_no} DATA.csv
def combine(dir_path, output_path):
    # get all csv files
    unit_no = dir_path.rstrip('/').split(' ')[-1]
    files = os.listdir(dir_path)
    files = [f for f in files if f.endswith('.csv')]

    # combine all csv files
    data = []
    for f in files:
        data.append(pd.read_csv(dir_path + f, on_bad_lines='skip'))
    data = pd.concat(data)
    Unit = unit.Unit()
    data = Unit.sort_data(data)

    # save combined data
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    data.to_csv(output_path + f'UNIT {unit_no} DATA.csv', index=False)
    print(f'UNIT {unit_no} combined data saved')

# combine all csv files in all directories
def combine_all():
    # get all directories

    block_1 = [2804, 2806, 2808, 2810, 2812, 2814, 2816, 2818]
    block_3 = [77, 78, 79, 80, 81, 82, 83, 84, 85, 86]
    dirs = [f'UNIT {unit_no}' for unit_no in block_1 + block_3]

    # combine all csv files in all directories
    for d in dirs:
        combine(f'Minute_Data/{d}/', 'combined/')
    print('All data combined')

if __name__ == '__main__':
    combine_all()

