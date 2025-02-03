import pandas as pd
import re
import os
from datetime import datetime
from rules import check_missing_rows
from channels import channels
from log import Log
from color import color

class Unit:
    block_1 = [2804, 2806, 2808, 2810, 2812, 2814, 2816, 2818]
    block_3 = [77, 78, 79, 80, 81, 82, 83, 84, 85, 86]

    stack_units = [2818, 2820, 87, 77, 86, 78]

    def __init__(self, unit_no: int, block: int, ip_address: str, port: str, serial:str, channels:dict, data:pd.DataFrame = None):
        self.unit_no = unit_no
        self.block = block # Deprecated
        self.data = data
        self.ip_address = ip_address
        self.port = port
        self.serial = serial
        self.channels = channels
                
    def __str__(self):
        return f"Unit {self.unit_no}, block{self.block}"
    
    def __repr__(self):
        return f"Unit {self.unit_no}, Block {self.block}, {self.ip_address}:{self.port}/{self.serial}\n"
    
    def _fix_order(self, df:pd.DataFrame):
        '''
        Sort the data is ascending order of time if not already sorted

        param: df: pd.DataFrame: data to be sorted
        return: pd.DataFrame: sorted data
        '''
        if df.iloc[0, 0] > df.iloc[1, 0]:
            return df.iloc[::-1]
        else:
            return df

    def _natural_sort_key(self, s):
        return [int(text) if text.isdigit() else text.lower() for text in re.split('(\\d+)', s)]

    def load_data(self, path:str):
        '''
        Load data from a csv file or a directory of csv files
        Used for testing purposes

        param: path: str: path to the csv file or directory
        '''
        if os.path.isdir(path):
            all_files = [os.path.join(path, f) for f in os.listdir(path) if (os.path.isfile(os.path.join(path, f)) and f.endswith('.csv'))]
            all_files.sort(key=self._natural_sort_key)
            all_files = [self._fix_order(pd.read_csv(f)) for f in all_files]
            self.data = pd.concat((f for f in all_files), ignore_index=True)
        else:
            self.data = self._fix_order(pd.read_csv(path))
    
    def _download(self, url:str):
        '''
        Download data from the given url

        param: url: str: url to download data from
        '''
        self.data = self._fix_order(pd.read_csv(url, header=0, on_bad_lines='skip'))

    def download_minute_data(self):
        '''
        Download data for the last day (minute data)
        '''
        current_date = datetime.now().strftime('%Y-%m-%d') # gets date in YYYY-MM-DD format as a str
        # year, month, day = map(int, current_date.split('-')) # converts date to ints
        url = f'http://{self.ip_address}:{self.port}/index.php/pages/export/exportDaily/{self.serial}/{current_date}'
        self._download(url)

    def download_hourly_data(self):
        '''
        Download data for the last month (hourly data)
        '''
        current_date = datetime.now().strftime('%Y-%m') # gets date in YYYY-MM format as a str
        year, month, _ = map(int, current_date.split('-')) # converts date to ints
        url = f'http://{self.ip_address}:{self.port}/index.php/pages/export/exportMonthly/{self.serial}/{current_date}'
        self._download(url)

    def check_quality(self):
        '''
        Check the quality of the data using the rules provided

        param: rules: list[DataQualityRule]: list of rules to be applied
        return: None
        '''
        errors = []
        self.data, missing_row_errors, bad_indices = check_missing_rows(self.data, self.unit_no)
        errors += missing_row_errors
        for channel in self.channels:
            if self.channels[channel] == True:
                # use the channel check quality function
                errors += channels[channel].check(self.data, self.unit_no, bad_indices)
                  
        if len(errors) == 0:
            print(f"{color.GREEN}Unit {self.unit_no} passed all quality checks{color.END}")
            Log.write(f"Unit {self.unit_no}: Passed all quality checks")

        Log.write("\n")
        return errors