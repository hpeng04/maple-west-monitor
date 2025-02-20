import pandas as pd
import re
import os
from datetime import datetime
from rules import check_missing_rows, check_total_energy
from channels import channels
from log import Log
from color import color
from urllib.request import urlopen
from bs4 import BeautifulSoup
from alert import send_email

class Unit:
    block_1 = [2804, 2806, 2808, 2810, 2812, 2814, 2816, 2818]
    block_3 = [77, 78, 79, 80, 81, 82, 83, 84, 85, 86]
    units = block_1 + block_3

    stack_units = [2818, 2820, 87, 77, 86, 78]

    yesterday = (datetime.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')

    def __init__(self, unit_no: int, block: int, ip_address: str, port: str, serial:str, channels:dict, data:pd.DataFrame = None):
        self.unit_no = unit_no
        self.block = block # Deprecated
        self.data = data
        self.ip_address = ip_address
        self.port = port
        self.serial = serial
        self.channels = channels
        self.warnings = []
        self.errors = []
                
    def __str__(self):
        return f"Unit {self.unit_no}"
    
    def __repr__(self):
        return f"Unit {self.unit_no}, Block {self.block}, {self.ip_address}:{self.port}/{self.serial}\n"
    
    def __eq__(self, other):
        return self.unit_no == other.unit_no
    
    def __lt__(self, other):
        return self.unit_no < other.unit_no
    
    def __hash__(self):
        return hash(self.unit_no)

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
    
    def _download(self, url:str):
        '''
        Download data from the given url

        param: url: str: url to download data from
        '''
        Log.write(f"Downloading Unit {self.unit_no}: {self.ip_address}:{self.port}")
        print(f"Downloading Unit {self.unit_no}: {self.ip_address}:{self.port}")
        try:
            self.data = self._fix_order(pd.read_csv(url, header=0, on_bad_lines='skip'))
        except:
            Log.write(f"Unit {self.unit_no}: Failed to download data from {url}\n\n")
            print(f"{color.RED}Unit {self.unit_no}: Failed to download data from {url}{color.END}")
            self.data = None
            self.errors += [f"Unit {self.unit_no}: Failed to download data from {url}"]
            yesterday = (datetime.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            Log.record_missing(self.unit_no, yesterday)
            

    def load_data(self, path:str):
        '''
        Load data from a csv file or a directory of csv files
        Used for testing purposes

        param: path: str: path to the csv file or directory
        '''
        if os.path.isdir(path):
            for dir_name in os.listdir(path):
                if dir_name == f'{self.unit_no}':
                    dir_path = os.path.join(path, dir_name)
                    all_files = [os.path.join(dir_path, f) for f in os.listdir(dir_path) if (os.path.isfile(os.path.join(dir_path, f)) and f.endswith('.csv'))]
                    all_files.sort(key=self._natural_sort_key)
                    all_files = [self._fix_order(pd.read_csv(f)) for f in all_files]
                    self.data = pd.concat((f for f in all_files), ignore_index=True)
        else:
            self.data = self._fix_order(pd.read_csv(path))
        if self.data is None:
            return False
        return True

    def download_minute_data(self, date=yesterday):
        '''
        Download data for the last day (minute data)
        '''
        # year, month, day = map(int, current_date.split('-')) # converts date to ints
        url = f'http://{self.ip_address}:{self.port}/index.php/pages/export/exportDaily/{self.serial}/{date}'
        self._download(url)

    def download_hourly_data(self):
        '''
        Download data for the last month (hourly data)
        '''
        current_date = datetime.now().strftime('%Y-%m') # gets date in YYYY-MM format as a str
        year, month, _ = map(int, current_date.split('-')) # converts date to ints
        url = f'http://{self.ip_address}:{self.port}/index.php/pages/export/exportMonthly/{self.serial}/{current_date}'
        self._download(url)

    def check_space(self):
        '''
        Check the space available on the sd card
        '''
        url = f'http://{self.ip_address}:{self.port}/index.php/powerdisplay/getmainwatts'
        page = urlopen(url)
        html_bytes = page.read()
        html = html_bytes.decode("utf-8")
        soup = BeautifulSoup(html, 'html.parser')
        outer_span = soup.find_all("span", title='\\"Total')[-1]
        
        inner_span = outer_span.find("span")
        space = inner_span.text.split('<')[0]
        if is_float(space) and float(space) > 1 and float(space) < 40:
            Log.write(f"Unit {self.unit_no}: {space} GB left on the SD card")
            print(f"Unit {self.unit_no}: {space} GB left on the SD card")
        else:
            # self.errors += [f"Unit {self.unit_no}: Less than 1GB of space available on the SD card"]
            Log.write(f"Unit {self.unit_no}: Less than 1GB of space available on the SD card: {space} GB")
            print(f"{color.RED}Unit {self.unit_no}: {space} GB left on the SD card, clear storage{color.END}")
            body = f"Unit {self.unit_no}: Less than 1GB of space available on the SD card\n\n{space} GB left\n\nhttp://{self.ip_address}:{self.port}"
            send_email(subject=f"Maple West SD Card Storage Almost Full", body=body)

    def check_status(self):
        '''
        Check the status of the dashbox
        '''
        url = f'http://{self.ip_address}:{self.port}/index.php/powerdisplay/getmainwatts'
        page = urlopen(url)
        html_bytes = page.read()
        html = html_bytes.decode("utf-8")
        soup = BeautifulSoup(html, 'html.parser')
        status_logo = soup.find("img")
        # print(status_logo)
        if status_logo:
            img_src = status_logo['src']
            # print(img_src)
            if 'green' in img_src:
                Log.write(f"Unit {self.unit_no}: Dashbox Status OK")
                print(f"{color.GREEN}Unit {self.unit_no}: Dashbox Status OK{color.END}")
            else:
                Log.write(f"Unit {self.unit_no}: Dashbox Status Error")
                print(f"{color.RED}Unit {self.unit_no}: Dashbox Status Error{color.END}")
                body = f"Unit {self.unit_no}: Dashbox Status Error\n\nhttp://{self.ip_address}:{self.port}"
                send_email(subject=f"Maple West Dashbox Status Errors Detected", body=body)
                # self.errors.append(f"Unit {self.unit_no}: Dashbox Status Error")
        else:
            print("Something went wrong with status check")
        # print(status_logo)

    def check_quality(self, date=yesterday, save_files:bool = False):
        '''
        Check the quality of the data using the rules provided

        param: rules: list[DataQualityRule]: list of rules to be applied
        return: None
        '''
        if self.data is None:
            return self.errors, self.warnings
        
        Log.write(f"Checking Unit {self.unit_no}: {self.ip_address}:{self.port}")
        print(f"Checking Unit {self.unit_no}: {self.ip_address}:{self.port}")
        self.data, missing_row_errors, missing_row_warnings, bad_indices = check_missing_rows(self.data, self.unit_no)
        self.errors += missing_row_errors
        self.warnings += missing_row_warnings

        energy_errors, energy_warnings = check_total_energy(self.data, self.unit_no)
        self.errors += energy_errors
        self.warnings += energy_warnings

        for channel in self.channels:
            if self.channels[channel] == True:
                # use the channel check quality function
                channel_errors, channel_warnings = channels[channel].check_channel(self.data, self.unit_no, bad_indices)
                self.errors += channel_errors
                self.warnings += channel_warnings
        if len(self.errors) == 0 and len(self.warnings) == 0:
            print(f"{color.GREEN}Unit {self.unit_no}: Passed all quality checks{color.END}")
            Log.write(f"Unit {self.unit_no}: Passed all quality checks")

        if not os.path.exists(f'Data/{self.unit_no}'):
            os.makedirs(f'Data/{self.unit_no}')
        if save_files:
            self.data.to_csv(f'Data/{self.unit_no}/Unit_{self.unit_no}_{date}.csv', index=False)
        Log.write("\n")
        return self.errors, self.warnings
    
def is_float(value):
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False