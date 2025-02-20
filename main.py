TIMEOUT_SEC = 60
import socket
socket.setdefaulttimeout(TIMEOUT_SEC)
import os
from unit import Unit
from alert import send_email
from log import Log
import json
import datetime
from color import color
from download import download_all
import pandas as pd

MAX_WARNINGS = 25

def load_units(config_path: str) -> list[Unit]:
    '''
    Load unit config jsons from folder path

    param: config_path: str: path to the config file
    return: list[Unit]: list of units
    '''
    units = []
    for file in os.listdir(config_path):
        if file.endswith('.json'):
            with open(os.path.join(config_path, file), 'r') as f:
                unit = json.load(f)
                units.append(Unit(unit['unit_no'], unit['block'], unit['ip_address'], unit['port'], unit['serial'], unit['channels']))
    return sorted(units)


def delete_log():
    '''
    Delete the log file
    '''
    try:
        os.remove(Log.get_path())
    except FileNotFoundError:
        pass

def compile_email_body(units):
    '''
    Compile the email body from the list of errors

    param: errors: list[str]: list of errors
    return: str: email body
    '''
    body = f"Data errors detected in the following unit(s):\n"
    error_units = set()
    for unit in units:
        if len(unit.errors) > 0 or len(unit.warnings) > MAX_WARNINGS:
            error_units.add(unit)
        
    for unit in error_units:
        body += f"{unit}\n"
    return body

def run_load_units():
    delete_log()
    errors = []
    warnings = []
    max_warnings = 0
    units = load_units('config/')
    for unit in units:
        if not (unit.load_data('Data')): # True if data is loaded successfully
            print(f"{color.RED}{unit}: Failed to load data{color.END}")
            continue
        unit_errors, unit_warnings = unit.check_quality() 
        if unit_errors is None: # None if df is empty or data is not loaded
            continue
        errors += unit_errors
        warnings += unit_warnings
        max_warnings = max(max_warnings, len(unit_warnings))
        # if error len > 0, then send email and log to the user
    if len(errors) > 0 or max_warnings > MAX_WARNINGS:
        body = compile_email_body(units)
        send_email(subject=f"Maple West Data Quality Error(s) Detected", body=body, attachment=Log.get_path())

def run_download_units(save_files: bool = False):
    delete_log()
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    Log.write(f"{yesterday.strftime('%Y-%m-%d')}\n")
    errors = []
    warnings = []
    max_warnings = 0
    units = load_units('config/')
    for unit in units:
        unit.download_minute_data()
        unit.check_status()
        unit.check_space()
        unit_errors, unit_warnings = unit.check_quality(save_files)
        errors += unit_errors
        warnings += unit_warnings
        max_warnings = max(max_warnings, len(unit_warnings))
    # if error len > 0, then send email and log to the user
    if len(errors) > 0 or max_warnings > MAX_WARNINGS:
        body = compile_email_body(units)
        send_email(subject=f"Maple West Data Error(s) Detected", body=body, attachment=Log.get_path())

def check_missing_units():
    missing_units = {}
    units = load_units('config/')
    missing_data_path = 'missing_data.txt'
    if os.path.exists(missing_data_path):
        missing_data_df = pd.read_csv(missing_data_path)
        for _, row in missing_data_df.iterrows():
            unit_no = row['unit_no']
            if unit_no not in [unit.unit_no for unit in units]:
                missing_units[unit_no] = row.to_dict()
    if len(missing_units) > 0:
        print("Attempting to download missing data")
        for unit_no, date in missing_units.items():
            print(f"Downloading {unit_no} for {date} (WIP)")
            # download data for unit_no and date
            # if successful, remove from missing_data.txt
            # if not successful, log error
            pass
    else:
        print(f"{color.RED}Missing data file not found{color.END}")

def main():
    # download_all() # Separate independent function from the quality checking program
    run_download_units()

if __name__ == "__main__":
    main()
    # run_download_units(save_files=True)
    # run_load_units()
