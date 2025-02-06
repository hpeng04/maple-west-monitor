TIMEOUT_SEC = 60
import socket
socket.setdefaulttimeout(TIMEOUT_SEC)
import os
from unit import Unit
from alert import send_email
from log import Log
import json
import datetime
import re

# Deprecated function that loads config.csv
# def load_units(config_path: str) -> list[Unit]:
#     '''
#     initialize units from the config file

#     param: config_path: str: path to the config file
#     return: list[Unit]: list of units
#     '''
#     units = []
#     with open(config_path, 'r') as file:
#         lines = file.readlines()[1:] # Read all lines after header
#         for line in lines:
#             unit_no, block, ip_address, port, serial = line.strip().split(',')
#             unit = Unit(int(unit_no), int(block), ip_address, port, serial)
#             units.append(unit)
#     return units

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
    return units


def delete_log():
    '''
    Delete the log file
    '''
    try:
        os.remove('log.txt')
    except FileNotFoundError:
        pass

def compile_email_body(errors):
    '''
    Compile the email body from the list of errors

    param: errors: list[str]: list of errors
    return: str: email body
    '''
    body = f"Data errors detected in the following unit(s):\n"
    error_units = set()
    for error in errors:
        error_units.add(re.findall(r'\d+', error.split(':')[0])[0]) # Extract unit number from error message
    for unit in error_units:
        body += f"Unit {unit}\n"
    return body

def run_load_units():
    delete_log()
    errors = []
    units = load_units('config/')
    for unit in units:
        if not (unit.load_data('Data')): # True if data is loaded successfully
            continue
        unit_error = unit.check_quality() 
        if unit_error is None: # None if df is empty or data is not loaded
            continue
        errors += unit_error
        # if error len > 0, then send email and log to the user
    if len(errors) > 0:
        body = compile_email_body(errors)
        send_email(subject=f"Maple West Data Error(s) Detected", body=body, attachment='log.txt', to=['hhpeng@ualberta.ca'])

def run_download_units(email_to: list[str] = ['hhpeng@ualberta.ca'], save_files: bool = False):
    delete_log()
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    Log.write(f"{yesterday.strftime('%Y-%m-%d')}\n")
    errors = []
    units = load_units('config/')
    for unit in units:
        unit.download_minute_data()
        errors += unit.check_quality(save_files)
    # if error len > 0, then send email and log to the user
    if len(errors) > 0:
        body = compile_email_body(errors)
        send_email(subject=f"Maple West Data Error(s) Detected", body=body, attachment='log.txt', to=email_to)

def main():
    run_download_units(['hhpeng@ualberta.ca', 'by1@ualberta.ca'])

    # for unit in units:
    #     print(unit)
    #     try:
    #         unit.download_minute_data()
    #     except:
    #         print(f'{color.RED}Failed to load data for unit {unit}{color.END}')
    #         continue
    #     print(unit.data)

if __name__ == "__main__":
    # main()
    run_download_units()
    # run_load_units()


