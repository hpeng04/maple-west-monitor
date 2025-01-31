TIMEOUT_SEC = 60
import socket
socket.setdefaulttimeout(TIMEOUT_SEC)
import pandas as pd
import re
import os
from unit import Unit

def load_units(config_path: str) -> list[Unit]:
    '''
    initialize units from the config file

    param: config_path: str: path to the config file
    return: list[Unit]: list of units
    '''
    units = []
    with open(config_path, 'r') as file:
        lines = file.readlines()[1:] # Read all lines after header
        for line in lines:
            unit_no, block, ip_address, port, serial = line.strip().split(',')
            unit = Unit(int(unit_no), int(block), ip_address, port, serial)
            units.append(unit)
    return units

def delete_log():
    '''
    Delete the log file
    '''
    try:
        os.remove('log.txt')
    except FileNotFoundError:
        pass

def main():
    delete_log()
    units = load_units('config.csv')
    units[0].load_data('Data')
    print(units[0].data)
    units[0].check_quality()
    # for unit in units:
    #     print(unit)
    #     try:
    #         unit.download_minute_data()
    #     except:
    #         print(f'{color.RED}Failed to load data for unit {unit}{color.END}')
    #         continue
    #     print(unit.data)

if __name__ == "__main__":
    main()


