from datetime import datetime, timedelta
from log import Log
import pandas as pd
from color import color
import numpy as np

'''
Log error format:
Unit {unit_no}: {date}, Index {index}: {error_message}
'''

# Function to increment a given time string by a specified number of minutes
def increment_time(time: str, minutes: int = 1) -> str:
    time_format = "%Y-%m-%d %H:%M:%S"
    if type(time) == str:
        time_obj = datetime.strptime(time, time_format)
    else:
        time_obj = time
    new_time_obj = time_obj + timedelta(minutes=minutes)
    return new_time_obj

# Function to find the time step between two time strings and log the type of data (minute or hourly)
def find_time_step(initial_time, second_time, unit_no) -> int:
    time_format = "%Y-%m-%d %H:%M:%S"
    if type(initial_time) == str:
        initial_time_obj = datetime.strptime(initial_time, time_format)
    else:
        initial_time_obj = initial_time
    if type(second_time) == str:
        second_time_obj = datetime.strptime(second_time, time_format)
    else:
        second_time_obj = second_time
    time_diff = second_time_obj - initial_time_obj
    
    # Convert time difference to minutes
    time_step = int(time_diff.total_seconds() / 60)

    # Log and print the type of data based on the time step
    if time_step == 1:
        pass
        # Log.write(f'Unit {unit_no}: Minute data detected')
        # print(f"Unit {unit_no}: Minute data detected")
    elif time_step == 60:
        pass
        # Log.write(f'Unit {unit_no}: Hourly data detected')
        # print(f"Unit {unit_no}: Hourly data detected")
    elif time_step == -1:
        Log.write(f'Unit {unit_no}: Data order is reversed')
        print(f"{color.YELLOW}Unit {unit_no}: Data order is reversed{color.END}")
        time_step = -1
    elif time_step == -60:
        pass
        # Log.write(f'Unit {unit_no}: Data order is reversed, Hourly data detected')
        # print(f"{color.YELLOW}Unit {unit_no}: Data order is reversed, Hourly data detected{color.END}")
        # time_step = -60
    else:
        Log.write(f'Unit {unit_no}: Time step could not be determined')
        print(f"{color.YELLOW}Unit {unit_no}: Time step could not be determined{color.END}")
        time_step = 1
    
    return time_step

# Function to check for missing rows in a DataFrame and log errors
def check_missing_rows(data: pd.DataFrame, unit_no):
    errors = []
    warnings = []
    bad_indices = []  # you can still track indices/log positions if needed

    # Ensure the first column is datetime
    time_format = "%Y-%m-%d %H:%M:%S"
    data['Timestamp'] = pd.to_datetime(data.iloc[:, 0], format=time_format, errors='coerce')
    data = data.dropna(subset=['Timestamp'])
    data.sort_values('Timestamp', inplace=True)
    data.set_index('Timestamp', inplace=True)

    # Determine expected time-step using the first two entries
    timestamps = data.index
    if len(timestamps) < 2:
        Log.write(f"Unit {unit_no}: Not enough data to determine time step.")
        return data.reset_index(), errors, warnings, bad_indices

    time_diff = (timestamps[1] - timestamps[0]).total_seconds() / 60
    time_step = int(time_diff) if time_diff > 0 else 1

    # Create complete date range with the expected frequency (minutes)
    full_index = pd.date_range(start=data.index.min(), end=data.index.max(), freq=f'{time_step}min')

    # Find which timestamps are missing
    missing_timestamps = full_index.difference(data.index)
    if not missing_timestamps.empty:
        for ts in missing_timestamps:
            message = f"Unit {unit_no}: Missing data for timestamp {ts.strftime(time_format)}"
            Log.write(message)
            print(f"{color.YELLOW}{message}{color.END}")
            errors.append(message)  # Or warnings, depending on your logic
    # Remove duplicate timestamps, keeping the first occurrence
    data = data[~data.index.duplicated(keep='first')]
    # Reindex once, which is much more efficient than looping
    data = data.reindex(full_index)
    # Optionally, fill missing rows (e.g., with empty strings or NaN)
    # data = data.fillna("") 

    # Reset index and rename it to preserve the column name
    data.reset_index(inplace=True)
    data.rename(columns={'index': 'Date'}, inplace=True)
    return data, errors, warnings, bad_indices

def check_total_energy(data, unit_no):
    errors = []
    warnings = []


    # iterate through data rows
    for index, row in data.iterrows():
        main_elec_cols = row.filter(regex='Main\\s*Electricity(?!\\s*Gen).*(Watts)$')
        pv_cols = row.filter(regex="PV.*(Watts)$")
        energy_generated = pd.to_numeric(main_elec_cols).sum(skipna=True) + pd.to_numeric(pv_cols).sum(skipna=True)        
        elec_cols = row.filter(regex="^(?!.*Gen\\s).*(Watts)$")
        energy_consumed = pd.to_numeric(elec_cols).sum(skipna=True) - energy_generated

        # Round the energy values to 2 decimal places
        energy_generated = round(energy_generated, 2)
        energy_consumed = round(energy_consumed, 2)

        diff = np.float64(energy_generated) - np.float64(energy_consumed)
        if energy_generated*1.05 >= energy_consumed or abs(diff) < 10: # 1% tolerance
            pass
        else: # 5% tolerance
            Log.write(f"Unit {unit_no}: {data.iloc[index, 0]} Index {index}: Energy Consumed: {energy_consumed} > Energy Generated: {energy_generated}")
            print(f"{color.YELLOW}Unit {unit_no}: {data.iloc[index, 0]} Index {index}: Energy Consumed: {energy_consumed} > Energy Generated: {energy_generated}{color.END}")
            errors.append(f"{data.iloc[index, 0]} Index {index}: Energy Consumed: {energy_consumed} > Energy Generated: {energy_generated}")
        # else:
        #     Log.write(f"Unit {unit_no}: {data.iloc[index, 0]} Index {index}: Energy Consumed: {energy_consumed} > Energy Generated: {energy_generated}")
        #     print(f"{color.RED}Unit {unit_no}: {data.iloc[index, 0]} Index {index}: Energy Consumed: {energy_consumed} > Energy Generated: {energy_generated}{color.END}")
        #     errors.append(f"{data.iloc[index, 0]} Index {index}: Energy Consumed: {energy_consumed} > Energy Generated: {energy_generated}")
    return errors, warnings

# Function to check if values in a DataFrame column are within specified limits and log errors
def check_limits(regex, data, min_value, max_value, unit_no, bad_indices):
    errors = []
    warnings = []
    try:
        column = data.filter(regex=regex).columns[0]
        values = data[column]
        column_name = column.lstrip("0123456789- ")
        null_counter = 0
        limit_counter = 0
        for index, value in values.items():
            if index in bad_indices:
                continue
            if value == None or pd.isna(value) or value == "":  # Skip empty values
                null_counter += 1
                # print(f"{color.YELLOW}Unit {unit_no}: {data.iloc[index, 0]} Index {index}: Missing data in {column_name}{color.END}")
                Log.write(f"Unit {unit_no}: {data.iloc[index, 0]} Index {index}: Missing data in {column_name}")
                if null_counter > 10:
                    errors.append(f"{data.iloc[index, 0]} Multiple missing data in {column_name}")
                else:
                    warnings.append(f"{data.iloc[index, 0]} Index {index}: Missing data in {column_name}")
            elif float(value) < min_value or float(value) > max_value:
                limit_counter += 1
                # print(f"{color.YELLOW}Unit {unit_no}: {data.iloc[index, 0]} Index {index}: {column_name} out of limits, Value: {value}, Limits: ({min_value}, {max_value}){color.END}")
                Log.write(f"Unit {unit_no}: {data.iloc[index, 0]} Index {index}: {column_name} out of limits, Value: {value}, Limits: ({min_value}, {max_value})")
                if limit_counter > 2:
                    errors.append(f"{data.iloc[index, 0]} Index {index}: {column_name} out of limits, Value: {value}, Limits: ({min_value}, {max_value})")
                else:
                    warnings.append(f"{data.iloc[index, 0]} Index {index}: {column_name} out of limits, Value: {value}, Limits: ({min_value}, {max_value})")
            else:
                null_counter = 0
                limit_counter = 0
        return errors, warnings
    except IndexError:
        print(f"{color.RED}Unit {unit_no}: Column not found: {regex}{color.END}")
        Log.write(f"***Unit {unit_no}: Column not found: {regex}")
        errors.append(f"Column not found: {regex}")
        return errors, warnings

def check_activity(regex, data, unit_no):
    errors, warnings = [], []
    column = data.filter(regex=regex).columns[0]
    column_name = column.lstrip("0123456789- ")
    if pd.to_numeric(data[column]).sum(skipna=True) == 0:
        print(f"{color.YELLOW}Unit {unit_no}: {column_name} no response - Possible Disconnection{color.END}")
        Log.write(f"Unit {unit_no}: {column_name} no response - Possible Disconnection")
        errors.append(f"{column_name} no response - Possible Disconnection")
    return errors, warnings

def check_pulse(regex, data, min_value, max_value, unit_no, bad_indices):
    errors, warnings = check_limits(regex, data, min_value, max_value, unit_no, bad_indices)
    if f"Unit {unit_no}: Column not found: {regex}" in errors:
        return errors, warnings
    column = data.filter(regex=regex).columns[0]
    column_name = column.lstrip("0123456789- ")
    if pd.to_numeric(data[column]).sum(skipna=True) == 0:
        print(f"{color.YELLOW}Unit {unit_no}: {column_name} no response - Possible Disconnection{color.END}")
        Log.write(f"Unit {unit_no}: {column_name} no response - Possible Disconnection")
        errors.append(f"{column_name} no response - Possible Disconnection")
    return errors, warnings

def find_diff(regex ,data, unit_no, bad_indices):
    column = data.filter(regex=regex).columns[0]
    values = data[column]
    start_index = 0
    while pd.isna(values[start_index]) or values[start_index] == "":
        start_index += 1
    min_value = values[start_index]
    max_value = values[start_index]
    for index, value in values.items():
        if index in bad_indices:
            continue
        if value == None or pd.isna(value) or value == "":  # Skip empty values
            continue
        if float(value) < min_value:
            min_value = float(value)
        if float(value) > max_value:
            max_value = float(value)
    diff = abs(max_value - min_value)
    return min_value, max_value, diff


def check_water_pulse(regex, data, min_limit, max_limit, unit_no, bad_indices):
    errors, warnings = check_limits(regex, data, min_limit, max_limit, unit_no, bad_indices)
    if f"Unit {unit_no}: Column not found: {regex}" in errors:
        return errors, warnings
    min_value, max_value, diff = find_diff(regex, data, unit_no, bad_indices)
    if diff > 5:
        activity_errors, activity_warnings = check_activity(regex, data, unit_no)
        errors += activity_errors
        warnings += activity_warnings
    return errors, warnings