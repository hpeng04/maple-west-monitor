from datetime import datetime, timedelta
from log import Log
import pandas as pd
from color import color

# Function to increment a given time string by a specified number of minutes
def increment_time(time: str, minutes: int = 1) -> str:
    time_format = "%Y-%m-%d %H:%M:%S"
    time_obj = datetime.strptime(time, time_format)
    new_time_obj = time_obj + timedelta(minutes=minutes)
    return str(new_time_obj.strftime(time_format))

# Function to find the time step between two time strings and log the type of data (minute or hourly)
def find_time_step(initial_time: str, second_time: str, unit_no) -> int:
    time_format = "%Y-%m-%d %H:%M:%S"
    initial_time_obj = datetime.strptime(initial_time, time_format)
    second_time_obj = datetime.strptime(second_time, time_format)
    time_diff = second_time_obj - initial_time_obj
    
    # Convert time difference to minutes
    time_step = int(time_diff.total_seconds() / 60)

    # Log and print the type of data based on the time step
    if time_step == 1:
        Log.write(f'Unit {unit_no}: Minute data detected')
        print(f"Unit {unit_no}: Minute data detected")
    elif time_step == 60:
        Log.write(f'Unit {unit_no}: Hourly data detected')
        print(f"Unit {unit_no}: Hourly data detected")
    elif time_step == -1:
        Log.write(f'Unit {unit_no}: Data order is reversed, Minute data detected')
        print(f"{color.YELLOW}Unit {unit_no}: Data order is reversed, Minute data detected{color.END}")
        time_step = -1
    elif time_step == -60:
        Log.write(f'Unit {unit_no}: Data order is reversed, Hourly data detected')
        print(f"{color.YELLOW}Unit {unit_no}: Data order is reversed, Hourly data detected{color.END}")
        time_step = -60
    else:
        Log.write(f'Unit {unit_no}: Time step could not be determined')
        print(f"{color.YELLOW}Unit {unit_no}: Time step could not be determined{color.END}")
        time_step = 1
    
    return time_step

# Function to check for missing rows in a DataFrame and log errors
def check_missing_rows(df: pd.DataFrame, unit_no) -> pd.DataFrame:
    if df is None:
        return None, [], []
    errors = []
    bad_indices = []

    first_row = df.iloc[0]
    num_columns = len(first_row)

    initial_time = df.iloc[0, 0]
    second_row_time = df.iloc[1, 0]
    time_step = find_time_step(initial_time, second_row_time, unit_no)

    final_time = df.iloc[-1, 0]
    
    index = 0
    current_time = df.iloc[index, 0]
    expected_time = current_time

    while current_time != final_time:
        expected_time = increment_time(expected_time, time_step)
        index += 1
        current_time = df.iloc[index, 0]

        # Check for missing rows and log errors
        while current_time != expected_time:
            current_time_obj = datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S")
            expected_time_obj = datetime.strptime(expected_time, "%Y-%m-%d %H:%M:%S")

            if current_time_obj < expected_time_obj:
                Log.write(f'Unit {unit_no}: Data order error at index {index}')
                print(f"{color.RED}Unit {unit_no}: Data order error at index {index}{color.END}")
                errors.append(f"Unit {unit_no}: Data order error at index {index}")
                bad_indices.append(index)
                expected_time = str(current_time)
                break

            Log.write(f'Unit {unit_no}: Missing all data at {expected_time}')
            print(f"{color.RED}Unit {unit_no}: Missing all data at {expected_time}{color.END}")
            errors.append(f"Unit {unit_no}: Missing all data at {expected_time}")
            bad_indices.append(index)
            missing_row = [expected_time] + [""] * (num_columns - 1)
            df = pd.concat([df.iloc[:index], pd.DataFrame([missing_row], columns=df.columns), df.iloc[index:]]).reset_index(drop=True)
            expected_time = increment_time(expected_time, time_step)
            index += 1  # Adjust index to account for the inserted row

    return df, errors, bad_indices

# Function to check if values in a DataFrame column are within specified limits and log errors
def check_limits(regex, data, min_value, max_value, unit_no, bad_indices):
    errors = []
    try:
        column = data.filter(regex=regex).columns[0]
        values = data[column]
        for index, value in values.items():
            if index in bad_indices:
                continue
            if value == None or pd.isna(value) or value == "":  # Skip empty values
                print(f"{color.YELLOW}Unit {unit_no}: Missing data at index {index}, in {column}{color.END}")
                Log.write(f"Unit {unit_no}: Missing data at index {index}, in {column}")
                errors.append(f"Unit {unit_no}: Missing data at index {index}, in {column}")
            elif float(value) < min_value or float(value) > max_value:
                print(f"{color.YELLOW}Unit {unit_no}: {column} out of limits: Index {index}, Value: {value}, Limits: ({min_value}, {max_value}){color.END}")
                Log.write(f"Unit {unit_no}: {column} out of limits: Index {index}, Value: {value}, Limits: ({min_value}, {max_value})")
                errors.append(f"Unit {unit_no}: {column} out of limits: Index {index}, Value: {value}, Limits: ({min_value}, {max_value})")
        return errors
    except IndexError:
        print(f"{color.RED}Unit {unit_no}: Column not found: {regex}{color.END}")
        Log.write(f"***Unit {unit_no}: Column not found: {regex}")
        errors.append(f"***Unit {unit_no}: Column not found: {regex}")
        return errors

# Function to check if temperature values in a DataFrame column are within specified limits and log errors
def check_temperature(regex, data, min_value, max_value, unit_no, bad_indices):
    errors = check_limits(regex, data, min_value+0.01, max_value, unit_no, bad_indices)
    if f"Unit {unit_no}: Column not found: {regex}" in errors:
        return errors
    column = data.filter(regex=regex).columns[0]
    values = data[column] 
    # if sum(values) == 0: # ERROR: values not all float, some str
    #     print(f"{color.YELLOW}Unit {unit_no}: {column} all zero - Possible disconnection{color.END}")
    #     Log.write(f"***Unit {unit_no}: {column} all zero - Possible disconnection")
    #     errors.append(f"***Unit {unit_no}: {column} all zero - Possible disconnection")
    return errors
