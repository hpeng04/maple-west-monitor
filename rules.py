from datetime import datetime, timedelta
from log import Log
import pandas as pd
from color import color

def increase_time(time: str, minutes: int = 1) -> str:
    time_format = "%Y-%m-%d %H:%M:%S"
    time_obj = datetime.strptime(time, time_format)
    new_time_obj = time_obj + timedelta(minutes=minutes)
    return str(new_time_obj.strftime(time_format))

def find_time_step(initial_time: str, second_time: str, unit_no) -> int:
    time_format = "%Y-%m-%d %H:%M:%S"
    initial_time_obj = datetime.strptime(initial_time, time_format)
    second_time_obj = datetime.strptime(second_time, time_format)
    time_diff = second_time_obj - initial_time_obj
    
    time_step =  int(time_diff.total_seconds() / 60) # Convert to minutes

    if time_step == 1:
        Log.write(f'Unit {unit_no}: Minute data detected')
        print(f"{color.GREEN}Unit {unit_no}: Minute data detected{color.END}")
    elif time_step == 60:
        Log.write(f'Unit {unit_no}: Hourly data detected')
        print(f"{color.GREEN}Unit {unit_no}: Hourly data detected{color.END}")
    else:
        Log.write(f'Unit {unit_no}: Time step could not be determined')
        print(f"{color.YELLOW}Unit {unit_no}: Time step could not be determined{color.END}")
        time_step = 1
    
    return time_step

def check_missing_rows(df, unit_no) -> pd.DataFrame:
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

        expected_time = increase_time(expected_time, time_step)
        index += 1
        current_time = df.iloc[index, 0]

        # print(current_time, expected_time) # Debugging

        while current_time != expected_time:
            current_time_obj = datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S")
            expected_time_obj = datetime.strptime(expected_time, "%Y-%m-%d %H:%M:%S")

            if current_time_obj < expected_time_obj:
                Log.write(f'Unit {unit_no}: Data order error at index {index}')
                print(f"{color.RED}Unit {unit_no}: Data order error at index {index}{color.END}")
                expected_time = str(current_time)
                break

            Log.write(f'Unit {unit_no}: Missing row data at {expected_time}')
            missing_row = [expected_time] + [""] * (num_columns - 1)
            df = pd.concat([df.iloc[:index], pd.DataFrame([missing_row], columns=df.columns), df.iloc[index:]]).reset_index(drop=True)
            expected_time = increase_time(expected_time, time_step)
            index += 1  # Adjust index to account for the inserted row

    return df

class DataQualityRule:
    def __init__(self, name: str, check_function):
        self.name = name
        self.check_function = check_function
  
    def apply(self, data):
        return self.check_function(data)