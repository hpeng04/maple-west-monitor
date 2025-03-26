import os
import pandas as pd
import json
from datetime import datetime
from channels import channels
import numpy as np
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from openpyxl.formatting.rule import ColorScaleRule
from openpyxl.utils import get_column_letter
import calendar
from unit import Unit

# portable
# reusable
# maintainable
# testable
# goal of this program is to take one file and check the quality of the data
# for checking directories, another program should be used to sort and combine the files
# this program should take in one file and the unit config
# it should then analyze the enabled channels and check for missing data
# it should then output a report of the quality of the data

block_1 = [2804, 2806, 2808, 2810, 2812, 2814, 2816, 2818]
block_3 = [77, 78, 79, 80, 81, 82, 83, 84, 85, 86]

class QualityChecker:
    def __init__(self, config_path='config/'):
        self.units = self._load_units(config_path)
        self.red_fill = PatternFill(start_color='FFFF0000', end_color='FFFF0000', fill_type='solid')
        self.yellow_fill = PatternFill(start_color='FFFFFF00', end_color='FFFFFF00', fill_type='solid')
        
    def _load_units(self, config_path: str) -> list[Unit]:
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

    def _get_quality_report(self, unit, path=""):
        '''
        Find the quality report for a specific unit

        param: unit: str: unit number
        return: str: path to the quality report
        '''
        unit_no = unit.unit_no
        unit = [unit for unit in self.units if unit.unit_no == unit_no][0]
        monitored_channels = [channel for channel, key in unit.channels.items() if key == True]
        bad_df_daily = pd.DataFrame(columns=monitored_channels)
        missing_df_daily = pd.DataFrame(columns=monitored_channels)
        bad_df_monthly = pd.DataFrame(columns=monitored_channels)
        missing_df_monthly = pd.DataFrame(columns=monitored_channels)
        if path != "" and os.path.exists(path):
            try:
                with pd.ExcelFile(path) as xls:
                    if 'Daily Bad Values' in xls.sheet_names:
                        bad_df_daily = pd.read_excel(xls, 'Bad Values', index_col=0)
                    if 'Daily Missing Values' in xls.sheet_names:
                        missing_df_daily = pd.read_excel(xls, 'Missing Values', index_col=0)
                    if 'Monthly Bad Values' in xls.sheet_names:
                        bad_df_monthly = pd.read_excel(xls, 'Bad Values', index_col=0)
                    if 'Monthly Missing Values' in xls.sheet_names:
                        missing_df_monthly = pd.read_excel(xls, 'Missing Values', index_col=0)
            except Exception as e:
                print(f"Error reading existing report: {str(e)}")
        return (bad_df_daily, missing_df_daily, bad_df_monthly, missing_df_monthly)

    def check_data_quality(self, unit_no):
        '''
        Check the quality of the data
        '''
        unit = [unit for unit in self.units if unit.unit_no == unit_no][0]
        bad_df_daily, missing_df_daily, bad_df_monthly, missing_df_monthly = self._get_quality_report(unit)
        # Get list of dates from unit.data
        if not unit.load_data(f'Minute_Data/'):
            print(f'Unit {unit.unit_no} has no data')
            return ((bad_df_daily, missing_df_daily), (bad_df_monthly, missing_df_monthly))
        data = unit.data
        unit.data['Date'] = pd.to_datetime(unit.data['Date'])
        unit.data.set_index('Date', inplace=True)
        # dates = unit.data.index.to_pydatetime()
        # Group data by date (each group corresponds to one day)
        daily_groups = data.groupby(data.index.date)
        # Extract unique year-month combinations in the format: YYYY-MM
        unique_months = np.unique([date.strftime('%Y-%m') for date, _ in daily_groups])
        monitored_channels = [channel for channel, key in unit.channels.items() if key == True]
        # Process daily quality checks using vectorized operations
        for date, daily_data in daily_groups:
            for channel in monitored_channels:
                min_val = channels[channel].min_value
                max_val = channels[channel].max_value
                
                # Find the column matching the channel regex
                matching_cols = data.columns[data.columns.str.contains(channels[channel].regex, regex=True)]
                channel_name = matching_cols[0] if len(matching_cols) > 0 else None
                if channel_name is None:
                    continue

                # Create vectorized masks for bad and missing values
                bad_mask = (daily_data[channel_name] < min_val) | (daily_data[channel_name] > max_val)
                missing_mask = daily_data[channel_name].isna() | np.isinf(daily_data[channel_name])
                bad_values = bad_mask.sum()
                missing_values = missing_mask.sum()

                # Compute percentage assuming an expected 1440 data points per day
                bad_df_daily.loc[date, channel] = round(bad_values / 1440 * 100, 3)
                missing_df_daily.loc[date, channel] = round(missing_values / 1440 * 100, 3)
                    
        daily = (bad_df_daily, missing_df_daily)
        for month in unique_months:
            # Filter data for the current month
            monthly_data = data[data.index.strftime('%Y-%m') == month]
            # Process data for each enabled channel
            for channel in monitored_channels:
                min_val = channels[channel].min_value
                max_val = channels[channel].max_value
                # Extract year and month from the 'YYYY-MM' format
                year = int(month.split('-')[0])
                month_num = int(month.split('-')[1])
                # Get number of days in the month
                num_days = calendar.monthrange(year, month_num)[1]
                # Find the column matching the channel regex
                matching_cols = data.columns[data.columns.str.contains(channels[channel].regex, regex=True)]
                channel_name = matching_cols[0] if len(matching_cols) > 0 else None
                if channel_name is None:
                    continue

                # Create vectorized masks for bad and missing values
                bad_mask = (monthly_data[channel_name] < min_val) | (monthly_data[channel_name] > max_val)
                missing_mask = monthly_data[channel_name].isna() | np.isinf(monthly_data[channel_name])
                bad_values = bad_mask.sum()
                missing_values = missing_mask.sum()

                # Compute percentage using the expected number of data points
                bad_df_monthly.loc[month, channel] = round(bad_values / (1440 * num_days) * 100, 3)
                missing_df_monthly.loc[month, channel] = round(missing_values / (1440 * num_days) * 100, 3)
                monthly = (bad_df_monthly, missing_df_monthly)
        # Add new derived columns to bad_df and missing_df
        for df in [bad_df_monthly, missing_df_monthly]:
            # Sum Main Electricity 1 & 2 with percentage calculation
            elec_cols = df.columns[df.columns.str.contains('Main\\s*Electricity\\s*[12]\\s*(Watts)$', regex=True)]
            if len(elec_cols) >= 2:
                df['Total Electricity'] = df[elec_cols].sum(axis=1)/len(elec_cols)
            
            # Copy Natural Gas column
            if 'Natural Gas' in df.columns:
                df['Gas'] = df['Natural Gas']

        return (daily, monthly)

    def _format_quality_result(self, path):
        wb = load_workbook(path)
        
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            
            # Skip sheets with no data
            if ws.max_row <= 1 or ws.max_column <= 1:
                continue
                
            # Get the maximum row and column
            max_row = ws.max_row
            max_col = ws.max_column
            
            # Create range string for formatting (B2 to end)
            end_col_letter = get_column_letter(max_col)
            data_range = f"B2:{end_col_letter}{max_row}"
            
            try:
                # Create a color scale rule
                color_scale = ColorScaleRule(
                    start_type="num", start_value=0, start_color="FFFFFFFF",  # White for 0
                    end_type="num", end_value=100, end_color="FFFF0000"      # Red for 100
                )
                
                # Apply the color scale to the data range
                ws.conditional_formatting.add(data_range, color_scale)
            except Exception as e:
                print(f"Warning: Could not apply formatting to sheet '{sheet_name}': {str(e)}")
                
        wb.save(path)

    def update_quality_report(self, unit_no, dataframes):
        '''
        Update the quality report with the new data
        '''
        daily, monthly = dataframes
        if not os.path.exists(f'reports/UNIT {unit_no}'):
            os.makedirs(f'reports/UNIT {unit_no}')
        path = f'reports/UNIT {unit_no}/UNIT {unit_no} REPORT.xlsx'
        with pd.ExcelWriter(path) as writer:
            bad_df_daily, missing_df_daily = daily
            bad_df_daily.to_excel(writer, sheet_name='Daily Bad Values')
            missing_df_daily.to_excel(writer, sheet_name='Daily Missing Values')
            bad_df_monthly, missing_df_monthly = monthly
            bad_df_monthly.to_excel(writer, sheet_name='Monthly Bad Values')
            missing_df_monthly.to_excel(writer, sheet_name='Monthly Missing Values')
        try:
            self._format_quality_result(path)
        except Exception as e:
            print(f"Error formatting quality report: {str(e)}")
        print(f'Quality report for unit {unit_no} updated')

        return
    
def main():
    checker = QualityChecker()
    for unit in block_1+block_3:
        dataframes = checker.check_data_quality(unit)
        checker.update_quality_report(unit, dataframes)


if __name__ == "__main__":
    # checker = QualityChecker()
    # dataframes = checker.check_data_quality(2812)
    # checker.update_quality_report(2812, dataframes)
    main()
