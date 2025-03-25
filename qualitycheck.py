import os
import pandas as pd
import json
from datetime import datetime
from channels import channels
import numpy as np
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from openpyxl.formatting.rule import ColorScaleRule
import calendar
from rules import check_missing_rows
from unit import Unit

## updates:
## changed load_units to load unit objects instead of config as it was before
## need to change how channels are read from units objects in check_quality and other methods

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

    def _check_data_quality(self, data, unit, channel_name):
        """Check data quality for a specific channel"""
        if unit.channels[channel_name] == False:
            return None  # Channel not monitored for this unit
            
        channel_info = channels[channel_name]
        values = data[data.columns[data.columns.str.contains(channel_info.regex, regex=True)]]
        
        if values.empty:
            return [0, 60*24, 0] # all data missing
            # return None  # Channel not found in data
            
        values = values.iloc[:, 0]  # Take first matching column
        
        good_count = 0
        missing_count = 0
        bad_count = 0
        
        for value in values:
            if str(value).isspace() or pd.isna(value) or np.isnan(value):
                missing_count += 1
            elif not isinstance(value, (int, float)):
                try:
                    value = float(value)
                except:
                    missing_count += 1
                    continue
                    
            elif isinstance(value, (int, float)):
                if channel_info.min_value <= value <= channel_info.max_value:
                    good_count += 1
                else:
                    bad_count += 1
                    
        return [good_count, missing_count, bad_count]

    def _format_quality_result(self, counts):
        """Format quality results as text"""
        if counts is None or len(counts) != 3:
            return "Good: 0, Missing: 0, Bad: 0"
        return f"Good: {counts[0]}, Missing: {counts[1]}, Bad: {counts[2]}"

    def update_quality_report(self, data_type='Minute'):
        """Update quality reports for all units"""
        data_path = f'{data_type}_Data'  # Use Minute_Data or Hour_Data
        if not os.path.exists(data_path):
            print(f"No {data_path} directory found")
            return
            
        for unit in self.units:
            unit_no = unit.unit_no
            unit_path = os.path.join(data_path, f'UNIT {unit_no}')
            if not os.path.exists(unit_path):
                print(f"No data directory found for Unit {unit_no}")
                continue

            report_file = f'quality_reports/unit_{unit_no}_{data_type.lower()}_quality.xlsx'
            os.makedirs('quality_reports', exist_ok=True)
            
            # Create DataFrames to store quality results - one for bad counts and one for missing counts
            monitored_channels = [ch for ch in unit.channels]
            bad_df = pd.DataFrame(columns=monitored_channels)
            missing_df = pd.DataFrame(columns=monitored_channels)
            
            # Load existing report if it exists
            if os.path.exists(report_file):
                try:
                    with pd.ExcelFile(report_file) as xls:
                        if 'Bad Values' in xls.sheet_names:
                            bad_df = pd.read_excel(xls, 'Bad Values', index_col=0)
                        if 'Missing Values' in xls.sheet_names:
                            missing_df = pd.read_excel(xls, 'Missing Values', index_col=0)
                except Exception as e:
                    print(f"Error reading existing report: {str(e)}")
            
            # Get dates from filenames
            dates = set()
            # Find the first file in the directory sorted by date
            csv_files = [file for file in os.listdir(unit_path) if file.endswith('.csv')]
            if csv_files:
                if data_type == 'Minute':
                    csv_files.sort(key=lambda x: datetime.strptime(x.split('_')[-1].replace('.csv', ''), '%Y-%m-%d'))
                else:
                    csv_files.sort(key=lambda x: datetime.strptime(x.split('_')[-1].replace('.csv', ''), '%Y-%m'))
                beginning_date = csv_files[0].split('_')[-1].replace('.csv', '')
                end_date = csv_files[-1].split('_')[-1].replace('.csv', '')
                print(f"Processing data for Unit {unit_no} from {beginning_date} to {end_date}")
            else:
                print(f"No CSV files found for Unit {unit_no}")
                continue
            
            date = beginning_date
            dates.add(beginning_date)
            while date != end_date:
                if data_type == 'Minute':
                    date_obj = datetime.strptime(date, '%Y-%m-%d')
                    next_date_obj = date_obj + pd.DateOffset(days=1)
                    date = next_date_obj.strftime('%Y-%m-%d')
                else:
                    date_obj = datetime.strptime(date, '%Y-%m')
                    next_date_obj = date_obj + pd.DateOffset(months=1)
                    date = next_date_obj.strftime('%Y-%m')
                dates.add(date)
            dates.add(end_date)
            
            # Process each date's data
            new_data_added = False
            for date_str in dates:
                if date_str not in bad_df.index or date_str not in missing_df.index:
                    if data_type == 'Minute':
                        missing_counts = {ch: 0 for ch in monitored_channels}
                        bad_counts = {ch: 0 for ch in monitored_channels}
                        # For minute data, process single day
                        file_path = os.path.join(unit_path, f'Unit_{unit_no}_{date_str}.csv')
                        if not os.path.exists(file_path):
                            # data is missing for the day
                            missing_counts = {ch: 24*60 for ch in monitored_channels}
                            bad_counts = {ch: 0 for ch in monitored_channels}
                            new_data_added = True

                        try:
                            data = pd.read_csv(file_path)
                            results = {}
                            
                            # Initialize results for all channels
                            _, _, _, missing_indices = check_missing_rows(data, -1)
                            num_missing = len(missing_indices)
                            for channel_name in channels.keys():
                                if unit.channels[channel_name] == True:
                                    quality = self._check_data_quality(data, unit, channel_name)
                                    if quality:
                                        results[channel_name] = quality
                                    results[channel_name][1] += num_missing
                                        
                            if results:
                                # Extract bad and missing counts for each channel
                                bad_counts = {ch: results[ch][2] if results.get(ch) else 0 for ch in monitored_channels}
                                missing_counts = {ch: results[ch][1] if results.get(ch) else 0 for ch in monitored_channels}
                        
                        except FileNotFoundError as e:
                            pass # if no file exists, then add missing to that day

                        except Exception as e:
                            # missing_counts = {ch: 24*60 for ch in monitored_channels}
                            # bad_counts = {ch: 0 for ch in monitored_channels}
                            print(f"Error processing file {file_path}: {str(e)}")
                        
                        # Add to respective DataFrames
                        bad_df.loc[date_str] = bad_counts
                        missing_df.loc[date_str] = missing_counts
                        new_data_added = True
                    # else:
                    #     # For hour data, process entire month
                    #     # results = self.process_data(data_path, unit_no, date_str, data_type)
                    #     if results:
                    #         # Extract bad and missing counts for each channel
                    #         bad_counts = {ch: results[ch][2] if results.get(ch) else 0 for ch in monitored_channels}
                    #         missing_counts = {ch: results[ch][1] if results.get(ch) else 0 for ch in monitored_channels}
                            
                    #         # Add to respective DataFrames
                    #         bad_df.loc[date_str] = bad_counts
                    #         missing_df.loc[date_str] = missing_counts
                    #         new_data_added = True
                else:
                    print(f"{date_str} already exists in report for Unit {unit_no}")
            
            if not new_data_added:
                print(f"No new data to add for Unit {unit_no}")
                continue
            
            # Save updated report with both sheets
            with pd.ExcelWriter(report_file, engine='openpyxl') as writer:
                bad_df.sort_index().to_excel(writer, sheet_name='Bad Values')
                missing_df.sort_index().to_excel(writer, sheet_name='Missing Values')
            
            # Apply conditional formatting to both sheets
            wb = load_workbook(report_file)
            ws = wb.active
            # Find the last row and column dynamically
            max_row = ws.max_row
            max_col = ws.max_column

            # Define the range dynamically
            table_range = f"B2:{ws.cell(row=max_row, column=max_col).coordinate}"
            
            # Function to apply formatting based on percentage
            def apply_formatting_to_sheet(worksheet):
                color_scale_rule = ColorScaleRule(
                    start_type="min", start_color="FFFFFF",  # White
                    end_type="max", end_color="FF0000"       # Red
                )
                worksheet.conditional_formatting.add(table_range, color_scale_rule)
            
            # Apply formatting to both sheets
            for sheet_name in ['Bad Values', 'Missing Values']:
                if sheet_name in wb.sheetnames:
                    apply_formatting_to_sheet(wb[sheet_name])
            
            wb.save(report_file)
            
            print(f"Updated quality report for Unit {unit_no} ({data_type} data)")

def main():
    checker = QualityChecker()
    checker.update_quality_report('Minute')
    # checker.update_quality_report('Hour')

if __name__ == "__main__":
    main()
