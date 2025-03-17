import os
import pandas as pd
import json
from datetime import datetime
from channels import channels
import numpy as np
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
import calendar

class QualityChecker:
    def __init__(self, config_path='config/'):
        self.config_path = config_path
        self.units = self._load_units()
        self.red_fill = PatternFill(start_color='FFFF0000', end_color='FFFF0000', fill_type='solid')
        self.yellow_fill = PatternFill(start_color='FFFFFF00', end_color='FFFFFF00', fill_type='solid')
        
    def _load_units(self):
        """Load all unit configurations from the config directory"""
        units = {}
        for file in os.listdir(self.config_path):
            if file.endswith('.json'):
                with open(os.path.join(self.config_path, file), 'r') as f:
                    config = json.load(f)
                    units[config['unit_no']] = config
        return units

    def _check_data_quality(self, data, unit_config, channel_name):
        """Check data quality for a specific channel"""
        if not unit_config['channels'].get(channel_name, False):
            return None  # Channel not monitored for this unit
            
        channel_info = channels[channel_name]
        values = data[data.columns[data.columns.str.contains(channel_info.regex, regex=True)]]
        
        if values.empty:
            return None  # Channel not found in data
            
        values = values.iloc[:, 0]  # Take first matching column
        
        good_count = 0
        missing_count = 0
        bad_count = 0
        
        for value in values:
            if pd.isna(value) or value == "":
                missing_count += 1
            elif not isinstance(value, (int, float)):
                try:
                    value = float(value)
                except:
                    missing_count += 1
                    continue
                    
            if isinstance(value, (int, float)):
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

    def _apply_conditional_formatting(self, worksheet):
        """Apply conditional formatting to cells based on missing + bad percentage"""
        for row in worksheet.iter_rows(min_row=2):  # Skip header row
            for cell in row[1:]:  # Skip date column
                if cell.value:
                    # Extract counts from the formatted string
                    parts = cell.value.split(', ')
                    good = int(parts[0].split(': ')[1])
                    missing = int(parts[1].split(': ')[1])
                    bad = int(parts[2].split(': ')[1])
                    
                    total = good + missing + bad
                    if total > 0:
                        problem_percentage = (missing + bad) / total * 100
                        if problem_percentage > 5:
                            cell.fill = self.red_fill
                        elif problem_percentage > 1:
                            cell.fill = self.yellow_fill

    def process_data(self, data_path, unit_no, month, data_type):
        """Process data for a specific unit and month"""
        unit_config = self.units[unit_no]
        results = {}
        
        # Initialize results for all channels
        for channel_name in channels.keys():
            if unit_config['channels'].get(channel_name, False):
                results[channel_name] = [0, 0, 0]  # [good, missing, bad]
                
        # Read all CSV files for the unit and month
        unit_path = os.path.join(data_path, f'UNIT {unit_no}')
        if not os.path.exists(unit_path):
            return None
            
        for file in os.listdir(unit_path):
            # Check if file matches the data type and month
            if data_type in file and month in file and file.endswith('.csv'):
                try:
                    data = pd.read_csv(os.path.join(unit_path, file))
                    
                    # Check quality for each channel
                    for channel_name in channels.keys():
                        if unit_config['channels'].get(channel_name, False):
                            quality = self._check_data_quality(data, unit_config, channel_name)
                            if quality:
                                results[channel_name] = [x + y for x, y in zip(results[channel_name], quality)]
                except Exception as e:
                    print(f"Error processing file {file}: {str(e)}")
                    continue
                            
        return results # results[channel] = [good, missing, bad]

    def update_quality_report(self, data_type='Minute'):
        """Update quality reports for all units"""
        data_path = f'{data_type}_Data'  # Use Minute_Data or Hour_Data
        if not os.path.exists(data_path):
            print(f"No {data_path} directory found")
            return
            
        for unit_no in self.units.keys():
            unit_path = os.path.join(data_path, f'UNIT {unit_no}')
            if not os.path.exists(unit_path):
                print(f"No data directory found for Unit {unit_no}")
                continue

            report_file = f'quality_reports/unit_{unit_no}_{data_type.lower()}_quality.xlsx'
            os.makedirs('quality_reports', exist_ok=True)
            
            # Create DataFrames to store quality results - one for bad counts and one for missing counts
            monitored_channels = [ch for ch in channels.keys() if self.units[unit_no]['channels'].get(ch, False)]
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
            for file in os.listdir(unit_path):
                if file.endswith('.csv'):
                    try:
                        # Extract date from filename
                        date_str = file.split('_')[-1].replace('.csv', '')
                        if data_type == 'Minute':
                            # For minute data, use YYYY-MM-DD
                            if len(date_str) == 10:  # YYYY-MM-DD format
                                dates.add(date_str)
                        else:  # Hour data
                            # For hour data, use YYYY-MM
                            if len(date_str) >= 7:  # Ensure we have at least YYYY-MM
                                dates.add(date_str[:7])
                    except:
                        continue
            
            # Process each date's data
            new_data_added = False
            for date_str in dates:
                if date_str not in bad_df.index or date_str not in missing_df.index:
                    if data_type == 'Minute':
                        # For minute data, process single day
                        file_path = os.path.join(unit_path, f'Unit_{unit_no}_{date_str}.csv')
                        if not os.path.exists(file_path):
                            continue
                            
                        try:
                            data = pd.read_csv(file_path)
                            results = {}
                            
                            # Initialize results for all channels
                            for channel_name in channels.keys():
                                if self.units[unit_no]['channels'].get(channel_name, False):
                                    quality = self._check_data_quality(data, self.units[unit_no], channel_name)
                                    if quality:
                                        results[channel_name] = quality
                                        
                            if results:
                                # Extract bad and missing counts for each channel
                                bad_counts = {ch: results[ch][2] if results.get(ch) else 0 for ch in monitored_channels}
                                missing_counts = {ch: results[ch][1] if results.get(ch) else 0 for ch in monitored_channels}
                                
                                # Add to respective DataFrames
                                bad_df.loc[date_str] = bad_counts
                                missing_df.loc[date_str] = missing_counts
                                new_data_added = True
                        except Exception as e:
                            print(f"Error processing file {file_path}: {str(e)}")
                            continue
                    else:
                        # For hour data, process entire month
                        results = self.process_data(data_path, unit_no, date_str, data_type)
                        if results:
                            # Extract bad and missing counts for each channel
                            bad_counts = {ch: results[ch][2] if results.get(ch) else 0 for ch in monitored_channels}
                            missing_counts = {ch: results[ch][1] if results.get(ch) else 0 for ch in monitored_channels}
                            
                            # Add to respective DataFrames
                            bad_df.loc[date_str] = bad_counts
                            missing_df.loc[date_str] = missing_counts
                            new_data_added = True
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
            
            # Function to apply formatting based on percentage
            def apply_formatting_to_sheet(worksheet):
                for row in worksheet.iter_rows(min_row=2):  # Skip header row
                    for cell in row[1:]:  # Skip date column
                        if cell.value is not None:
                            try:
                                value = float(cell.value)
                                # Calculate total points based on data type
                                date_str = worksheet.cell(row=cell.row, column=1).value
                                if date_str:
                                    if data_type == 'Minute':
                                        total_points = 1440  # Points per day for minute data
                                    else:
                                        year, month = map(int, date_str.split('-'))
                                        days_in_month = calendar.monthrange(year, month)[1]
                                        total_points = days_in_month * 24  # Points per month for hour data
                                        
                                    percentage = (value / total_points) * 100
                                    if percentage > 5:
                                        cell.fill = self.red_fill
                                    elif percentage > 1:
                                        cell.fill = self.yellow_fill
                            except (ValueError, TypeError):
                                continue
            
            # Apply formatting to both sheets
            for sheet_name in ['Bad Values', 'Missing Values']:
                if sheet_name in wb.sheetnames:
                    apply_formatting_to_sheet(wb[sheet_name])
            
            wb.save(report_file)
            
            print(f"Updated quality report for Unit {unit_no} ({data_type} data)")

def main():
    checker = QualityChecker()
    checker.update_quality_report('Minute')
    checker.update_quality_report('Hour')

if __name__ == "__main__":
    main()
