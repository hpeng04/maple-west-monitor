import os
import pandas as pd
import json
from datetime import datetime
from channels import channels
import numpy as np
from openpyxl import load_workbook
from openpyxl.styles import PatternFill

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
                            
        return results

    def update_quality_report(self, data_type='Minute'):
        """Update quality reports for all units"""
        data_path = 'Combined_Data'
        if not os.path.exists(data_path):
            print(f"No Combined_Data directory found")
            return
            
        for unit_no in self.units.keys():
            unit_path = os.path.join(data_path, f'UNIT {unit_no}')
            if not os.path.exists(unit_path):
                print(f"No data directory found for Unit {unit_no}")
                continue

            report_file = f'quality_reports/unit_{unit_no}_{data_type.lower()}_quality.xlsx'
            os.makedirs('quality_reports', exist_ok=True)
            
            # Load existing report or create new one
            if os.path.exists(report_file):
                report_df = pd.read_excel(report_file, index_col=0)
            else:
                report_df = pd.DataFrame(columns=[ch for ch in channels.keys() if self.units[unit_no]['channels'].get(ch, False)])
            
            # Get unique months from filenames
            months = set()
            for file in os.listdir(unit_path):
                if data_type in file and file.endswith('.csv'):
                    try:
                        # Extract YYYY-MM from filename
                        date_str = file.split('_')[-1].replace('.csv', '')
                        if len(date_str) >= 7:  # Ensure we have at least YYYY-MM
                            months.add(date_str[:7])  # Get YYYY-MM part
                    except:
                        continue
            
            # Process each month's data
            new_data_added = False
            for month in months:
                if month not in report_df.index:
                    results = self.process_data(data_path, unit_no, month, data_type)
                    if results:
                        report_df.loc[month] = {ch: self._format_quality_result(results.get(ch)) for ch in report_df.columns}
                        new_data_added = True
                else:
                    print(f"Month {month} already exists in report for Unit {unit_no}")
            
            if not new_data_added:
                print(f"No new data to add for Unit {unit_no}")
                continue
            
            # Save updated report with conditional formatting
            report_df.sort_index().to_excel(report_file)
            
            # Apply conditional formatting
            wb = load_workbook(report_file)
            ws = wb.active
            self._apply_conditional_formatting(ws)
            wb.save(report_file)
            
            print(f"Updated quality report for Unit {unit_no} ({data_type} data)")

def main():
    checker = QualityChecker()
    checker.update_quality_report('Minute')
    checker.update_quality_report('Hour')

if __name__ == "__main__":
    main()
