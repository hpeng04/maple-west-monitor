import os
import json
import calendar
import pandas as pd
import numpy as np
from datetime import datetime
from channels import channels
from openpyxl import load_workbook
from openpyxl.styles import PatternFill


class QualityChecker:
    def __init__(self, config_path='config/'):
        self.config_path = config_path
        self.units = self._load_units()
        self.red_fill = PatternFill(start_color='FFFF0000', end_color='FFFF0000', fill_type='solid')
        self.yellow_fill = PatternFill(start_color='FFFFFF00', end_color='FFFFFF00', fill_type='solid')
        self.output_directory = 'quality_reports'  # Define output directory for reports

    def _load_units(self):
        """Load all unit configurations from the config directory"""
        units = {}
        for file in os.listdir(self.config_path):
            if file.endswith('.json'):
                path = os.path.join(self.config_path, file)
                with open(path, 'r') as f:
                    config = json.load(f)
                    units[config['unit_no']] = config
        return units

    def _parse_value(self, value):
        """Try to convert value to float; return None if not possible."""
        if pd.isna(value) or value == "":
            return None
        if isinstance(value, (int, float)):
            return value
        try:
            return float(value)
        except Exception:
            return None

    def _check_data_quality(self, data, unit_config, channel_name):
        """Check data quality for a specific channel"""
        if not unit_config['channels'].get(channel_name, False):
            return None  # Channel is not monitored for this unit

        channel_info = channels[channel_name]
        matching_cols = data.columns[data.columns.str.contains(channel_info.regex, regex=True)]
        if matching_cols.empty:
            return None  # Channel not found in data

        # Take first matching column
        values = data[matching_cols[0]]

        good, missing, bad = 0, 0, 0
        for value in values:
            num_value = self._parse_value(value)
            if num_value is None:
                missing += 1
            else:
                if channel_info.min_value <= num_value <= channel_info.max_value:
                    good += 1
                else:
                    bad += 1

        return [good, missing, bad]

    def _format_quality_result(self, counts):
        """Format counts into a text summary"""
        if not counts or len(counts) != 3:
            return "Good: 0, Missing: 0, Bad: 0"
        return f"Good: {counts[0]}, Missing: {counts[1]}, Bad: {counts[2]}"

    def _update_excel_sheet(self, worksheet, total_points, fill_mapping):
        """
        Apply conditional formatting to cells in a worksheet.
        fill_mapping should be a function which takes the percentage and returns a fill style.
        """
        for row in worksheet.iter_rows(min_row=2):  # skip header row
            date_cell = row[0]
            for cell in row[1:]:
                if cell.value is not None:
                    try:
                        # Convert cell value to percentage using total_points
                        value = float(cell.value)
                        percentage = (value / total_points) * 100
                        cell.fill = fill_mapping(percentage)
                    except (ValueError, TypeError):
                        continue

    def _get_total_points(self, data_type, date_str):
        """Return total points based on data type and date"""
        if data_type == 'Minute':
            return 1440  # points per day
        else:
            try:
                year, month = map(int, date_str.split('-'))
                days = calendar.monthrange(year, month)[1]
            except Exception:
                days = 30  # fallback
            return days * 24  # hourly points per month

    def process_data(self, data_path, unit_no, month, data_type):
        """Process all CSV files for a unit and month"""
        unit_config = self.units[unit_no]
        # Prepare results for monitored channels
        results = {ch: [0, 0, 0]
                   for ch in channels.keys() if unit_config['channels'].get(ch, False)}
        unit_path = os.path.join(data_path, f'UNIT {unit_no}')
        if not os.path.exists(unit_path):
            return None

        for file in os.listdir(unit_path):
            if file.endswith('.csv') and data_type in file and month in file:
                try:
                    data = pd.read_csv(os.path.join(unit_path, file))
                    for channel_name in results.keys():
                        quality = self._check_data_quality(data, unit_config, channel_name)
                        if quality:
                            results[channel_name] = [x + y for x, y in zip(results[channel_name], quality)]
                except Exception as e:
                    print(f"Error processing file {file}: {str(e)}")
        return results

    def _update_report_for_date(self, unit_no, date_str, data_type, unit_path, monitored_channels):
        """
        Process and return results (bad and missing counts) for a single date (or month)
        based on the data type.
        """
        unit_config = self.units[unit_no]
        if data_type == 'Minute':
            file_name = f'Unit_{unit_no}_{date_str}.csv'
            file_path = os.path.join(unit_path, file_name)
            if not os.path.exists(file_path):
                return None
            try:
                data = pd.read_csv(file_path)
            except Exception as e:
                print(f"Error processing file {file_path}: {str(e)}")
                return None

            results = {}
            for channel_name in monitored_channels:
                quality = self._check_data_quality(data, unit_config, channel_name)
                if quality:
                    results[channel_name] = quality
            if results:
                bad_counts = {ch: results.get(ch, [0, 0, 0])[2] for ch in monitored_channels}
                missing_counts = {ch: results.get(ch, [0, 0, 0])[1] for ch in monitored_channels}
                return bad_counts, missing_counts
        else:
            results = self.process_data(os.path.join(f'{data_type}_Data'), unit_no, date_str, data_type)
            if results:
                bad_counts = {ch: results.get(ch, [0, 0, 0])[2] for ch in monitored_channels}
                missing_counts = {ch: results.get(ch, [0, 0, 0])[1] for ch in monitored_channels}
                return bad_counts, missing_counts
        return None

    def update_quality_report(self, data_type='Minute'):
        """Update quality reports for all units"""
        data_path = f'{data_type}_Data'
        if not os.path.exists(data_path):
            print(f"No {data_path} directory found")
            return

        os.makedirs('quality_reports', exist_ok=True)

        for unit_no, unit_config in self.units.items():
            monitored_channels = [ch for ch in channels.keys() if unit_config['channels'].get(ch, False)]
            unit_directory = os.path.join(data_path, f'UNIT {unit_no}')
            if not os.path.exists(unit_directory):
                print(f"No data directory found for Unit {unit_no}")
                continue

            report_file = f'quality_reports/unit_{unit_no}_{data_type.lower()}_quality.xlsx'
            # Load existing report if it exists
            bad_df, missing_df = self._load_existing_report(report_file, monitored_channels)
            dates = self._get_dates_from_files(unit_directory, data_type)
            new_data_added = False

            for date_str in dates:
                if date_str in bad_df.index and date_str in missing_df.index:
                    print(f"{date_str} already exists in report for Unit {unit_no}")
                    continue

                result = self._update_report_for_date(unit_no, date_str, data_type, unit_directory, monitored_channels)
                if result:
                    bad_counts, missing_counts = result
                    bad_df.loc[date_str] = bad_counts
                    missing_df.loc[date_str] = missing_counts
                    new_data_added = True

            if not new_data_added:
                print(f"No new data to add for Unit {unit_no}")
                continue

            # Save updated report
            with pd.ExcelWriter(report_file, engine='openpyxl') as writer:
                bad_df.sort_index().to_excel(writer, sheet_name='Bad Values')
                missing_df.sort_index().to_excel(writer, sheet_name='Missing Values')

            self._apply_excel_formatting(report_file, data_type)
            print(f"Updated quality report for Unit {unit_no} ({data_type} data)")

    def _load_existing_report(self, report_file, monitored_channels):
        """Load an existing report's sheets or create new DataFrames if not present."""
        bad_df = pd.DataFrame(columns=monitored_channels)
        missing_df = pd.DataFrame(columns=monitored_channels)
        if os.path.exists(report_file):
            try:
                with pd.ExcelFile(report_file) as xls:
                    if 'Bad Values' in xls.sheet_names:
                        bad_df = pd.read_excel(xls, 'Bad Values', index_col=0)
                    if 'Missing Values' in xls.sheet_names:
                        missing_df = pd.read_excel(xls, 'Missing Values', index_col=0)
            except Exception as e:
                print(f"Error reading existing report: {str(e)}")
        return bad_df, missing_df

    def _get_dates_from_files(self, unit_directory, data_type):
        """Extract relevant unique dates from file names in the unit directory."""
        dates = set()
        for file in os.listdir(unit_directory):
            if file.endswith('.csv'):
                try:
                    date_str = file.split('_')[-1].replace('.csv', '')
                    if data_type == 'Minute' and len(date_str) == 10:
                        dates.add(date_str)
                    elif data_type != 'Minute' and len(date_str) >= 7:
                        dates.add(date_str[:7])
                except Exception:
                    continue
        return dates

    def _apply_excel_formatting(self, report_file, data_type):
        """Apply conditional formatting to both Bad and Missing sheets."""
        wb = load_workbook(report_file)

        def fill_mapping(percentage):
            if percentage > 5:
                return self.red_fill
            elif percentage > 1:
                return self.yellow_fill
            else:
                return None

        for sheet_name in ['Bad Values', 'Missing Values']:
            if sheet_name in wb.sheetnames:
                ws = wb[sheet_name]
                for row in ws.iter_rows(min_row=2):  # skip header
                    date_cell = row[0]
                    total_points = self._get_total_points(data_type, str(date_cell.value)) if date_cell.value else 1
                    for cell in row[1:]:
                        if cell.value is not None:
                            try:
                                value = float(cell.value)
                                percentage = (value / total_points) * 100
                                cell.fill = fill_mapping(percentage) or cell.fill
                            except Exception:
                                continue
        wb.save(report_file)

    def get_current_quality_data(self):
        """
        Get the current quality data for all units by reading from their latest reports.
        
        Returns:
        --------
        dict
            Dictionary with units as keys and quality metrics as values
        """
        quality_data = {}
        
        # Ensure output directory exists
        if not os.path.exists(self.output_directory):
            return quality_data
        
        # Gather data from minute reports (more detailed) if available
        data_type_priority = ['minute', 'hour']
        
        for unit_no in self.units.keys():
            for data_type in data_type_priority:
                report_file = os.path.join(self.output_directory, f'unit_{unit_no}_{data_type}_quality.xlsx')
                
                if os.path.exists(report_file):
                    try:
                        # Read the latest records from both bad and missing sheets
                        with pd.ExcelFile(report_file) as xls:
                            if 'Bad Values' in xls.sheet_names and 'Missing Values' in xls.sheet_names:
                                bad_df = pd.read_excel(xls, 'Bad Values', index_col=0)
                                missing_df = pd.read_excel(xls, 'Missing Values', index_col=0)
                                
                                if not bad_df.empty and not missing_df.empty:
                                    # Sort by index (date) and get the latest date
                                    bad_df = bad_df.sort_index()
                                    missing_df = missing_df.sort_index()
                                    
                                    latest_date_bad = bad_df.index[-1]
                                    latest_date_missing = missing_df.index[-1]
                                    
                                    # Use most recent date's data
                                    latest_bad = bad_df.loc[latest_date_bad].to_dict()
                                    latest_missing = missing_df.loc[latest_date_missing].to_dict()
                                    
                                    quality_data[unit_no] = {
                                        'bad_counters': latest_bad,
                                        'missing_counters': latest_missing,
                                        'date': str(latest_date_bad)
                                    }
                                    
                                    # Break the loop if we found data for this unit
                                    break
                    except Exception as e:
                        print(f"Error reading report for Unit {unit_no} ({data_type}): {str(e)}")
        
        return quality_data

    def create_continuous_quality_report(self, output_path=None):
        """
        Creates or updates a continuous XLSX quality report that shows each unit as a column
        and dates as rows, with bad and missing counters summed for each date/unit.
        
        Parameters:
        -----------
        output_path : str, optional
            Path to save the continuous report. If None, uses default path.
        
        Returns:
        --------
        str
            Path to the saved report file
        """
        from datetime import datetime
        
        # Set default output path if none provided
        if output_path is None:
            output_path = os.path.join(self.output_directory, "continuous_quality_report.xlsx")
        
        # Get the current date
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        # Load existing data if file exists
        existing_data = None
        if os.path.exists(output_path):
            try:
                existing_data = pd.read_excel(output_path, index_col=0)
                print(f"Loaded existing continuous report from {output_path}")
            except Exception as e:
                print(f"Error loading existing report: {str(e)}")
        
        # Get current quality data
        current_data = self.get_current_quality_data()
        
        # Format the data: Transform to have units as columns and dates as rows
        formatted_data = {}
        
        for unit, metrics in current_data.items():
            if unit not in formatted_data:
                formatted_data[unit] = 0
            
            # Sum up all bad and missing counters for this unit
            bad_count = sum(metrics.get('bad_counters', {}).values())
            missing_count = sum(metrics.get('missing_counters', {}).values())
            formatted_data[unit] = bad_count + missing_count
        
        # Create a new row for today's data
        new_row = pd.DataFrame([formatted_data], index=[current_date])
        
        # Combine with existing data or create new DataFrame
        if existing_data is not None:
            # If current date already exists in the report, update it
            if current_date in existing_data.index:
                existing_data.loc[current_date] = new_row.loc[current_date]
                combined_data = existing_data
            else:
                # Append the new row
                combined_data = pd.concat([existing_data, new_row])
            
            # Ensure all columns exist in the combined data
            for unit in formatted_data.keys():
                if unit not in combined_data.columns:
                    combined_data[unit] = None
                    combined_data.loc[current_date, unit] = formatted_data[unit]
        else:
            combined_data = new_row
        
        # Sort by date (index)
        combined_data = combined_data.sort_index()
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save to Excel
        combined_data.to_excel(output_path)
        print(f"Continuous quality report updated at {output_path}")
        
        return output_path


def main():
    checker = QualityChecker()
    checker.update_quality_report('Minute')
    checker.update_quality_report('Hour')


if __name__ == "__main__":
    main()
