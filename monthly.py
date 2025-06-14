import os
import pandas as pd
import re
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload
from io import BytesIO
from google.oauth2 import service_account
from color import color
from datetime import datetime
from dateutil.relativedelta import relativedelta
from rules import check_missing_rows
from alert import alert_failed_downloads
import qualitycheck

SERVICE_ACCOUNT_JSON = 'service_account.json'
SCOPES = ['https://www.googleapis.com/auth/drive']

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_JSON, scopes=SCOPES)
drive_service = build('drive', 'v3', credentials=credentials)

MINUTE_PATH = './Minute_Data'
HOUR_PATH = './Hour_Data'
OUTPUT_PATH = './Combined_Data'
FAILED_DOWNLOAD_PATH = './failed_downloads.txt'
QUALITY_REPORTS_PATH = './quality_reports'
QUALITY_REPORTS_FOLDER = '1VTlQzomRsOXTUEOKfOaH94J15WApczri'

locations = {
    "UNIT 77": "1lDSBeFE5p9snL9rHBVBzsnQcX_0YV5Ju",
    "UNIT 78": "103QnxldwU07-yrlA4XFQGMpmya0EIcW2",
    "UNIT 79": "1RnVLcH2rV4jhsi0GY6N3x_-PmpRzmqRY",
    "UNIT 80": "1fSk1ikNSaR8P-F19RglXOgYg5cF1DHyy",
    "UNIT 81": "1jlytoisi-JISBRencOaqHxaKYCny1rFA",
    "UNIT 82": "1Siq34KBC1QNIvcKokOOhpMo3-RCVQYIH",
    "UNIT 83": "1KfAAEJgonrEoL5ffP8VHCflg44no_ddm",
    "UNIT 84": "1-rpJ59XpGoNjscg-Rra9EvNbMR8Oy8MJ",
    "UNIT 85": "1AMKvlt7pm7_8Y_zsbaWTTZu7QRopzUdU",
    "UNIT 86": "1wwnoiYmlNAZKRCJ4o3yIbrrTT_KXLbe1",
    "UNIT 2804": "1MsjQ5IgvnU5TGUBXPKWWFMvJme682SNH",
    "UNIT 2806": "1SxUChu2uQ6_A9x8ZPFMvi0SXaDq9-Y7y",
    "UNIT 2808": "1kofZCSXwdgdjx01qu_ruOV8zKohyBNTE",
    "UNIT 2810": "1ANJLFZ_Ve0BHlH94KKHdz9F_AvPKwHBs",
    "UNIT 2812": "17Zuo7fpmpU5PeBKmqD0ICqHoAyMCBQ8r",
    "UNIT 2814": "1qsbxo7iD4L7NRHFsQVNDxWMbuCosm3Cq",
    "UNIT 2816": "1NpPsGArjNYETE7jG1wdROZESslTaUz5w",
    "UNIT 2818": "1nXd5l88n6KmSdAonIq7pDiFBsaZ5NeQd"
}

def natural_sort_key(s):
    return [int(text) if text.isdigit() else text.lower() for text in re.split('(\\d+)', s)]

""" Data combination."""
def combine_csv_files(input_folder):
    # List all CSV files in the input folder
    csv_files = []
    for root, _, files in os.walk(input_folder):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))
            else:
                print('File does not end in .csv:' + file)
    csv_files.sort(key=natural_sort_key)

    ### Filter out November files: Daylight Savings on Nov 2 adds one extra row to the data which is not accomodated for.
    # csv_files = [file for file in csv_files if not file.endswith('11.csv')]
    
    if not csv_files:
        print(f"{color.RED}No CSV files found in {input_folder}.{color.END}")
        return pd.DataFrame()
    
    # Initialize an empty list to store DataFrames
    dfs = []

    # Read each CSV file and append to the dfs list
    for file in csv_files:
        cols = pd.read_csv(file, nrows=1).columns.size
        print("Reading file: ", file)
        df = pd.read_csv(file, on_bad_lines=lambda x: x[:cols], engine='python')
        if df.iloc[0, 0] > df.iloc[1, 0]:
            df = df.iloc[::-1]
        dfs.append(df)
    # Concatenate all DataFrames in the dfs list
    combined_data = pd.concat(dfs, ignore_index=True)
    
    return combined_data

def save_to_csv(df, output_folder, unit_no, datatype):
    # Create the output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)
    
    # Save the DataFrame to a new CSV file in the output folder
    last_month = (datetime.today() - relativedelta(months=1)).strftime('%Y-%m')
    output_file_path = os.path.join(output_folder, f'Unit_{unit_no}_{datatype}_{last_month}.csv')
    df.to_csv(output_file_path, index=False)
    
    print(f"Combined CSV file saved successfully at {output_file_path}")

def combine_all(input_path, output_path):
    for _, dirs, _ in os.walk(input_path):
        for dir in dirs:
            in_path = os.path.join(input_path, dir)
            out_path = os.path.join(output_path, dir)
            unit_no = dir.split(' ')[-1]
            df = combine_csv_files(in_path)
            datatype = ""
            if "Minute" in input_path:
                datatype = "Minute"
            else:
                datatype = "Hour"
            if not df.empty:
                save_to_csv(df, out_path, unit_no, datatype)
                print(f"Unit {unit_no} combined successfully.")
            else:
                print(f"{color.RED}Unit {unit_no} could not be combined.{color.END}")
        break

def upload_combined(combined_path):
    for _, dirs, _ in os.walk(combined_path):
        for dir in dirs:
            unit_no = dir.split(' ')[-1]
            folder_path = os.path.join(combined_path, dir)
            for root, _, files in os.walk(folder_path):
                for file in files:
                    if file.endswith('.csv'):
                        file_path = os.path.join(root, file)
                        # Check if file already exists
                        query = f"name='{file}' and '{locations[dir]}' in parents and trashed=false"
                        results = drive_service.files().list(
                            q=query,
                            fields="files(id, name)",
                            supportsAllDrives=True,
                            includeItemsFromAllDrives=True
                        ).execute()
                        
                        if results.get('files', []):
                            print(f"{file} already exists in Google Drive folder {locations[dir]} for Unit {unit_no}")
                            continue
                            
                        # Upload file
                        file_metadata = {
                            'name': file,
                            'parents': [locations[dir]]
                        }
                        media = MediaFileUpload(file_path, resumable=True)
                        uploaded_file = drive_service.files().create(
                            body=file_metadata,
                            media_body=media,
                            fields='id',
                            supportsAllDrives=True
                        ).execute()
                        print(f"Uploaded {file} to Google Drive folder {locations[dir]} for Unit {unit_no}")

def delete_all(paths:list):
    for folder in paths:
        for root, dirs, files in os.walk(folder, topdown=False):
            for file in files:
                print(f"Deleted {os.path.join(root, file)}")
                os.remove(os.path.join(root, file))
    print(f"Deleted all files in {paths}")

def fix_order(df:pd.DataFrame):
    '''
    Sort the data is ascending order of time if not already sorted

    param: df: pd.DataFrame: data to be sorted
    return: pd.DataFrame: sorted data
    '''
    if df.iloc[0, 0] > df.iloc[1, 0]:
        return df.iloc[::-1]
    else:
        return df

def download_failed(failed_units_path: str):
    with open(failed_units_path, 'r+') as f:
        lines = f.readlines()  # Read all lines
        new_lines = []  # Store lines that should remain
        for line in lines:
            if line.strip() == "":
                continue  # Skip empty lines
            try:
                unit_no, datatype, url = line.strip().split(', ')
                print(f"Attempting to download Unit {unit_no}, {datatype} from {url}")
                data = fix_order(pd.read_csv(url, header=0, on_bad_lines='skip'))
                data, _, _, _ = check_missing_rows(data, unit_no)
                date = url.split('/')[-1].strip()
                data.to_csv(f'./{datatype}_Data/UNIT {str(unit_no)}/Unit_{str(unit_no)}_{str(date)}.csv', index=False)
                print(f"{color.GREEN}Download successful{color.END}")
            except:
                print(f"{color.RED}Unit {unit_no} could not be downloaded from {url}{color.END}")
                new_lines.append(line)  # Keep this line if download fails
        f.seek(0)  # Move cursor to start of file
        f.truncate(0)  # Clear the file
        f.writelines(new_lines)  # Write back only failed lines
    return

def download_quality_reports():
    """Download quality reports from Google Drive"""
    query = f"'{QUALITY_REPORTS_FOLDER}' in parents and trashed=false"
    results = drive_service.files().list(
        q=query,
        fields="files(id, name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    
    for item in results.get('files', []):
        file_id = item['id']
        file_name = item['name']
        
        # Download the file
        request = drive_service.files().get_media(fileId=file_id)
        file_path = os.path.join(QUALITY_REPORTS_PATH, file_name)
        
        with open(file_path, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                print(f"Downloaded {file_name} {int(status.progress() * 100)}%.")

def upload_quality_reports():
    """Upload quality reports to Google Drive"""
    if not os.path.exists(QUALITY_REPORTS_PATH):
        os.makedirs(QUALITY_REPORTS_PATH)
        return

    for file in os.listdir(QUALITY_REPORTS_PATH):
        if file.endswith('.xlsx'):
            file_path = os.path.join(QUALITY_REPORTS_PATH, file)
            
            # Check if file already exists in Drive
            query = f"name='{file}' and '{QUALITY_REPORTS_FOLDER}' in parents and trashed=false"
            results = drive_service.files().list(
                q=query,
                fields="files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()
            
            # Delete existing file if found
            for item in results.get('files', []):
                
                drive_service.files().delete(
                    fileId=item['id'],
                    supportsAllDrives=True
                ).execute()
                print(f"Deleted existing file {file} from Google Drive")

            # Upload file
            file_metadata = {
                'name': file,
                'parents': [QUALITY_REPORTS_FOLDER]
            }
            media = MediaFileUpload(file_path, resumable=True)
            uploaded_file = drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id',
                supportsAllDrives=True
            ).execute()
            print(f"Uploaded {file} to Google Drive quality reports folder")

if __name__ == '__main__':
    download_failed(FAILED_DOWNLOAD_PATH)
    combine_all(MINUTE_PATH, OUTPUT_PATH)
    download_quality_reports()
    qualitycheck.main()
    upload_combined(OUTPUT_PATH)
    upload_quality_reports()
    delete_all([MINUTE_PATH, HOUR_PATH, OUTPUT_PATH])
    alert_failed_downloads(FAILED_DOWNLOAD_PATH)