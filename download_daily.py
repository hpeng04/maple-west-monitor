from datetime import datetime, date, timedelta
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import urllib.request, csv
from oauth2client.service_account import ServiceAccountCredentials
# import pymysql, pyodbc, tempfile
# from sqlalchemy import create_engine
# import pandas as pd

from color import color
import os
import shutil

SERVICE_ACCOUNT_JSON = 'service_account.json'
SCOPES = ['https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_JSON, SCOPES)
gauth = GoogleAuth()
gauth.credentials = credentials
drive = GoogleDrive(gauth)

# Define serial of GEM and IP address for each unit
# locations = [["UNIT 77", "01021504", "68.182.34.229:9006", "2022-10-03", "1Z1ETfHlgitFOBx8SWqYiotkezFwL2ahb"],
#     ["UNIT 78", "01021510", "68.182.34.229:9001", "2022-10-03", "1oOKr0Kt2Jg2j_tqzv6sdRF4bEuNofxy2"],
#     ["UNIT 79", "01021526", "68.182.34.229:9002", "2022-20-03", "1NUruYAn2kafZTrUOFeMndmKHCyXnBVFM"],
#     ["UNIT 80", "01021531", "68.182.34.229:9002", "2022-10-03", "1Idx4pE-vBeRzFQXoBpiwRgfHbh-CZ5Go"],
#     ["UNIT 81", "01021511", "68.182.34.229:9003", "2022-10-03", "1Y17UL5XGQPlFf361V-9FLhP-JhAuFzaI"],
#     ["UNIT 82", "01021525", "68.182.34.229:9003", "2022-09-08", "1ud6EOvBFkXGPR8McKDEJR6Dx3y_NssiF"],
#     ["UNIT 83", "01021505", "68.182.34.229:9004", "2022-08-26", "1O08BMUVd4CnNsYYoH5l1mRYGW97AXsA5"],
#     ["UNIT 84", "01021529", "68.182.34.229:9004", "2022-08-26", "1UHTnt1gOF28LoF7QnS-JcQ69q20pdeDc"],
#     ["UNIT 85", "01021521", "68.182.34.229:9006", "2022-08-26", "1j2tTzGza7hDmwGukHo1HQFTYlMGg1qmo"],
#     ["UNIT 86", "01021506", "68.182.34.229:9005", "", "1pmiaLqn_M3y3Ebn3tT9LnHBvRbgHOsA6"],
#     # ["UNIT 87", "01021514", "68.182.35.138:9006", "", "1JPCc3CIt1R5pW61DHtC24wVslnWC8QhN"]]
#     ["UNIT 2804", "01021542", "68.182.34.129:9007", "", "1X8M-Ec0y-vl3CrJ9zZxuMUMYAVlMF7fJ"],
#     ["UNIT 2806", "01021522", "68.182.34.129:9007", "", "1Fh7r38--RT9_J1-e4XJnzLdQkXjD0zO5"],
#     ["UNIT 2808", "01021520", "68.182.34.129:9008", "", "127cFRH_0pa1eZN67dYp4H63dM4hwX_43"],
#     ["UNIT 2810", "01120982", "68.182.34.129:9008", "", "1euo9FBA2qem2oaa_ze0bxudcA9ZSOSVc"],
#     ["UNIT 2814", "01120966", "68.182.34.129:9009", "", "1KH_x8Sb7ix-KdBUJYvsGZDOB5Ec3oaei"],
#     ["UNIT 2812", "01121001", "68.182.34.129:9011", "", "125PAJ1RCLN4IDGVUeyh-lmvXQEhY6vMH"],
#     ["UNIT 2816", "01120969", "68.182.34.129:9010", "", "1uCERJeoSE2Oexa59g3Nv7SZNrzQZfHp0"],
#     ["UNIT 2818", "01120965", "68.182.34.129:9010", "", "17dVMIaaF0kzZofbUA4kJAIeYbVhrlNHt"]]
#     # ["UNIT 2820", "01120971", "68.182.35.138:9004", "", "1T126lxUeQR55BdxNX1ToD0FEsZu2Mcpf"]]

locations = [["UNIT 77", "01021504", "68.182.34.229:9006", "2022-10-03", "12jVbNJcXzMJIYPogBHDKcgppk42DCYGE"],
    ["UNIT 78", "01021510", "68.182.34.229:9001", "2022-10-03", "12jVbNJcXzMJIYPogBHDKcgppk42DCYGE"],
    ["UNIT 79", "01021526", "68.182.34.229:9002", "2022-20-03", "12jVbNJcXzMJIYPogBHDKcgppk42DCYGE"],
    ["UNIT 80", "01021531", "68.182.34.229:9002", "2022-10-03", "12jVbNJcXzMJIYPogBHDKcgppk42DCYGE"],
    ["UNIT 81", "01021511", "68.182.34.229:9003", "2022-10-03", "12jVbNJcXzMJIYPogBHDKcgppk42DCYGE"],
    ["UNIT 82", "01021525", "68.182.34.229:9003", "2022-09-08", "12jVbNJcXzMJIYPogBHDKcgppk42DCYGE"],
    ["UNIT 83", "01021505", "68.182.34.229:9004", "2022-08-26", "12jVbNJcXzMJIYPogBHDKcgppk42DCYGE"],
    ["UNIT 84", "01021529", "68.182.34.229:9004", "2022-08-26", "12jVbNJcXzMJIYPogBHDKcgppk42DCYGE"],
    ["UNIT 85", "01021521", "68.182.34.229:9006", "2022-08-26", "12jVbNJcXzMJIYPogBHDKcgppk42DCYGE"],
    ["UNIT 86", "01021506", "68.182.34.229:9005", "", "12jVbNJcXzMJIYPogBHDKcgppk42DCYGE"],
    # ["UNIT 87", "01021514", "68.182.35.138:9006", "", "12jVbNJcXzMJIYPogBHDKcgppk42DCYGE"]]
    ["UNIT 2804", "01021542", "68.182.34.129:9007", "", "12jVbNJcXzMJIYPogBHDKcgppk42DCYGE"],
    ["UNIT 2806", "01021522", "68.182.34.129:9007", "", "12jVbNJcXzMJIYPogBHDKcgppk42DCYGE"],
    ["UNIT 2808", "01021520", "68.182.34.129:9008", "", "12jVbNJcXzMJIYPogBHDKcgppk42DCYGE"],
    ["UNIT 2810", "01120982", "68.182.34.129:9008", "", "12jVbNJcXzMJIYPogBHDKcgppk42DCYGE"],
    ["UNIT 2814", "01120966", "68.182.34.129:9009", "", "12jVbNJcXzMJIYPogBHDKcgppk42DCYGE"],
    ["UNIT 2812", "01121001", "68.182.34.129:9011", "", "12jVbNJcXzMJIYPogBHDKcgppk42DCYGE"],
    ["UNIT 2816", "01120969", "68.182.34.129:9010", "", "12jVbNJcXzMJIYPogBHDKcgppk42DCYGE"],
    ["UNIT 2818", "01120965", "68.182.34.129:9010", "", "12jVbNJcXzMJIYPogBHDKcgppk42DCYGE"]]
    # ["UNIT 2820", "01120971", "68.182.35.138:9004", "", "12jVbNJcXzMJIYPogBHDKcgppk42DCYGE"]]

## Get yesterday's date
yesterday = date.today() - timedelta(days=1)

## Use this if a day is missed otherwise comment out
#yesterday = datetime(2023,4,17).date()

def google_upload(id, filename, title):
    file_list = drive.ListFile({'q': f"'{id}' in parents and title = '{title}' and trashed=false"}).GetList()
    if file_list:
        print(f"File {title} already exists in the drive folder with id {id}. Skipping upload.")
        return
    gfile = drive.CreateFile({'parents': [{'id': id}]})
    gfile['title'] = title
    gfile.SetContentFile(filename)
    gfile.Upload(param={'supportsTeamDrives': True})

### MYSQL CONNECTION THROUGH sqlalchemy
#try:
#    engine = create_engine('mysql+pymysql://idobe:IDOBE12345678@localhost:3308/mw_data',connect_args={
#        'client_flag': 2048,
#        'sql_mode': 'ANSI_QUOTES'  # This will handle problematic column names
#    })
#except:
#   print('sometin wrong')
#

#Loop through locations, download and save each CSV from yesterday

def download_all():
    if not os.path.exists("./Data"):
        os.makedirs("./Data")

    for i in range(len(locations)):
        current_filename = "./Data/" + locations[i][0] + "/" + locations[i][1] + "_" + yesterday.strftime("%Y-%m-%d") + ".csv"
        if not os.path.exists("./Data/" + locations[i][0]):
            os.makedirs("./Data/" + locations[i][0])
        
        print(f"Downloading {locations[i][0]}: {locations[i][2]}")
        try:
            urllib.request.urlretrieve("http://"+ locations[i][2] + "/index.php/pages/export/exportDaily/" + locations[i][1] + "/" + yesterday.strftime("%Y-%m-%d") + "/0", current_filename)
        except:
            print(f"{color.RED}Error downloading {locations[i][0]}: {locations[i][2]}{color.END}")
            continue

        google_upload(locations[i][4], current_filename, current_filename.split('/')[-1])
        print(f"Uploaded {locations[i][0]}: {locations[i][2]}")

        with open(current_filename, newline= '') as f:        
            reader = csv.reader(f)
            data = list(reader)
        del data[0]
        data.sort()
        times = [i[0] for i in data]
        diffs = []
        nData = []

        for j in range(1,len(data) - 1):
            diffs.append(datetime.strptime(times[j + 1], "%Y-%m-%d %H:%M:%S") - datetime.strptime(times[j], "%Y-%m-%d %H:%M:%S"))

        for j, s in enumerate(diffs):
            mins = s.seconds/60 - 1
            nData.append([datetime.strptime(times[j + 1],"%Y-%m-%d %H:%M:%S"), 0, 0, 0, 0])
            if mins > 0:
                for k in range(int(s.seconds/60) - 1):
                    if mins == 1:
                        nData.append([datetime.strptime(times[j + 1],"%Y-%m-%d %H:%M:%S") + timedelta(minutes=k + 1), 1, 0 ,0, 0])
                    elif mins == 2:
                        nData.append([datetime.strptime(times[j + 1],"%Y-%m-%d %H:%M:%S") + timedelta(minutes=k + 1), 0, 1 ,0, 0])
                    elif mins == 3:
                        nData.append([datetime.strptime(times[j + 1],"%Y-%m-%d %H:%M:%S") + timedelta(minutes=k + 1), 0, 0 ,1, 0])
                    elif mins > 3:
                        nData.append([datetime.strptime(times[j + 1],"%Y-%m-%d %H:%M:%S") + timedelta(minutes=k + 1), 0, 0 ,0, 1])
        
        nData.insert(0,["Date", "Single Point", "Two Points", "Three Points", "Multiple Points"])
        with open("./Data/" + locations[i][0] + "/" + locations[i][1] + "_" + yesterday.strftime("%Y-%m-%d") + "_missing.csv", 'w') as f:
            writer = csv.writer(f)
            writer.writerows(nData)

        google_upload(locations[i][4], "./Data/" + locations[i][0] + "/" + locations[i][1] + "_" + yesterday.strftime("%Y-%m-%d") + "_missing.csv", locations[i][1] + "_" + yesterday.strftime("%Y-%m-%d") + "_missing.csv")


def delete_data_folder():
    if os.path.exists("./Data"):
        shutil.rmtree("./Data")
        print("Deleted ./Data folder")
    else:
        print("./Data folder does not exist")


# for unit in locations:
#    data = pd.read_csv('/home/charlie/SCRIPTS/data/' + unit[1] + '_' + datetime.strftime(yesterday,'%Y-%m-%d') + '.csv', on_bad_lines='skip')
#    df = pd.DataFrame(data)
#    df = df.sort_values(by=['Date'])
#    df.to_sql(unit[0], con=engine, if_exists='append', index=False)

if __name__ == '__main__':
    download_all()



