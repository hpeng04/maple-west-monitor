from datetime import datetime, timedelta
import os

class Log:
    path = r'Logs/'
    missing_path = r'missing_data.txt'
    yesterday = (datetime.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')

    @staticmethod
    def write(message, date=yesterday):
        if not os.path.exists(Log.path):
            os.makedirs(Log.path)
        path = Log.path + date + '.txt'
        with open(path, 'a') as file:
            file.write(message + '\n')
    
    @staticmethod
    def get_path(date=yesterday):
        path = Log.path + date + '.txt'
        return path
    
    @staticmethod
    def record_missing(unit_no, date):
        with open(Log.missing_path, 'a') as file:
            file.write(f'{unit_no},{date}\n')
        return