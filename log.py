from datetime import datetime, timedelta
import os

class Log:
    path = r'Logs/'
    missing_path = r'missing_data.txt'
    @staticmethod
    def write(message, date=None):
        if not os.path.exists(Log.path):
            os.makedirs(Log.path)
        if not date:
            yesterday = datetime.now() - timedelta(days=1)
            formatted_date = yesterday.strftime('%Y-%m-%d')
            path = Log.path + formatted_date + '.txt'
        else:
            path = Log.path + date + '.txt'
        with open(path, 'a') as file:
            file.write(message + '\n')
    
    @staticmethod
    def get_path():
        yesterday = datetime.now() - timedelta(days=1)
        formatted_date = yesterday.strftime('%Y-%m-%d')
        path = Log.path + formatted_date + '.txt'
        return path
    
    @staticmethod
    def record_missing(unit_no, date):
        with open(Log.missing_path, 'a') as file:
            file.write(f'{unit_no},{date}\n')
        return