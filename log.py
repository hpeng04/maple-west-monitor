from datetime import datetime, timedelta
import os

class Log:
    path = r'Logs/'

    @staticmethod
    def write(message, is_loaded=False):
        if not os.path.exists(Log.path):
            os.makedirs(Log.path)
        if is_loaded == False:
            yesterday = datetime.now() - timedelta(days=1)
            formatted_date = yesterday.strftime('%Y-%m-%d')
            path = Log.path + formatted_date + '.txt'
        else:
            path = Log.path + 'log.txt'
        with open(path, 'a') as file:
            file.write(message + '\n')
    
    @staticmethod
    def get_path():
        yesterday = datetime.now() - timedelta(days=1)
        formatted_date = yesterday.strftime('%Y-%m-%d')
        path = Log.path + formatted_date + '.txt'
        return path