from datetime import datetime, timedelta
import os

class Log:
    path = r'Logs/'

    @staticmethod
    def write(message):
        if not os.path.exists(Log.path):
            os.makedirs(Log.path)
        yesterday = datetime.now() - timedelta(days=1)
        formatted_date = yesterday.strftime('%Y-%m-%d')
        path = Log.path + formatted_date + '.txt'
        with open(path, 'a') as file:
            file.write(message + '\n')