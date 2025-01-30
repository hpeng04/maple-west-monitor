class Log:
    path = 'log.txt'

    @staticmethod
    def write(message):
        
        with open(Log.path, 'a') as file:
            file.write(message + '\n')