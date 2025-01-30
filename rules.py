def check_missing_rows(df):
    initial_time = df.iloc[0, 0]
    index = 0
    current_time = df.iloc[index, 0]
    while current_time != df.iloc[-1, 0]:
        
        index += 1
        current_time = df.iloc[index, 0]

class DataQualityRule:
    def __init__(self, name: str, check_function: function):
        self.name = name
        self.check_function = check_function
  
    def apply(self, data):
        return self.check_function(data)