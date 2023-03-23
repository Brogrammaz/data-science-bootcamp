from pyforest import *
import requests # To fetch data through APIs
import json

class Extract:
    def __init__(self) -> None:
        # load json file here to use it across other methods.
        self.data_sources = json.load(open('data_config.json'))
        self.api = self.data_sources['data_sources']['api_data']
        self.csv_path = self.data_sources['data_sources']['csv_data']


    def getAPIData(self, api_name):
        # pass thr required api_name for the data you need.
        api_url = self.api[api_name]
        response = requests.get(api_url)

        # response.json() converts json data into a python dictionary.
        return response.json()
    
    def getCSVData(self, csv_name):
        # pass the csv data source name.
        df = pd.read_csv(self.csv_path[csv_name])

        return df