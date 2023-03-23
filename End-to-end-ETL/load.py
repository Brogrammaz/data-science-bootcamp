from pymongo import MongoClient
from pyforest import *

class MongoStorage:

    # define the MongoDB configuration variables.
    def __init__(self,user,password,host,db_name,port='27017', authSource = 'admin') -> None:
        self.user = user
        self.password = password
        self.host = host
        self.db_name = db_name
        self.port = port
        self.authSource = authSource
        self.uri = f'mongodb://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}?authSource={self.authSource}'

        try:
            self.client = MongoClient(self.uri)
            self.db = self.client[self.db_name]
            print("Connected Successfully:)")
        except Exception as e:
            print("Connection Failed!")
            print(str(e))

    # method to insert(Load) data into DB.
    def insert_data(self,data,collection):
        if isinstance(data,pd.DataFrame):
            try:
                self.db[collection].insert_many(data.to_dict('records'))
                print("Data inserted successfully:)")
            except Exception as e:
                print("Insertion Error")
                print(str(e))
        else:
            try:
                self.db[collection].insert_many(data)
                print("Data insertion successfull:)")
            except Exception as e:
                print("Insertion Unsuccessfull")
                print(str(e))
        
    # method to read data stored in the DB
    def read_data(self, collection):
        try:
            data = pd.DataFrame(list(self.db[collection].find()))
            print("Data Fetched Successfully :)")
            return data
        except Exception as e:
            print("Data Retreival Error")
            print(str(e))