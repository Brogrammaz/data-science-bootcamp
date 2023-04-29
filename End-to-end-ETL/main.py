from transformations import Transformation

import json

class Engine:

    def __init__(self, data_source, data_set) -> None:
        transform_obj = Transformation(data_source, data_set)
        
if __name__ == "__main__":

    etl_data = json.load(open('data_config.json'))
    for data_source, data_set in etl_data['data_source'].items():
        print(data_source)
        for data in data_set:
            print(data)
            main_obj = Engine(data_source,data)