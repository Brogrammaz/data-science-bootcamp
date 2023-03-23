from extract import Extract
from load import MongoStorage
from pyforest import *
import urllib.parse

class Transformation:

    def __init__(self,datasource, dataset) -> None:
        # create an extraction object.
        extract_obj = Extract()

        if datasource == 'api':
            self.data = extract_obj.getAPIData(dataset)

            func_name = datasource+dataset

            # getattr function takes in function name of class and calls it.
            getattr(self,func_name)

        elif datasource == 'csv':
            self.data = extract_obj.getCSVData(dataset)
            func_name = datasource+dataset

            getattr(self,func_name)
        
        else:
            print("Datasource not valid!")

    # Pollution Data Transformation.
    def pollutionApi(self):
        air_data = self.data['result']

        # convert nested data into linear structure
        air_data_list = []
        air_dict = {}

        for data in air_data:
            for measurement in data['measurements']:
                air_dict['city'] = data['city']
                air_dict['country'] = data['country']
                air_dict['parameter'] = measurement['parameter']
                air_dict['value'] = measurement['value']
                air_dict['unit'] = measurement['unit']
                air_data_list.append(air_dict)

        # convert {air_data_list} to pandas dataframe
        df = pd.DataFrame(air_data_list, columns=air_dict.keys())

        # connect to mongo DB
        mongoDB_obj = MongoStorage(
            urllib.parse.quote_plus('root'),
            urllib.parse.quote_plus('password'),
            "host",
            'Pollution_Data'
            )
        
        # insert data into the DB
        mongoDB_obj.insert_data(df,"Air_Quality")

    # Economy Data Transformation
    def economyApi(self):
        gdp_data_dict = {}
        gdp = {}
        gdp_data_years = []

        for record in self.data['records']:
            # get annual GDP values from records.
            gdp['GDP_in_rs_cr'] = int(record['gross_domestic_product_in_rs_cr_at_2004_05_prices'])
            gdp_data_dict[record['financial_year']] = gdp
            gdp_data_years.append(gdp_data_dict)

        for r in range(len(gdp_data_years)):
            if r == 0:
                pass
            else:
                key = f'GDP_Growth_{gdp_data_years[r]}'
                # calculate annual GDP growth.
                gdp_data_dict[gdp_data_years[r]][key] = round(((gdp_data_dict[gdp_data_years[r]]['GDP_in_rs_cr'] - gdp_data_dict[gdp_data_years[r-1]]['GDP_in_rs_cr'])/gdp_data_dict[gdp_data_years[r-1]]['GDP_in_rs_cr'])*100,2)

        # connect to mongoDB
        mongoDb_obj = MongoStorage(
            urllib.parse.quote_plus('root'),
            urllib.parse.quote_plus('password'),
            'host',
            'GDP'
        )

        # Insert Data
        mongoDb_obj.insert_data(gdp_data_dict, 'GDP_Data')

    # Crypto Data Transformation.
    def csvDataApi(self):

        currency_code = ['BTC','ETH','XRP','LTC']
        self.crypto_df = pd.Dataframe()

        self.crypto_df['open'] = self.crypto_df[['open','asset']].apply(lambda x: (float(x[0]) * 134.56) if x[1] in currency_code else np.nan, axis=1)
        self.crypto_df['close'] = self.crypto_df[['close','asset']].apply(lambda x: (float(x[0]) * 134.56) if x[1] in currency_code else np.nan, axis=1)
        self.crypto_df['low'] = self.crypto_df[['low','asset']].apply(lambda x: (float(x[0]) * 134.56) if x[1] in currency_code else np.nan, axis=1)
        self.crypto_df['high'] = self.crypto_df[['high','asset']].apply(lambda x: (float(x[0]) * 134.56) if x[1] in currency_code else np.nan, axis=1)

        # drop entries with null values
        self.crypto_df.dropna(inplace=True)

        # save the csv file
        self.crypto_df.to_csv("Crypto-Yen.csv")