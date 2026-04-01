from fastparquet import ParquetFile, write
from pandas.core.interchange import dataframe

from auth import api_auth
from requests import get
import pandas as pd
import datetime as dt
import os

class BatchHandler:
    def __init__(self):
        self.Schwab = api_auth.SchwabAPICredentials()

    def write_parquet_file(self, dataframe, data_pull:str) -> None:
        """
        Takes pandas dataframe and type of data that it is writing (historical equities/options/etc.) and writes
        a parquet file
        """

        if data_pull == 'historical_equity':
            asset_type = 'equity'
        elif data_pull == 'options':
            asset_type  = 'options'
        else:
            return None

        project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        dir = project_dir + f"/storage/bronze/asset_type={asset_type}/{dt.date.today()}"
        file_name = f"{data_pull}_{dt.date.today()}"
        file_path = os.path.join(project_dir, dir, file_name)
        file_path = os.path.abspath(file_path)

        if not os.path.exists(dir):
            os.makedirs(dir)
        if os.path.isfile(file_path):
            write(file_path, dataframe, append=True)
            return None
        else:
            write(file_path, dataframe)
            return None

    def historical_equities(self, symbol: str) -> None:
        """
        This data takes equity symbol and returns data from previous year until current day
        :return:
        """
        self.Schwab.token_handler()
        access_token = self.Schwab.tokens['Access']['Token']
        auth_header = {"Authorization": f"Bearer {access_token}"}

        url = f'https://api.schwabapi.com/marketdata/v1/pricehistory?symbol={symbol}&periodType=year&period=1&frequencyType=daily&frequency=1'
        response = get(url, headers=auth_header)
        if response.status_code == 200:
            data = {"timestamp": [str(dt.date.today()).encode("UTF-8")],
                    "symbol": [symbol.encode("UTF-8")],
                    "asset_type": ["equities".encode("UTF-8")],
                    "raw_json": [response.text.encode("UTF-8")]}
            df = pd.DataFrame(data=data)

            self.write_parquet_file(df, 'historical_equity')
        else:
            print(response.status_code)

    def get_equities(self) -> None:
        self.Schwab.token_handler()
        access_token = self.Schwab.tokens['Access']['Token']
        auth_header = {"Authorization": f"Bearer {access_token}"}

        #Need to implement list of stock symbols. This is in progress
        url = f'https://api.schwabapi.com/marketdata/v1/quotes?symbols=AAPL%2CMSFT%2CTSLA&indicative=false'
        response = get(url, headers=auth_header)
        if response.status_code == 200:
            data = {"timestamp": [str(dt.date.today()).encode("UTF-8")],
                    "symbol": [symbol.encode("UTF-8")],
                    "asset_type": ["options".encode("UTF-8")],
                    "raw_json": [response.text.encode("UTF-8")]}
            df = pd.DataFrame(data=data)

            self.write_parquet_file(df, 'equity')
        else:
            print(response.status_code)

    def get_option_chains(self, symbol:str) -> None:
        """
        This takes the stock symbol and returns currently available option contracts
        :return:
        """
        self.Schwab.token_handler()
        access_token = self.Schwab.tokens['Access']['Token']
        auth_header = {"Authorization": f"Bearer {access_token}"}

        url = f'https://api.schwabapi.com/marketdata/v1/chains?symbol={symbol}&contractType=ALL'
        response = get(url, headers=auth_header)
        if response.status_code == 200:
            data = {"timestamp": [str(dt.date.today()).encode("UTF-8")],
                    "symbol": [symbol.encode("UTF-8")],
                    "asset_type": ["options".encode("UTF-8")],
                    "raw_json": [response.text.encode("UTF-8")]}
            df = pd.DataFrame(data=data)

            self.write_parquet_file(df, 'options')
        else:
            print(response.status_code)

if __name__ == '__main__':
    batch_handler = BatchHandler()
    batch_handler.get_equities('aapl')