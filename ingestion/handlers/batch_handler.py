from fastparquet import ParquetFile, write
from auth import api_auth
from requests import get
import pandas as pd
import datetime as dt
import os

class BatchHandler:
    def __init__(self):
        self.Schwab = api_auth.SchwabAPICredentials()
        self.auth_header = None

    def set_auth_header(self) -> None:
        self.Schwab.token_handler()
        access_token = self.Schwab.tokens['Access']['Token']
        self.auth_header = {"Authorization": f"Bearer {access_token}"}

    def call_api(self, url:str) -> str:
        self.set_auth_header()
        response = get(url, headers=self.auth_header)
        if response.status_code == 200:
            return response.text
        else:
            print(response.status_code)
            return None

    def create_pandas_dataframe(self, data:str, symbols, asset_type):
        data_dict = {"timestamp": [str(dt.date.today()).encode("UTF-8")],
                "symbols": [symbols.encode("UTF-8")],
                "asset_type": [asset_type.encode("UTF-8")],
                "raw_json": [data.encode("UTF-8")]}

        df = pd.DataFrame(data=data_dict)
        return df

    def write_parquet_file(self, dataframe, data_pull:str) -> None:
        """
        Takes pandas dataframe and type of data that it is writing (historical equities/options/etc.) and writes
        a parquet file
        """

        if data_pull in ['historical_equity', 'equity']:
            asset_type = 'equity'
        elif data_pull == 'options':
            asset_type  = 'options'
        else:
            raise ValueError

        project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        dir = project_dir + f"/storage/bronze/asset_type={asset_type}/{dt.date.today()}"
        file_name = f"{data_pull}_{dt.date.today()}"
        file_path = os.path.join(project_dir, dir, file_name)
        file_path = os.path.abspath(file_path)

        if not os.path.exists(dir):
            os.makedirs(dir)
        if os.path.isfile(file_path):
            write(file_path, dataframe, append=True)
            print("Data has been appended")
            return None
        else:
            write(file_path, dataframe)
            print("File has been created")
            return None

    def get_historical_equities_data(self, symbol: str) -> None:
        """
        This data takes equity symbol and returns data from previous year until current day
        :return:
        """
        url = f'https://api.schwabapi.com/marketdata/v1/pricehistory?symbol={symbol}&periodType=year&period=1&frequencyType=daily&frequency=1'
        response = self.call_api(url)
        df = self.create_pandas_dataframe(response, symbol, "equity")
        self.write_parquet_file(df, "historical_equity")

    def get_equities_data(self, symbols) -> None:

        symbols = '%2C'.join(symbols)
        url = f'https://api.schwabapi.com/marketdata/v1/quotes?symbols={symbols}&indicative=false'
        response = self.call_api(url)
        df = self.create_pandas_dataframe(response, symbols, "equity")
        self.write_parquet_file(df, "equity")

    def get_option_chains_data(self, symbol:str) -> None:
        """
        This takes the stock symbol and returns currently available option contracts
        :return:
        """
        url = f'https://api.schwabapi.com/marketdata/v1/chains?symbol={symbol}&contractType=ALL'
        response = self.call_api(url)
        df = self.create_pandas_dataframe(response, symbol, "options")
        self.write_parquet_file(df, "options")

if __name__ == '__main__':
    batch_handler = BatchHandler()
    batch_handler.get_option_chains_data('AAPL')
    batch_handler.get_equities_data('AAPL')
    batch_handler.get_historical_equities_data('AAPL')