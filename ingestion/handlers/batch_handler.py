from auth import api_auth
from requests import get
from storage.parquet import parquet
from processing.dataframes import dataframes

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

    def get_historical_equities_data(self, symbol: str) -> None:
        """
        This data takes equity symbol and returns data from previous year until current day
        :return:
        """
        url = f'https://api.schwabapi.com/marketdata/v1/pricehistory?symbol={symbol}&periodType=year&period=1&frequencyType=daily&frequency=1'
        response = self.call_api(url)
        df = dataframes.create_pandas_dataframe(response, symbol, "equity")
        parquet.write_parquet_file(df, "bronze", "historical_equity")

    def get_equities_data(self, symbols) -> None:

        symbols = '%2C'.join(symbols)
        url = f'https://api.schwabapi.com/marketdata/v1/quotes?symbols={symbols}&indicative=false'
        response = self.call_api(url)
        df = dataframes.create_pandas_dataframe(response, symbols, "equity")
        parquet.write_parquet_file(df, "bronze", "equity")

    def get_option_chains_data(self, symbol:str) -> None:
        """
        This takes the stock symbol and returns currently available option contracts
        :return:
        """
        url = f'https://api.schwabapi.com/marketdata/v1/chains?symbol={symbol}&contractType=ALL'
        response = self.call_api(url)
        df = dataframes.create_pandas_dataframe(response, symbol, "options")
        parquet.write_parquet_file(df,"bronze", "options")


if __name__ == '__main__':
    batch_handler = BatchHandler()
    batch_handler.get_historical_equities_data('AAPL')
    batch_handler.get_equities_data('AAPL')
    batch_handler.get_option_chains_data('AAPL')