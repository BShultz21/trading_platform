from auth import api_auth
from requests import get
from storage.parquet import parquet
from processing.dataframes import dataframes

class BatchHandler:
    def __init__(self):
        self.Schwab = api_auth.SchwabAPICredentials()
        self.auth_header = None

    def set_auth_header(self) -> None:
        """
        Sets the authorization header that is required to make Schwab API calls
        """
        self.Schwab.token_handler()
        access_token = self.Schwab.tokens['Access']['Token']
        self.auth_header = {"Authorization": f"Bearer {access_token}"}

    def call_api(self, url:str) -> str:
        """
        Takes auth url and calls Scwhab API
        """
        self.set_auth_header()
        response = get(url, headers=self.auth_header)
        if response.status_code == 200:            return response.text
        else:
            print(response.status_code)
            return None

    def get_historical_equities_data(self, symbol: str) -> None:
        """
        Takes equity symbol and returns data from previous year until current day
        """
        url = f'https://api.schwabapi.com/marketdata/v1/pricehistory?symbol={symbol}&periodType=year&period=1&frequencyType=daily&frequency=1'
        response = self.call_api(url)
        df = dataframes.create_pandas_dataframe(response, symbol, "historical_equity")
        parquet.write_parquet_file(df, "bronze")

    def get_equities_data(self, symbols) -> None:
        """
        Takes equity symbols and returns a snapshot of metrics for specified symbols
        """
        symbols = '%2C'.join(symbols)
        url = f'https://api.schwabapi.com/marketdata/v1/quotes?symbols={symbols}&fields=quote&indicative=false'
        response = self.call_api(url)
        df = dataframes.create_pandas_dataframe(response, symbols, "equity")
        parquet.write_parquet_file(df, "bronze")

    def get_option_chains_data(self, symbol:str) -> None:
        """
        This takes the stock symbol and returns currently available option contracts
        :return:
        """
        url = f'https://api.schwabapi.com/marketdata/v1/chains?symbol={symbol}&contractType=ALL'
        response = self.call_api(url)
        df = dataframes.create_pandas_dataframe(response, symbol, "options")
        parquet.write_parquet_file(df,"bronze")

