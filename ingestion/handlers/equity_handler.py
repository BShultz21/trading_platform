from fastparquet import ParquetFile, write
from auth import api_auth
from requests import get
import pandas as pd
import datetime as dt
import os

class EquityHandler():
    def __init__(self):
        self.Schwab = api_auth.SchwabAPICredentials()

    def historical_equities(self, symbol: str) -> None:
        """
        This takes the stock symbol, make sure access token is valid creates parquet file in bronze level storage
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
            project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            dir = project_dir + f"/storage/bronze/asset_type=equity/{dt.date.today()}"
            file_name = f"historical_equity_{dt.date.today()}"
            file_path = os.path.join(project_dir, dir, file_name)
            file_path = os.path.abspath(file_path)

            if not os.path.exists(dir):
                os.makedirs(dir)
            if os.path.isfile(file_path):
                write(file_path, df, append=True)
            else:
                write(file_path, df)
        else:
            print(response.status_code)


if __name__ == '__main__':
    equity_handler = EquityHandler()
    equity_handler.historical_equities('AAPL')

    pf = ParquetFile(f"historical_equity_{dt.date.today()}")
    df = pf.to_pandas()
    print(df)