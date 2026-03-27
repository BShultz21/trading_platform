from fastparquet import ParquetFile, write
from auth import api_auth
from requests import get
import pandas as pd
import datetime as dt



if __name__ == '__main__':
    Schwab = api_auth.SchwabAPICredentials()
    Schwab.token_handler()
    access_token = Schwab.tokens['Access']['Token']
    headers = {"Authorization": f"Bearer {access_token}"}

    url = 'https://api.schwabapi.com/marketdata/v1/pricehistory?symbol=AAPL&periodType=year&period=1&frequencyType=daily&frequency=1'

    response = get(url, headers=headers)
    data = {"timestamp" : [str(dt.datetime.now()).encode("UTF-8")],
            "symbol": ["AAPL".encode("UTF-8")],
            "asset_type": ["equities".encode("UTF-8")],
            "raw_json": [response.text.encode("UTF-8")]}

    df = pd.DataFrame(data=data)
    write("languages.parquet", df, append=True)
    pf = ParquetFile("languages.parquet")
    df = pf.to_pandas()
    print(df)