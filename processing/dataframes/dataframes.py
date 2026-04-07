import pandas as pd
import datetime as dt

def create_pandas_dataframe(data: str, symbols, asset_type):
    data_dict = {"timestamp": [str(dt.date.today()).encode("UTF-8")],
                 "symbols": [symbols.encode("UTF-8")],
                 "asset_type": [asset_type.encode("UTF-8")],
                 "raw_json": [data.encode("UTF-8")]}

    df = pd.DataFrame(data=data_dict)
    return df