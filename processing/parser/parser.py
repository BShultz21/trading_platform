from xmlrpc.client import DateTime
import pandas as pd
import json
from time import strftime, localtime
from numpy import datetime64, float64, uint64

def convert_epoch_to_local_time(epoch_time):
    return strftime('%Y-%m-%d %H:%M:%S', localtime(epoch_time))


def clean_historical_equity_data(dataframe):
    print(dataframe)
    symbol = dataframe['symbols'][0].decode('utf-8')
    json_str = dataframe['raw_json'][0].decode('utf-8')
    data = json.loads(json_str)

    columns = ['Timestamp', 'Asset_Type', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume']
    #data_types = {'Timestamp': datetime64['s'], 'Asset_Type': str, 'Symbol': str, 'Open': float64, 'High': float64, 'Low': float64,
    #            'Close': float64, 'Volume': uint64}
    cleaned_dataframe = pd.DataFrame(columns=columns)
    #cleaned_dataframe = cleaned_dataframe.astype(data_types)
    for i in range(len(data['candles'])):
        cleaned_dataframe.loc[i, 'Timestamp'] = pd.to_datetime(data['candles'][i]['datetime'], unit= 'ms')
        cleaned_dataframe.loc[i, 'Asset_Type'] = 'Historical_Equity'
        cleaned_dataframe.loc[i, 'Symbol'] = symbol
        cleaned_dataframe.loc[i, 'Open'] = data['candles'][i]['open']
        cleaned_dataframe.loc[i, 'High'] = data['candles'][i]['high']
        cleaned_dataframe.loc[i, 'Low'] = data['candles'][i]['low']
        cleaned_dataframe.loc[i, 'Close'] = data['candles'][i]['close']
        cleaned_dataframe.loc[i, 'Volume'] = data['candles'][i]['volume']

    print(cleaned_dataframe)