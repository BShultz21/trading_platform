import pandas as pd
import json
import datetime as dt
from numpy import datetime64, float64, uint64


def clean_historical_equity_data(dataframe):
    symbol = dataframe['symbols'][0].decode('utf-8')
    json_str = dataframe['raw_json'][0].decode('utf-8')
    data = json.loads(json_str)

    columns = ['date','timestamp', 'asset_type', 'symbol', 'open', 'high', 'low', 'close', 'volume']
    data_types = {'timestamp': 'datetime64[s]', 'asset_type': object, 'symbol': object, 'open': float64, 'high': float64, 'low': float64,
               'close': float64, 'volume': uint64}
    cleaned_dataframe = pd.DataFrame(columns=columns)
    for i in range(len(data['candles'])):
        cleaned_dataframe.loc[i, 'date'] = dt.date.today()
        cleaned_dataframe.loc[i, 'timestamp'] = data['candles'][i]['datetime']
        cleaned_dataframe.loc[i, 'asset_type'] = 'historical_equity'
        cleaned_dataframe.loc[i, 'symbol'] = symbol
        cleaned_dataframe.loc[i, 'open'] = data['candles'][i]['open']
        cleaned_dataframe.loc[i, 'high'] = data['candles'][i]['high']
        cleaned_dataframe.loc[i, 'low'] = data['candles'][i]['low']
        cleaned_dataframe.loc[i, 'close'] = data['candles'][i]['close']
        cleaned_dataframe.loc[i, 'volume'] = data['candles'][i]['volume']

    cleaned_dataframe['timestamp'] = pd.to_datetime(cleaned_dataframe['timestamp'], unit='ms')
    cleaned_dataframe = cleaned_dataframe.astype(data_types)
    return cleaned_dataframe

def clean_equity_data(dataframe):
    symbols = dataframe['symbols'][0].decode('utf-8')
    symbols = symbols.split('%2C')
    json_str = dataframe['raw_json'][0].decode('utf-8')
    data = json.loads(json_str)

    columns = ['date','timestamp', 'asset_type', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'bid', 'ask', 'mark']
    data_types = {'timestamp': 'datetime64[s]', 'asset_type': object, 'symbol': object, 'open': float64, 'high': float64,
                  'close': float64, 'volume': uint64, 'bid': float64, 'ask': float64, 'mark': float64}

    cleaned_dataframe = pd.DataFrame(columns=columns)
    for symbol in symbols:
        cleaned_dataframe.loc[symbol, 'date'] = dt.date.today()
        cleaned_dataframe.loc[symbol, 'timestamp'] = data[symbol]['quote']['quoteTime']
        cleaned_dataframe.loc[symbol, 'asset_type'] = 'equity'
        cleaned_dataframe.loc[symbol, 'symbol'] = symbol
        cleaned_dataframe.loc[symbol, 'open'] = data[symbol]['quote']['openPrice']
        cleaned_dataframe.loc[symbol, 'high'] = data[symbol]['quote']['highPrice']
        cleaned_dataframe.loc[symbol, 'low'] = data[symbol]['quote']['lowPrice']
        cleaned_dataframe.loc[symbol, 'close'] = data[symbol]['quote']['closePrice']
        cleaned_dataframe.loc[symbol, 'volume'] = data[symbol]['quote']['totalVolume']
        cleaned_dataframe.loc[symbol, 'bid'] = data[symbol]['quote']['bidPrice']
        cleaned_dataframe.loc[symbol, 'ask'] = data[symbol]['quote']['askPrice']
        cleaned_dataframe.loc[symbol, 'mark'] = data[symbol]['quote']['mark']

    cleaned_dataframe['timestamp'] = pd.to_datetime(cleaned_dataframe['timestamp'], unit='ms')
    cleaned_dataframe = cleaned_dataframe.astype(data_types)
    return cleaned_dataframe

def clean_option_data(dataframe):
    symbol = dataframe['symbols'][0].decode('utf-8')
    json_str = dataframe['raw_json'][0].decode('utf-8')
    data = json.loads(json_str)

    available_options = []
    for date in data['callExpDateMap'].keys():
        for strike_price in data['callExpDateMap'][date].keys():
            available_options.append((date,strike_price))
    columns = ['date','timestamp', 'asset_type', 'symbol', 'expiration_date', 'option_type', 'strike_price', 'bid', 'ask',
                'bid_size', 'ask_size', 'mark_price', 'last_price', 'volume', 'volatility', 'open_interest', 'delta',
               'gamma', 'theta', 'vega', 'rho']
    data_types = {'timestamp': 'datetime64[s]', 'asset_type': object, 'symbol': object, 'expiration_date': 'datetime64[ns]',
                  'option_type': object, 'strike_price': float64, 'bid': float64, 'ask': float64, 'bid_size': uint64,
                  'ask_size': uint64, 'mark_price': float64, 'last_price': float64, 'volume': uint64, 'volatility': float64,
                  'open_interest': uint64, 'delta': float64, 'gamma': float64, 'theta': float64, 'vega': float64, 'rho': float64}
    today_timestamp = dt.datetime.now()
    cleaned_dataframe = pd.DataFrame(columns=columns)
    for i in range(len(available_options)):
        date = available_options[i][0]
        strike_price = available_options[i][1]
        cleaned_dataframe.loc[i, 'date'] = dt.date.today()
        cleaned_dataframe.loc[i, 'timestamp'] = today_timestamp
        cleaned_dataframe.loc[i, 'asset_type'] = 'options'
        cleaned_dataframe.loc[i, 'symbol'] = symbol
        cleaned_dataframe.loc[i, 'expiration_date'] = data['callExpDateMap'][date][strike_price][0]['expirationDate']
        cleaned_dataframe.loc[i, 'option_type'] = data['callExpDateMap'][date][strike_price][0]['putCall']
        cleaned_dataframe.loc[i, 'strike_price'] = data['callExpDateMap'][date][strike_price][0]['strikePrice']
        cleaned_dataframe.loc[i, 'bid'] = data['callExpDateMap'][date][strike_price][0]['bid']
        cleaned_dataframe.loc[i, 'ask'] = data['callExpDateMap'][date][strike_price][0]['ask']
        cleaned_dataframe.loc[i, 'bid_size'] = data['callExpDateMap'][date][strike_price][0]['bidSize']
        cleaned_dataframe.loc[i, 'ask_size'] = data['callExpDateMap'][date][strike_price][0]['askSize']
        cleaned_dataframe.loc[i, 'mark_price'] = data['callExpDateMap'][date][strike_price][0]['mark']
        cleaned_dataframe.loc[i, 'last_price'] = data['callExpDateMap'][date][strike_price][0]['last']
        cleaned_dataframe.loc[i, 'volume'] = data['callExpDateMap'][date][strike_price][0]['totalVolume']
        cleaned_dataframe.loc[i, 'volatility'] = data['callExpDateMap'][date][strike_price][0]['volatility']
        cleaned_dataframe.loc[i, 'open_interest'] = data['callExpDateMap'][date][strike_price][0]['openInterest']
        cleaned_dataframe.loc[i, 'delta'] = data['callExpDateMap'][date][strike_price][0]['delta']
        cleaned_dataframe.loc[i, 'gamma'] = data['callExpDateMap'][date][strike_price][0]['gamma']
        cleaned_dataframe.loc[i, 'theta'] = data['callExpDateMap'][date][strike_price][0]['theta']
        cleaned_dataframe.loc[i, 'vega'] = data['callExpDateMap'][date][strike_price][0]['vega']
        cleaned_dataframe.loc[i, 'rho'] = data['callExpDateMap'][date][strike_price][0]['rho']

    cleaned_dataframe['timestamp'] = pd.to_datetime(cleaned_dataframe['timestamp'], unit='ms')
    cleaned_dataframe['expiration_date'] = pd.to_datetime(cleaned_dataframe['expiration_date'],utc=True)
    cleaned_dataframe['expiration_date'] = pd.to_datetime(cleaned_dataframe['expiration_date'].dt.tz_localize(None))
    cleaned_dataframe = cleaned_dataframe.astype(data_types)

    return cleaned_dataframe