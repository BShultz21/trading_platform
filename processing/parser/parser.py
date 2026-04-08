import pandas as pd
import json
import datetime as dt

def clean_equity_data(dataframe):
    symbol = dataframe['symbols'][0].decode('utf-8')
    json_str = dataframe['raw_json'][0].decode('utf-8')
    data = json.loads(json_str)

    columns = ['timestamp', 'asset_type', 'symbol', 'open', 'high', 'low', 'close', 'volume']
    #data_types = {'Timestamp': datetime64['s'], 'Asset_Type': str, 'Symbol': str, 'Open': float64, 'High': float64, 'Low': float64,
    #            'Close': float64, 'Volume': uint64}
    cleaned_dataframe = pd.DataFrame(columns=columns)
    #cleaned_dataframe = cleaned_dataframe.astype(data_types)
    for i in range(len(data['candles'])):
        cleaned_dataframe.loc[i, 'timestamp'] = pd.to_datetime(data['candles'][i]['datetime'], unit= 'ms')
        cleaned_dataframe.loc[i, 'asset_type'] = 'Equity'
        cleaned_dataframe.loc[i, 'symbol'] = symbol
        cleaned_dataframe.loc[i, 'open'] = data['candles'][i]['open']
        cleaned_dataframe.loc[i, 'high'] = data['candles'][i]['high']
        cleaned_dataframe.loc[i, 'low'] = data['candles'][i]['low']
        cleaned_dataframe.loc[i, 'close'] = data['candles'][i]['close']
        cleaned_dataframe.loc[i, 'volume'] = data['candles'][i]['volume']

    print(cleaned_dataframe)

def clean_option_data(dataframe):
    symbol = dataframe['symbols'][0].decode('utf-8')
    json_str = dataframe['raw_json'][0].decode('utf-8')
    data = json.loads(json_str)

    available_options = []
    for date in data['callExpDateMap'].keys():
        for strike_price in data['callExpDateMap'][date].keys():
            available_options.append((date,strike_price))
    columns = ['timestamp', 'asset_type', 'symbol', 'expiration_date', 'option_type', 'strike_price', 'bid', 'ask',
                'bid_size', 'ask_size', 'mark_price', 'last_price', 'volume', 'volatility', 'open_interest', 'delta',
               'gamma', 'theta', 'vega', 'rho']
    today_timestamp = dt.datetime.now()
    cleaned_dataframe = pd.DataFrame(columns=columns)
    for i in range(len(available_options)):
        date = available_options[i][0]
        strike_price = available_options[i][1]
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

    print(cleaned_dataframe)