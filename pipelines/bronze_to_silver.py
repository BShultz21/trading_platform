from storage.parquet import parquet
from processing.parser import parser


def parse_bronze_dataframe(data_pull):
    dataframe = parquet.load_parquet_file('bronze', data_pull)

    if data_pull == 'equity':
        return parser.clean_equity_data(dataframe)
    elif data_pull == 'options':
        return parser.clean_option_data(dataframe)
    elif data_pull == 'historical_equity':
        return parser.clean_historical_equity_data(dataframe)
    else:
        print("Invalid data pull")
        raise ValueError

def load_to_silver_layer(dataframe):
    parquet.write_parquet_file(dataframe, 'silver')

if __name__ == '__main__':
    df = parse_bronze_dataframe('options')
    load_to_silver_layer(df)
