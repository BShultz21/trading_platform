import pandas as pd
import os
import datetime as dt
from storage.parquet import parquet
from processing.parser import parser

class BronzeLayerETL:
    def __init__(self):
        pass

    def parse_dataframe(self):
        df = self.load_parquet_to_dataframe()
        print(df['raw_json'])

if __name__ == '__main__':
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', None)
    df = parquet.load_parquet_file('bronze', 'options')
    parser.clean_option_data(df)