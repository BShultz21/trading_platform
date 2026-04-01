from fastparquet import ParquetFile, write
import pandas as pd
import os
import datetime as dt


class BronzeLayerETL():
    def __init__(self):
        pass

    def load_parquet_to_dataframe(self):
        project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        file_dir = project_dir + f"/storage/bronze/asset_type=equity/{dt.date.today()}"
        file_name = f"/historical_equity_{dt.date.today()}"
        file_path = os.path.abspath(file_dir + file_name)

        pf = ParquetFile(file_path)
        df = pf.to_pandas()
        return df

    def parse_dataframe(self):
        df = self.load_parquet_to_dataframe()
        print(df['raw_json'])

if __name__ == '__main__':
    test = BronzeLayerETL()
    test.load_parquet_to_dataframe()
    test.parse_dataframe()