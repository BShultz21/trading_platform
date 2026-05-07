import pandas as pd
from fastparquet import ParquetFile, write
from pathlib import Path
import datetime as dt
import os


def write_parquet_file(dataframe, medallion_level) -> None:
    """
    Takes pandas dataframe and type of data that it is writing (historical equities/options/etc.) and writes
    a parquet file
    """
    if medallion_level == 'bronze':
        asset_type = dataframe['asset_type'].iloc[0].decode('utf-8')
        date = dataframe['date'].iloc[0].decode('utf-8')
    else:
        asset_type = dataframe['asset_type'].iloc[0]
        date = str(dataframe['date'].iloc[0])
    full_output_path = Path(__file__).resolve().parent.parent / medallion_level / asset_type / date
    has_parquet = any(p.is_file() for p in full_output_path.rglob("*.parquet"))

    output_path = Path(__file__).resolve().parent.parent / medallion_level

    if medallion_level == 'bronze' and has_parquet:
        dataframe['asset_type'] = dataframe['asset_type'].apply(
            lambda x: x.decode('utf-8') if isinstance(x, bytes) else x
        )

        dataframe['date'] = dataframe['date'].apply(
            lambda x: x.decode('utf-8') if isinstance(x, bytes) else x
        )
        write(
            str(output_path),
            dataframe,
            file_scheme='hive',
            partition_on=['asset_type', 'date'],
            write_index=False,
            append=True
        )
    elif medallion_level == 'bronze' and not has_parquet:
        dataframe['asset_type'] = dataframe['asset_type'].apply(
            lambda x: x.decode('utf-8') if isinstance(x, bytes) else x
        )

        dataframe['date'] = dataframe['date'].apply(
            lambda x: x.decode('utf-8') if isinstance(x, bytes) else x
        )

        write(
            str(output_path),
            dataframe,
            file_scheme='hive',
            partition_on=['asset_type', 'date'],
            write_index=False
        )

    else:
        write(
            str(output_path),
            dataframe,
            file_scheme='hive',
            partition_on=['asset_type', 'date'],
            write_index=False
        )

def load_parquet_file(medallion_level, asset_type):
    file_dir = Path(__file__).resolve().parent.parent / f"{medallion_level}/asset_type={asset_type}/date={dt.date.today()}"
    pf = ParquetFile(str(file_dir)+'/'+str(os.listdir(file_dir)[-1]))
    return pf.to_pandas()

if __name__ == '__main__':
    print(load_parquet_file('bronze', 'equity'))