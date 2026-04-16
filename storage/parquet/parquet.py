import pandas as pd
from fastparquet import ParquetFile, write
from pathlib import Path
import datetime as dt


def write_parquet_file(dataframe, medallion_level, data_pull: str) -> None:
    """
    Takes pandas dataframe and type of data that it is writing (historical equities/options/etc.) and writes
    a parquet file
    """

    if medallion_level == 'bronze':
        dataframe['asset_type'] = dataframe['asset_type'].apply(
            lambda x: x.decode('utf-8') if isinstance(x, bytes) else x
        )

        dataframe['date'] = dataframe['date'].apply(
            lambda x: x.decode('utf-8') if isinstance(x, bytes) else x
        )
    else:
        dataframe['asset_type'] = dataframe['asset_type'].apply(
            lambda x: x.decode('utf-8') if isinstance(x, bytes) else x
        )

        dataframe['timestamp'] = dataframe['timestamp'].apply(
            lambda x: x.decode('utf-8') if isinstance(x, bytes) else x
        )

    write(
        str(Path(__file__).resolve().parent.parent / medallion_level),
        dataframe,
        file_scheme='hive',
        partition_on=['asset_type', 'date'],
        write_index=False,
        append=True
    )

def load_parquet_file(medallion_level, data_pull):
    if data_pull in ['historical_equity', 'equity']:
        asset_type = 'equity'
    elif data_pull == 'options':
        asset_type = 'options'
    else:
        raise ValueError

    file_dir = Path(__file__).resolve().parent.parent / f"{medallion_level}/asset_type={asset_type}/date={dt.date.today()}"
    #file_name = f"{data_pull}_{dt.date.today()}"
    pf = ParquetFile(file_dir/'part.0.parquet')
    return pf.to_pandas()


if __name__ == '__main__':
    root_dir = Path(__file__).resolve().parent.parent
    print(root_dir)