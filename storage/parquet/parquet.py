import pandas as pd
from fastparquet import ParquetFile, write
from pathlib import Path
import datetime as dt


def write_parquet_file(dataframe, medallion_level) -> None:
    """
    Takes pandas dataframe and type of data that it is writing (historical equities/options/etc.) and writes
    a parquet file
    """

    output_path = Path(__file__).resolve().parent.parent / medallion_level
    output_path.mkdir(parents=True, exist_ok=True)
    has_parquet = any(p.is_file() for p in output_path.rglob("*.parquet"))
    
    if medallion_level == 'bronze':
        dataframe['asset_type'] = dataframe['asset_type'].apply(
            lambda x: x.decode('utf-8') if isinstance(x, bytes) else x
        )

        dataframe['date'] = dataframe['date'].apply(
            lambda x: x.decode('utf-8') if isinstance(x, bytes) else x
        )
        if has_parquet:
            write(
                str(output_path),
                dataframe,
                file_scheme='hive',
                partition_on=['asset_type', 'date'],
                write_index=False,
                append=True
            )
        else:
            write(
                str(output_path),
                dataframe,
                file_scheme='hive',
                partition_on=['asset_type', 'date'],
                write_index=False
            )
    else:
        dataframe['asset_type'] = dataframe['asset_type'].apply(
            lambda x: x.decode('utf-8') if isinstance(x, bytes) else x
        )

        dataframe['timestamp'] = dataframe['timestamp'].apply(
            lambda x: x.decode('utf-8') if isinstance(x, bytes) else x
        )

        if has_parquet:
            write(
                str(output_path),
                dataframe,
                file_scheme='hive',
                partition_on=['asset_type', 'date'],
                write_index=False,
                append=True
            )
        else:
            write(
                str(output_path),
                dataframe,
                file_scheme='hive',
                partition_on=['asset_type', 'date'],
                write_index=False
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