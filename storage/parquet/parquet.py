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
        dataframe['asset_type'] = dataframe['asset_type'].apply(
            lambda x: x.decode('utf-8') if isinstance(x, bytes) else x
        )
        dataframe['date'] = dataframe['date'].apply(
            lambda x: x.decode('utf-8') if isinstance(x, bytes) else x
        )
    else:
        asset_type = dataframe['asset_type'].iloc[0]
        date = str(dataframe['date'].iloc[0])

    output_path = Path(__file__).resolve().parent.parent / medallion_level / f'asset_type={asset_type}'
    full_output_path = Path(__file__).resolve().parent.parent / f"{medallion_level}/asset_type={asset_type}/date={dt.date.today()}"

    existing_files = list(full_output_path.rglob("*.parquet"))
    should_append = len(existing_files) > 0

    print(f"--- write_parquet_file ---")
    print(f"asset_type: {repr(asset_type)}")
    print(f"date: {repr(date)}")
    print(f"full_output_path: {full_output_path}")
    print(f"path exists: {full_output_path.exists()}")
    print(f"existing_files: {existing_files}")
    print(f"should_append: {should_append}")
    print(f"dataframe columns: {dataframe.columns.tolist()}")
    print(f"dataframe shape: {dataframe.shape}")

    write(
        str(output_path),
        dataframe,
        file_scheme='hive',
        partition_on=['date'],
        write_index=False,
        append=should_append
    )

def load_parquet_file(medallion_level, asset_type):
    file_dir = Path(__file__).resolve().parent.parent / f"{medallion_level}/asset_type={asset_type}/date={dt.date.today()}"

    if not file_dir.exists():
        raise FileNotFoundError(f"Directory not found: {file_dir}")

    latest_file = max(file_dir.glob("*.parquet"), key=os.path.getmtime)
    pf = ParquetFile(str(latest_file))

    return pf.to_pandas()

