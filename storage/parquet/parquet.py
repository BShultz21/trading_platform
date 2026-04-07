import pathlib
from fastparquet import ParquetFile, write
from pathlib import Path
import datetime as dt


def write_parquet_file(dataframe, medallion_level, data_pull: str) -> None:
    """
    Takes pandas dataframe and type of data that it is writing (historical equities/options/etc.) and writes
    a parquet file
    """

    if data_pull in ['historical_equity', 'equity']:
        asset_type = 'equity'
    elif data_pull == 'options':
        asset_type = 'options'
    else:
        raise ValueError

    file_dir = Path(__file__).resolve().parent.parent / f"{medallion_level}/asset_type={asset_type}/{dt.date.today()}"
    file_name = f"{data_pull}_{dt.date.today()}"

    if not Path.exists(file_dir):
        Path.mkdir(file_dir)
    if Path.exists(file_dir/file_name):
        write(file_dir/file_name, dataframe, append=True)
        print("Data has been appended")
        return None
    else:
        write(file_dir/file_name, dataframe)
        print("File has been created")
        return None

def load_parquet_file(medallion_level, data_pull):
    if data_pull in ['historical_equity', 'equity']:
        asset_type = 'equity'
    elif data_pull == 'options':
        asset_type = 'options'
    else:
        raise ValueError

    file_dir = Path(__file__).resolve().parent.parent / f"{medallion_level}/asset_type={asset_type}/{dt.date.today()}"
    file_name = f"{data_pull}_{dt.date.today()}"
    pf = ParquetFile(f"{file_dir}/{file_name}")
    return pf.to_pandas()


if __name__ == '__main__':
    root_dir = Path(__file__).resolve().parent.parent
    print(root_dir)