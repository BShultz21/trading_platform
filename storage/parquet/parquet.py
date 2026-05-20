import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import datetime as dt
import os


def write_parquet_file(dataframe, medallion_level) -> None:
    """
    Takes pandas dataframe and type of data that it is writing (historical equities/options/etc.) and writes
    a parquet file
    """
    output_path = Path(__file__).resolve().parent.parent / medallion_level

    table = pa.Table.from_pandas(df=dataframe, preserve_index=False)
    pq.write_to_dataset(table, output_path, partition_cols=['asset_type', 'date'])

def load_parquet_file(medallion_level, asset_type):
    """
    Takes medallion level, asset_type and loads the most recently modified parquet file
    """
    file_dir = Path(__file__).resolve().parent.parent / f"{medallion_level}/asset_type={asset_type}/date={dt.date.today()}"

    if not file_dir.exists():
        raise FileNotFoundError(f"Directory not found: {file_dir}")

    latest_file = max(file_dir.glob("*.parquet"), key=os.path.getmtime)
    pf = pq.read_table(latest_file)

    return pf.to_pandas()

