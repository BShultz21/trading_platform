from ingestion.handlers import batch_handler
from pipelines import bronze_to_silver, load_to_postgres
from storage.parquet import parquet
import pandas as pd

if __name__ == '__main__':
    batch_handler = batch_handler.BatchHandler()
    batch_handler.get_historical_equities_data('AAPL')
    batch_handler.get_equities_data(['AAPL', 'MSFT'])
    batch_handler.get_option_chains_data('AAPL')
    db_conn = load_to_postgres.create_db_connection()
    asset_types = ['options', 'historical_equity', 'equity']
    for asset_type in asset_types:
        bronze_df = bronze_to_silver.parse_bronze_dataframe(asset_type)
        bronze_to_silver.load_to_silver_layer(bronze_df)
        silver_df = parquet.load_parquet_file("silver", asset_type)
        load_to_postgres.load_sql_table(silver_df, asset_type, db_conn)

