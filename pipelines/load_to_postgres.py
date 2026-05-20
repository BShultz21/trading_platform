from sqlalchemy import create_engine
import psycopg2

def create_db_connection():
    username = 'postgres'
    password = 'postgres'
    ipaddress = 'localhost'
    port = 5332
    dbname = 'bank'

    postgres_str = f'postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'
    return create_engine(postgres_str)

def load_sql_table(dataframe, table, db_connection):
    dataframe = dataframe.drop(columns=['asset_type','date'])
    dataframe.to_sql(table, con=db_connection, if_exists="append")
    print("Data uploaded to SQL successfully")