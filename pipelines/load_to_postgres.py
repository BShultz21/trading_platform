from pandas.errors import DatabaseError
from sqlalchemy import create_engine
import psycopg2

def create_db_connection():
    """
    Creates a sqlalchemy engine for postgres
    :return:
    """
    username = 'postgres'
    password = 'postgres'
    host= 'db'
    port = 5432
    dbname = 'marketdata'

    postgres_str = f'postgresql://{username}:{password}@{host}:{port}/{dbname}'
    return create_engine(postgres_str)

def load_sql_table(dataframe, table, db_connection):
    """
    Takes pandas dataframe, desired table and sqlalchemy engine to load to postgres database
    """
    dataframe = dataframe.drop(columns=['asset_type','date'])
    try:
        dataframe.to_sql(table, con=db_connection, if_exists="append")
        print("Data uploaded to SQL successfully")
    except DatabaseError:
        print(f"Data upload to SQL failure: Duplicate data detected for table {table}")