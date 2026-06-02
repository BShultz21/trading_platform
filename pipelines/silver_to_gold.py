from sqlalchemy import text
import psycopg2

def refresh_mat_views(db_conn):
    with db_conn.connect() as connection:
        views = ['equity_hourly_metrics', 'equity_rankings', 'options_chain_summary', 'options_expiration_summary']
        for view in views:
            connection.execute(text(f"refresh materialized view {view};"))
            print(f"{view} view has been refreshed")
        connection.commit()