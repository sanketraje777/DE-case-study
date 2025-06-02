from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import timedelta
import pandas as pd
import logging

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    dag_id='etl_landing_temp_tables_streaming_cursor',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
)

chunk_size = 10000

def insert_in_batches(pg_hook, table_name, df, batch_size=1000):
    cols = df.columns.tolist()
    for i in range(0, len(df), batch_size):
        try:
            pg_hook.insert_rows(table=table_name, rows=df[i:i+batch_size].values.tolist(), target_fields=cols)
        except Exception as e:
            logging.error(f"Error inserting batch into {table_name}: {e}")
            raise

def fetch_in_chunks_cursor(mysql_hook, query, chunk_size=chunk_size):
    conn = mysql_hook.get_conn()
    cursor = conn.cursor(dictionary=True, buffered=False)  # fetch results as dicts and use unbuffered (server-side) cursor
    cursor.execute(query)
    while True:
        rows = cursor.fetchmany(chunk_size)
        if not rows:
            break
        yield pd.DataFrame(rows)
    cursor.close()
    conn.close()

def run_etl_landing(**context):
    mysql = MySqlHook(mysql_conn_id='mysql_source')
    pg = PostgresHook(postgres_conn_id='postgres_target')

    last_ts = '2021-07-06 00:00:00'
    current_ts = '2021-07-07 23:59:59'

    # Create temp tables (same as before)
    queries = [
        f"""
        CREATE TEMPORARY TABLE temp_filtered_items AS
        SELECT * FROM salesorderitem
        WHERE modified_at > '{last_ts}' AND modified_at <= '{current_ts}';
        """,
        """
        CREATE TEMPORARY TABLE temp_filtered_order_ids AS
        SELECT DISTINCT order_id FROM temp_filtered_items;
        """,
        """
        CREATE TEMPORARY TABLE temp_filtered_orders AS
        SELECT s.id AS order_id, s.customer_id
        FROM salesorder s
        JOIN temp_filtered_order_ids fo ON s.id = fo.order_id;
        """,
        """
        CREATE TEMPORARY TABLE temp_filtered_customers AS
        SELECT DISTINCT customer_id FROM temp_filtered_orders;
        """
    ]
    for query in queries:
        mysql.run(query)

    # Queries for final selects
    df_items_query = "SELECT * FROM temp_filtered_items"
    df_orders_query = """
        SELECT s.* FROM salesorder s
        JOIN temp_filtered_orders fo ON s.id = fo.order_id
    """
    df_customers_query = """
        SELECT c.* FROM customer c
        JOIN temp_filtered_customers fc ON c.id = fc.customer_id
    """

    # Stream fetch and insert in batches for salesorderitem
    for chunk in fetch_in_chunks_cursor(mysql, df_items_query):
        insert_in_batches(pg, 'lz.salesorderitem', chunk)

    # Stream fetch and insert in batches for salesorder
    for chunk in fetch_in_chunks_cursor(mysql, df_orders_query):
        insert_in_batches(pg, 'lz.salesorder', chunk)

    # Stream fetch and insert in batches for customer
    for chunk in fetch_in_chunks_cursor(mysql, df_customers_query):
        insert_in_batches(pg, 'lz.customer', chunk)

etl_task = PythonOperator(
    task_id='run_etl_landing_streaming',
    python_callable=run_etl_landing,
    provide_context=True,
    dag=dag,
)

etl_task
