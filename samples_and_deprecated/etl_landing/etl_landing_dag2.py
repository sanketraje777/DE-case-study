from airflow import DAG
from airflow.decorators import task, task_group
from airflow.utils.dates import days_ago
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd

CHUNK_SIZE = 10000

# DAG config
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    dag_id='etl_landing_parallel_chunks',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False
)

@task(pool="db_pool")
def generate_chunks(table_name: str, id_col: str, time_col: str):
    mysql = MySqlHook(mysql_conn_id='mysql_source')
    min_max_query = f"""
        SELECT MIN({id_col}) as min_id, MAX({id_col}) as max_id FROM {table_name}
        WHERE {time_col} > '2021-07-06 00:00:00' AND {time_col} <= '2021-07-07 23:59:59';
    """
    result = mysql.get_first(min_max_query)
    min_id, max_id = result
    if min_id is None or max_id is None:
        return []
    chunks = []
    for start in range(min_id, max_id + 1, CHUNK_SIZE):
        end = min(start + CHUNK_SIZE - 1, max_id)
        chunks.append({"start_id": start, "end_id": end})
    return chunks

@task()
def process_chunk(table_name: str, id_col: str, time_col: str, target_table: str, chunk):
    start_id = chunk['start_id']
    end_id = chunk['end_id']

    mysql = MySqlHook(mysql_conn_id='mysql_source')
    pg = PostgresHook(postgres_conn_id='postgres_target')

    df = mysql.get_pandas_df(f"""
        SELECT * FROM {table_name}
        WHERE {time_col} > '2021-07-06 00:00:00'
          AND {time_col} <= '2021-07-07 23:59:59'
          AND {id_col} BETWEEN {start_id} AND {end_id};
    """)

    if df.empty:
        return 'No data'

    try:
        pg.insert_rows(target_table, df.values.tolist(), target_fields=df.columns.tolist(), commit_every=1000)
    except Exception as e:
        raise RuntimeError(f"Failed to insert rows for chunk {start_id}-{end_id} of {table_name}: {e}")

    return f"Inserted {len(df)} rows into {target_table}"

with dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with TaskGroup("tg_items") as tg_items:
        chunks_items = generate_chunks.override(task_id="chunks_items")("salesorderitem", "id", "modified_at")
        process_items = process_chunk.partial(
            table_name="salesorderitem",
            id_col="id",
            time_col="modified_at",
            target_table="lz.salesorderitem"
        ).expand(chunk=chunks_items)

    with TaskGroup("tg_orders") as tg_orders:
        chunks_orders = generate_chunks.override(task_id="chunks_orders")("salesorder", "id", "modified_at")
        process_orders = process_chunk.partial(
            table_name="salesorder",
            id_col="id",
            time_col="modified_at",
            target_table="lz.salesorder"
        ).expand(chunk=chunks_orders)

    with TaskGroup("tg_customers") as tg_customers:
        chunks_customers = generate_chunks.override(task_id="chunks_customers")("customer", "id", "id")
        process_customers = process_chunk.partial(
            table_name="customer",
            id_col="id",
            time_col="id",  # assuming no timestamp for customer, use id as fallback
            target_table="lz.customer"
        ).expand(chunk=chunks_customers)

    start >> [tg_items, tg_orders, tg_customers] >> end
