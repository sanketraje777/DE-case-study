from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd
import math
import logging

CHUNK_SIZE = int(Variable.get("chunk_size", default_var=10000))
BATCH_SIZE = int(Variable.get("batch_size", default_var=1000))
DATETIME_INTERVAL = int(Variable.get("datetime_interval", default_var=1))

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='etl_landing_dynamic_chunks_full',
    default_args=default_args,
    start_date=days_ago(DATETIME_INTERVAL),
    schedule_interval='@daily',
    catchup=False
) as dag:

    @task()
    def get_load_range():
        current_ts = datetime.now()
        last_ts = Variable.get(
            "last_loaded_ts",
            default_var=(current_ts - timedelta(days=DATETIME_INTERVAL)).strftime('%Y-%m-%d %H:%M:%S')
        )
        testing_ts = Variable.get("testing_ts", default_var="false") == "true"
        if testing_ts:
            current_ts = datetime.strptime(last_ts, "%Y-%m-%d %H:%M:%S") + timedelta(days=DATETIME_INTERVAL)
        return {"start_time": last_ts, "end_time": current_ts.strftime('%Y-%m-%d %H:%M:%S')}

    @task()
    def generate_datetime_chunks(start_time: str, end_time: str, chunk_size: int = CHUNK_SIZE):
        chunks = []
        start_window = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        end_window = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        current_start = start_window
        while current_start < end_window:
            current_end = min(current_start + timedelta(minutes=chunk_size), end_window)
            chunks.append({"start_val": current_start.strftime('%Y-%m-%d %H:%M:%S'), "end_val": current_end.strftime('%Y-%m-%d %H:%M:%S')})
            current_start = current_end
        return chunks

    @task()
    def create_temp_customer_table(start_time: str, end_time: str):
        mysql = MySqlHook(mysql_conn_id='mysql_db')
        query = f"""
            CREATE TEMPORARY TABLE temp_customer AS
            SELECT * FROM customer c
            WHERE EXISTS (
                SELECT 1 FROM (
                    SELECT s.customer_id FROM salesorder s
                    WHERE EXISTS (
                        SELECT 1 FROM (
                            SELECT order_id FROM salesorderitem WHERE modified_at >= '{start_time}' AND modified_at < '{end_time}'
                            UNION
                            SELECT id AS order_id FROM salesorder WHERE modified_at >= '{start_time}' AND modified_at < '{end_time}'
                        ) ssi WHERE ssi.order_id = s.id
                    )
                ) sc WHERE sc.customer_id = c.id
            );
        """
        mysql.run(query)
        mysql.run("ALTER TABLE temp_customer ADD PRIMARY KEY (id);")
        mysql.run("ALTER TABLE temp_customer ADD INDEX (id);")

    @task(pool="db_pool")
    def generate_id_chunks(query, no_of_chunks: int, min_chunk_size: int = CHUNK_SIZE, params=None):
        mysql = MySqlHook(mysql_conn_id='mysql_source')
        result = mysql.get_first(query, parameters=params)
        min_id, max_id = result
        if min_id is None or max_id is None:
            return []
        if max_id - min_id <= min_chunk_size:
            return [{"start_val": min_id, "end_val": max_id}]
        chunks = []
        chunk_size = math.ceil((max_id - min_id) / no_of_chunks)
        for start in range(min_id, max_id + 1, chunk_size):
            end = min(start + chunk_size, max_id + 1)
            chunks.append({"start_val": start, "end_val": end})
        return chunks

    def insert_in_batches(pg_hook, table_name, df, batch_size=BATCH_SIZE):
        cols = df.columns.tolist()
        for i in range(0, len(df), batch_size):
            pg_hook.insert_rows(
                table=table_name,
                rows=df.iloc[i:i + batch_size].values.tolist(),
                target_fields=cols,
                commit_every=batch_size
            )

    def fetch_in_chunks_cursor(mysql_hook, query, chunk):
        conn = mysql_hook.get_conn()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, (chunk['start_val'], chunk['end_val']))
        while True:
            rows = cursor.fetchmany(CHUNK_SIZE)
            if not rows:
                break
            yield pd.DataFrame(rows)
        cursor.close()
        conn.close()

    @task(pool='db_pool_max')
    def process_chunk(chunk, query: str, target_table: str):
        mysql = MySqlHook(mysql_conn_id='mysql_source')
        pg = PostgresHook(postgres_conn_id='postgres_target')
        for df in fetch_in_chunks_cursor(mysql, query, chunk):
            insert_in_batches(pg, target_table, df)

    @task()
    def update_last_loaded_ts(ts: str):
        Variable.set("last_loaded_ts", ts)

    # DAG flow
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    load_range = get_load_range()
    chunk_datetimes = generate_datetime_chunks.partial(chunk_size=CHUNK_SIZE).expand_kwargs(load_range)

    with TaskGroup("tg_items") as tg_items:
        process_chunk.partial(
            query="""
                SELECT * FROM salesorderitem 
                WHERE modified_at >= %s AND modified_at < %s
            """,
            target_table="lz.salesorderitem"
        ).expand(chunk=chunk_datetimes)

    with TaskGroup("tg_orders") as tg_orders:
        process_chunk.partial(
            query="""
                SELECT * FROM salesorder 
                WHERE modified_at >= %s AND modified_at < %s
            """,
            target_table="lz.salesorder"
        ).expand(chunk=chunk_datetimes)

    with TaskGroup("tg_customers") as tg_customers:
        create = create_temp_customer_table.expand_kwargs(load_range)
        chunks = generate_id_chunks.override(task_id="chunks_customers").partial(
            query="SELECT MIN(id), MAX(id) FROM temp_customer",
            no_of_chunks=4,
            min_chunk_size=CHUNK_SIZE,
            params=tuple(load_range.values())
        )()
        process_chunk.partial(
            query="""
                SELECT * FROM temp_customer 
                WHERE id >= %s AND id < %s
            """,
            target_table="lz.customer"
        ).expand(chunk=chunks)

    update = update_last_loaded_ts.expand(ts=[load_range['end_time']])

    start >> load_range >> chunk_datetimes >> [tg_items, tg_orders, tg_customers] >> update >> end
