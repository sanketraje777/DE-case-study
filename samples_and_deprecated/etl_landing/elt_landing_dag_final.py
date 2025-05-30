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
import sys
import logging

CHUNK_SIZE = int(Variable.get("chunk_size", default_var=10000))
BATCH_SIZE = int(Variable.get("batch_size", default_var=1000))
DATETIME_INTERVAL = int(Variable.get("datetime_interval", default_var=1))   # in days

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

    def fetch_min_max_dt(start_time: str = None, end_time: str = None, query=None, params=None):
        min_dt_str = start_time
        max_dt_str = end_time
        if query:
            mysql = MySqlHook(mysql_conn_id='mysql_source')
            result = mysql.get_first(query, parameters=params)
            min_dt_str, max_dt_str = result  # expected to be (min, max) as strings
        if min_dt_str is None or max_dt_str is None:
            return None, None
        min_dt = datetime.strptime(min_dt_str, "%Y-%m-%d %H:%M:%S")
        max_dt = datetime.strptime(max_dt_str, "%Y-%m-%d %H:%M:%S")
        if query:
            max_dt += timedelta(seconds=1)
        return min_dt, max_dt

    @task()
    def generate_datetime_chunks(type_: str = "fixed", no_of_chunks: int = None, chunk_size: int = CHUNK_SIZE, min_chunk_minutes: int = CHUNK_SIZE, **kwargs):
        """
        Generalized chunk generator that handles both 'fixed' and 'dynamic' types.
        :param type_: 'fixed' or 'dynamic'
        :param no_of_chunks: Used in 'fixed' type to determine chunk count
        :param chunk_size: Used in 'dynamic' type for fixed chunk size in minutes
        :param min_chunk_minutes: Minimum chunk size in minutes for 'fixed' type
        :param kwargs: Passed to fetch_min_max_dt
        :return: List of chunks with start and end datetimes
        """
        min_dt, max_dt = fetch_min_max_dt(**kwargs)
        chunks = []
        if min_dt is None or max_dt is None:
            return chunks
        if type_ == "fixed":
            total_minutes = (max_dt - min_dt).total_seconds() // 60
            if total_minutes <= min_chunk_minutes:
                chunks.append({
                    "start_val": min_dt.strftime('%Y-%m-%d %H:%M:%S'),
                    "end_val": max_dt.strftime('%Y-%m-%d %H:%M:%S')
                })
                return chunks
            # Determine chunk size
            chunk_size_minutes = math.ceil(total_minutes / no_of_chunks)
        elif type_ == "dynamic":
            chunk_size_minutes = chunk_size
        else:
            raise ValueError(f"Unknown type: {type_}")
        # Generate chunks
        current_start = min_dt
        while current_start < max_dt:
            current_end = min(current_start + timedelta(minutes=chunk_size_minutes), max_dt)
            chunks.append({
                "start_val": current_start.strftime('%Y-%m-%d %H:%M:%S'),
                "end_val": current_end.strftime('%Y-%m-%d %H:%M:%S')
            })
            current_start = current_end
        logging.info(f"Generated datetime chunks: {chunks}")
        return chunks

    @task()
    def create_temporary_table_customer(start_time: str, end_time: str):
        mysql = MySqlHook(mysql_conn_id='mysql_db')
        queries = [
            """
            CREATE TEMPORARY TABLE temp_customer AS
            SELECT *
            FROM customer c
            WHERE EXISTS
                (SELECT 1
                FROM
                (SELECT s.customer_id 
                FROM salesorder s
                WHERE EXISTS
                    (SELECT 1
                    FROM
                        (SELECT order_id FROM salesorderitem WHERE modified_at >= %s AND modified_at < %s 
                        UNION
                        SELECT id as order_id FROM salesorder WHERE modified_at >= %s AND modified_at < %s")
                    AS ssi 
                    WHERE ssi.order_id = s.id))
                AS sc
                WHERE sc.customer_id = c.id);
            """,
            "ALTER TABLE temp_customer ADD PRIMARY KEY (id);",
            "ALTER TABLE temp_customer ADD INDEX (id);"
        ]
        params = [(start_time, end_time), None, None]
        for query, param in zip(queries, params):
            mysql.run(query, parameters=param)
        return mysql

    def fetch_min_max_num(start_val: str = None, end_val: str = None, query=None, params=None, is_integer=True):
        min_num_str = start_val
        max_num_str = end_val
        if query:
            mysql = MySqlHook(mysql_conn_id='mysql_source')
            result = mysql.get_first(query, parameters=params)
            min_num_str, max_num_str = result  # expected to be (min, max) as strings
        if min_num_str is None or max_num_str is None:
            return None, None
        min_num = float(min_num_str)
        max_num = float(max_num_str)
        # For query case, add smallest float precision
        if query:
            max_num += 1 if is_integer else sys.float_info.epsilon
        return min_num, max_num

    @task()
    def generate_num_chunks(
        type_: str = "fixed",
        no_of_chunks: int = None,
        chunk_size: int = CHUNK_SIZE,
        min_chunk_size: int = CHUNK_SIZE,
        is_integer = True,
        **kwargs
    ):
        """
        Generalized chunk generator for numeric values (int or float).
        :param type_: 'fixed' or 'dynamic'
        :param no_of_chunks: Number of chunks if type is 'fixed'
        :param chunk_size: Size of each chunk for 'dynamic'
        :param min_chunk_size: Minimum chunk size for 'fixed'
        :param is_integer: return chunks as rounded integers instead of floats
        :param kwargs: Passed to fetch_min_max_num
        :return: List of chunks with start and end numeric values
        """
        min_num, max_num = fetch_min_max_num(**kwargs)
        chunks = []
        if min_num is None or max_num is None:
            return chunks
        if type_ == "fixed":
            total_size = max_num - min_num
            if total_size <= min_chunk_size:
                chunks.append({
                    "start_val": round(min_num) if is_integer else min_num,
                    "end_val": round(max_num) if is_integer else max_num
                })
                return chunks
            # Determine chunk size
            chunk_size_val = total_size / no_of_chunks
            if is_integer:
                chunk_size_val = math.ceil(chunk_size_val)
        elif type_ == "dynamic":
            chunk_size_val = chunk_size
        else:
            raise ValueError(f"Unknown type: {type_}")
        # Generate chunks
        current_start = min_num
        if is_integer:
            while current_start < max_num:
                current_end = min(current_start + chunk_size_val, max_num)
                chunks.append({
                    "start_val": round(current_start),
                    "end_val": round(current_end)
                })
                current_start = current_end
        else:
            while current_start < max_num:
                current_end = min(current_start + chunk_size_val, max_num)
                chunks.append({
                    "start_val": current_start,
                    "end_val": current_end
                })
                current_start = current_end
        logging.info(f"Generated numeric chunks: {chunks}")
        return chunks

    def insert_in_batches(pg_hook, table_name, df, batch_size=BATCH_SIZE):
        cols = df.columns.tolist()
        for i in range(0, len(df), batch_size):
            try:
                pg_hook.insert_rows(
                    table=table_name,
                    rows=df.iloc[i:i+batch_size].values.tolist(),
                    target_fields=cols,
                    commit_every=batch_size
                )
            except Exception as e:
                logging.error(f"Error inserting batch into {table_name}: {e}")
                raise

    def fetch_in_chunks_cursor(mysql_hook, query, chunk, chunk_size=CHUNK_SIZE):
        conn = mysql_hook.get_conn()
        cursor = conn.cursor(dictionary=True, buffered=False)  # fetch results as dicts and use unbuffered (server-side) cursor
        cursor.execute(query, (chunk['start_val'], chunk['end_val']))
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            yield pd.DataFrame(rows)
        cursor.close()
        conn.close()

    @task(pool='db_pool_max')
    def process_chunk(chunk, query: str, target_table: str, mysql = None, pg = None):
        if not mysql:
            mysql = MySqlHook(mysql_conn_id='mysql_source')
        if not pg:
            pg = PostgresHook(postgres_conn_id='postgres_target')
        for df in fetch_in_chunks_cursor(mysql, query, chunk):
            insert_in_batches(pg, target_table, df)
        return f"Inserted {query} rows for chunk {chunk['start_val']}-{chunk['end_val']}"

    @task()
    def update_last_loaded_ts(ts: str):
        Variable.set("last_loaded_ts", ts)

    # DAG flow
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    load_range = get_load_range()
    chunk_datetimes = generate_datetime_chunks.partial(type_="dynamic",chunk_size=CHUNK_SIZE).expand_kwargs(load_range)

    with TaskGroup("tg_items") as tg_items:
        process_items = process_chunk.partial(
            query="""
                SELECT * 
                FROM salesorderitem 
                WHERE modified_at >= %s 
                AND modified_at < %s
                """,
            target_table="lz.salesorderitem",
        ).expand(chunk=chunk_datetimes)

    with TaskGroup("tg_orders") as tg_orders:
        process_items = process_chunk.partial(
            query="""
                SELECT * 
                FROM salesorder 
                WHERE modified_at >= %s 
                AND modified_at < %s
                """,
            target_table="lz.salesorder",
        ).expand(chunk=chunk_datetimes)

    with TaskGroup("tg_customers") as tg_customers:
        mysql = create_temporary_table_customer.expand_kwargs(load_range)
        chunk_customers = generate_num_chunks.override(task_id="chunks_customers")(
            query="""
                SELECT MIN(id) AS min_id, MAX(id) AS max_id
                FROM temp_customer 
                WHERE id >= %s 
                AND id < %s
                """, 
            no_of_chunks=4, min_chunk_size = CHUNK_SIZE, 
            params = tuple(load_range.values()), mysql = mysql)
        process_customers = process_chunk.partial(
            query="""
                SELECT * 
                FROM temp_customer 
                WHERE id >= %s 
                AND id < %s
                """,
            target_table="lz.customer",
        ).expand(chunk=chunk_customers)
    
    update_last_loaded = update_last_loaded_ts.expand(ts=[load_range["end_time"]])

    start >> load_range >> chunk_datetimes >> [tg_items, tg_orders, tg_customers] >> update_last_loaded >> end
