from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from datetime import timedelta

CHUNK_SIZE = 10000

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='etl_landing_dynamic_chunks_full',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    @task()
    def generate_chunks(table_name: str, id_col: str, date_filter_col: str, start_date: str, end_date: str):
        """
        Generate chunks of IDs for given table based on min/max id filtered by date.
        """
        mysql = MySqlHook(mysql_conn_id='mysql_source')
        query = f"""
            SELECT MIN({id_col}) as min_id, MAX({id_col}) as max_id FROM {table_name}
            WHERE {date_filter_col} > '{start_date}' AND {date_filter_col} <= '{end_date}';
        """
        min_max = mysql.get_first(query)
        min_id, max_id = min_max
        chunks = []
        if min_id is None or max_id is None:
            return chunks
        for start in range(min_id, max_id + 1, CHUNK_SIZE):
            end = min(start + CHUNK_SIZE - 1, max_id)
            chunks.append({"start_id": start, "end_id": end})
        return chunks

    @task()
    def process_salesorderitem_chunk(chunk, start_date: str, end_date: str):
        start_id = chunk['start_id']
        end_id = chunk['end_id']

        mysql = MySqlHook(mysql_conn_id='mysql_source')
        pg = PostgresHook(postgres_conn_id='postgres_target')

        df = mysql.get_pandas_df(f"""
            SELECT * FROM salesorderitem
            WHERE modified_at > '{start_date}'
              AND modified_at <= '{end_date}'
              AND id BETWEEN {start_id} AND {end_id};
        """)

        if df.empty:
            return f"No salesorderitem data for chunk {start_id}-{end_id}"

        try:
            pg.insert_rows(
                'lz.salesorderitem',
                df.values.tolist(),
                target_fields=df.columns.tolist(),
                commit_every=1000
            )
        except Exception as e:
            raise RuntimeError(f"Failed salesorderitem insert chunk {start_id}-{end_id}: {e}")

        return f"Inserted {len(df)} salesorderitem rows for chunk {start_id}-{end_id}"

    @task()
    def process_salesorder_chunk(chunk, start_date: str, end_date: str):
        start_id = chunk['start_id']
        end_id = chunk['end_id']

        mysql = MySqlHook(mysql_conn_id='mysql_source')
        pg = PostgresHook(postgres_conn_id='postgres_target')

        df = mysql.get_pandas_df(f"""
            SELECT * FROM salesorder
            WHERE modified_at > '{start_date}'
              AND modified_at <= '{end_date}'
              AND id BETWEEN {start_id} AND {end_id};
        """)

        if df.empty:
            return f"No salesorder data for chunk {start_id}-{end_id}"

        try:
            pg.insert_rows(
                'lz.salesorder',
                df.values.tolist(),
                target_fields=df.columns.tolist(),
                commit_every=1000
            )
        except Exception as e:
            raise RuntimeError(f"Failed salesorder insert chunk {start_id}-{end_id}: {e}")

        return f"Inserted {len(df)} salesorder rows for chunk {start_id}-{end_id}"

    @task()
    def process_customer_chunk(chunk, start_date: str, end_date: str):
        start_id = chunk['start_id']
        end_id = chunk['end_id']

        mysql = MySqlHook(mysql_conn_id='mysql_source')
        pg = PostgresHook(postgres_conn_id='postgres_target')

        df = mysql.get_pandas_df(f"""
            SELECT * FROM customer
            WHERE id BETWEEN {start_id} AND {end_id}
              AND id IN (
                SELECT DISTINCT customer_id FROM salesorder
                WHERE modified_at > '{start_date}'
                  AND modified_at <= '{end_date}'
              );
        """)

        if df.empty:
            return f"No customer data for chunk {start_id}-{end_id}"

        try:
            pg.insert_rows(
                'lz.customer',
                df.values.tolist(),
                target_fields=df.columns.tolist(),
                commit_every=1000
            )
        except Exception as e:
            raise RuntimeError(f"Failed customer insert chunk {start_id}-{end_id}: {e}")

        return f"Inserted {len(df)} customer rows for chunk {start_id}-{end_id}"

    # Define your date range here or make it a DAG param
    start_date = '2021-07-06 00:00:00'
    end_date = '2021-07-07 23:59:59'

    # Generate chunks per table
    chunks_items = generate_chunks('salesorderitem', 'id', 'modified_at', start_date, end_date)
    chunks_orders = generate_chunks('salesorder', 'id', 'modified_at', start_date, end_date)
    chunks_customers = generate_chunks('customer', 'id', 'id', start_date, end_date)  # Assuming customer.id is primary key

    with TaskGroup("salesorderitem_tasks") as tg_items:
        process_salesorderitem_chunk.expand(chunk=chunks_items, start_date=[start_date]*len(chunks_items), end_date=[end_date]*len(chunks_items))

    with TaskGroup("salesorder_tasks") as tg_orders:
        process_salesorder_chunk.expand(chunk=chunks_orders, start_date=[start_date]*len(chunks_orders), end_date=[end_date]*len(chunks_orders))

    with TaskGroup("customer_tasks") as tg_customers:
        process_customer_chunk.expand(chunk=chunks_customers, start_date=[start_date]*len(chunks_customers), end_date=[end_date]*len(chunks_customers))

    chunks_items >> tg_items >> chunks_orders >> tg_orders >> chunks_customers >> tg_customers
