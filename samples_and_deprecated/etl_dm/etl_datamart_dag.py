from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import logging

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def build_datamart(**ctx):
    pg = PostgresHook(postgres_conn_id='postgres_default')
    # Read ODS tables
    df_c = pg.get_pandas_df('SELECT id AS customer_id, first_name, last_name, gender, email FROM ods.customer;')
    df_o = pg.get_pandas_df('SELECT * FROM ods.salesorder;')
    df_i = pg.get_pandas_df('SELECT * FROM ods.salesorderitem;')

    df = df_i.merge(df_o, left_on='order_id', right_on='id', suffixes=('_item','_order'))
    df = df.merge(df_c, left_on='customer_id_order', right_on='customer_id')

    df_final = pd.DataFrame({
        'item_id': df['item_id'],
        'order_id': df['order_id'],
        'order_number': df['order_number'],
        'order_created_at': df['created_at_order'],
        'order_total': df['order_total'],
        'total_qty_ordered': df['total_qty_ordered'],
        'customer_id': df['customer_id'],
        'customer_name': df['first_name'] + ' ' + df['last_name'],
        'customer_gender': df['gender'],
        'customer_email': df['email'],
        'product_id': df['product_id'],
        'product_sku': df['product_sku'],
        'product_name': df['product_name'],
        'item_price': df['price'],
        'item_qty_order': df['qty_ordered'],
        'item_unit_total': df['line_total']
    })
    logger.info(f"Inserting {len(df_final)} records into dw.sales_order_item_flat")
    # Load into DW
    pg.insert_rows('dw.sales_order_item_flat', df_final.values.tolist(), list(df_final.columns), commit_every=1000)

with DAG(
    'etl_datamart',
    default_args=DEFAULT_ARGS,
    description='Populate data mart table',
    schedule_interval='@daily',
    start_date=datetime(2025,5,20),
    catchup=False
) as dag:
    task = PythonOperator(
        task_id='build_datamart',
        python_callable=build_datamart
    )
