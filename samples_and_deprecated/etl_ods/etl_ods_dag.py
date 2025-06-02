from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import pandas as pd
import re
import logging

DEFAULT_ARGS = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

logger = logging.getLogger(__name__)
EMAIL_REGEX = r'^[\w\.-]+@[\w\.-]+\.\w+$'

def clean_and_load(**ctx):
    pg = PostgresHook(postgres_conn_id='postgres_default')
    # Clean customer
    df_cust = pg.get_pandas_df('SELECT * FROM lz.customer;')
    df_cust = df_cust[df_cust['email'].str.match(EMAIL_REGEX)]
    logger.info(f"Cleaned customer records: {len(df_cust)}")
    # Load
    pg.insert_rows('ods.customer', df_cust.values.tolist(), list(df_cust.columns), commit_every=500)

    # Clean salesorder
    df_ord = pg.get_pandas_df('SELECT * FROM lz.salesorder;')
    df_ord = df_ord.dropna(subset=['order_total','total_qty_ordered'])
    logger.info(f"Cleaned salesorder records: {len(df_ord)}")
    pg.insert_rows('ods.salesorder', df_ord.values.tolist(), list(df_ord.columns), commit_every=500)

    # Clean salesorderitem
    df_item = pg.get_pandas_df('SELECT * FROM lz.salesorderitem;')
    df_item = df_item.dropna(subset=['price','qty_ordered'])
    logger.info(f"Cleaned salesorderitem records: {len(df_item)}")
    pg.insert_rows('ods.salesorderitem', df_item.values.tolist(), list(df_item.columns), commit_every=500)

with DAG(
    'etl_ods_old',
    default_args=DEFAULT_ARGS,
    description='Clean and normalize into ODS',
    schedule_interval='@daily',
    start_date=datetime(2025,5,20),
    catchup=False
) as dag:
    clean = PythonOperator(
        task_id='clean_and_load',
        python_callable=clean_and_load
    )

    trigger_dm = TriggerDagRunOperator(
        task_id='trigger_datamart_dag',
        trigger_dag_id='etl_datamart'
    )

    clean >> trigger_dm
