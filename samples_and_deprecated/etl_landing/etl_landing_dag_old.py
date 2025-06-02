from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
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

# Helper Functions
def get_date_range(**context):
    current_ts = datetime.now()
    testing_ts = Variable.get("testing_ts", default_var="false").lower() == "true"
    last_ts = Variable.get("last_loaded_ts", default_var=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S'))
    if testing_ts:
        current_ts = datetime.strptime(last_ts, '%Y-%m-%d %H:%M:%S') + timedelta(days=1)
    context['ti'].xcom_push(key='last_loaded_ts', value=last_ts)
    context['ti'].xcom_push(key='current_ts', value=current_ts.strftime('%Y-%m-%d %H:%M:%S'))

def extract_and_load(**ctx):
    mysql = MySqlHook(mysql_conn_id='mysql_default')
    pg = PostgresHook(postgres_conn_id='postgres_default')

    last_ts = ctx['ti'].xcom_pull(key='last_loaded_ts')
    current_ts = ctx['ti'].xcom_pull(key='current_ts')

    # Configurable tables via Airflow Variable or fallback
    tables = Variable.get("source_tables", default_var="customer,salesorder,salesorderitem").split(',')

    for table in tables:
        query = f"SELECT * FROM ounass_source.{table} WHERE DATE(created_at) <= CURRENT_DATE() - INTERVAL 1 DAY;"
        df = mysql.get_pandas_df(query)
        logger.info(f"Inserting {len(df)} records into lz.{table}")
        # Write to landing zone
        pg.insert_rows(table=f'lz.{table}', rows=df.values.tolist(), target_fields=list(df.columns), commit_every=1000)

with DAG(
    'etl_landing',
    default_args=DEFAULT_ARGS,
    description='Load raw data into landing zone',
    schedule_interval='@daily',
    start_date=datetime(2025,5,20),
    catchup=False
) as dag:
    load_task = PythonOperator(
        task_id='extract_and_load',
        python_callable=extract_and_load
    )

    trigger_ods = TriggerDagRunOperator(
        task_id='trigger_ods_dag',
        trigger_dag_id='etl_ods'
    )

    load_task >> trigger_ods
