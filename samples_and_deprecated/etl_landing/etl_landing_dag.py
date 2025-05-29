from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['dataops@company.com'],
}

dag = DAG(
    dag_id='incremental_mysql_to_postgres_optimized',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False
)

# Helper Functions
def get_date_range(**context):
    current_ts = datetime.now()
    testing_ts = Variable.get("testing_ts", default_var="false").lower() == "true"
    last_ts = Variable.get("last_loaded_ts", default_var=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S'))
    if testing_ts:
        current_ts = datetime.strptime(last_ts, '%Y-%m-%d %H:%M:%S') + timedelta(days=1)
    context['ti'].xcom_push(key='last_loaded_ts', value=last_ts)
    context['ti'].xcom_push(key='current_ts', value=current_ts.strftime('%Y-%m-%d %H:%M:%S'))

def extract_transform_load(**context):
    last_ts = context['ti'].xcom_pull(key='last_loaded_ts')
    current_ts = context['ti'].xcom_pull(key='current_ts')

    mysql = MySqlHook(mysql_conn_id='mysql_source')
    pg_hook = PostgresHook(postgres_conn_id='postgres_target')

    # Extract salesorderitem
    item_query = f"""
        SELECT * FROM salesorderitem
        WHERE modified_at > '{last_ts}' AND modified_at <= '{current_ts}';
    """
    df_items = mysql.get_pandas_df(item_query)
    if df_items.empty:
        context['ti'].xcom_push(key='item_count', value=0)
        return

    context['ti'].xcom_push(key='item_count', value=len(df_items))

    order_ids = tuple(df_items['order_id'].unique()) or (0,)
    order_query = f"SELECT * FROM salesorder WHERE id IN {order_ids};"
    df_orders = mysql.get_pandas_df(order_query)

    customer_ids = tuple(df_orders['customer_id'].unique()) or (0,)
    customer_query = f"SELECT * FROM customer WHERE id IN {customer_ids};"
    df_customers = mysql.get_pandas_df(customer_query)

    # Load into PostgreSQL
    df_items.to_csv('/tmp/items.csv', index=False)
    df_orders.to_csv('/tmp/orders.csv', index=False)
    df_customers.to_csv('/tmp/customers.csv', index=False)

    with open('/tmp/items.csv', 'r') as f:
        pg_hook.copy_expert("COPY staging.salesorderitem FROM STDIN WITH CSV HEADER", f)
    with open('/tmp/orders.csv', 'r') as f:
        pg_hook.copy_expert("COPY staging.salesorder FROM STDIN WITH CSV HEADER", f)
    with open('/tmp/customers.csv', 'r') as f:
        pg_hook.copy_expert("COPY staging.customer FROM STDIN WITH CSV HEADER", f)

def should_continue(**context):
    count = context['ti'].xcom_pull(key='item_count', task_ids='etl_task')
    return 'update_last_loaded_ts' if count > 0 else 'skip_load'

def update_timestamp(**context):
    current_ts = context['ti'].xcom_pull(key='current_ts')
    Variable.set("last_loaded_ts", current_ts)

# Tasks
get_range = PythonOperator(
    task_id='get_date_range',
    python_callable=get_date_range,
    provide_context=True,
    dag=dag
)

etl = PythonOperator(
    task_id='etl_task',
    python_callable=extract_transform_load,
    provide_context=True,
    dag=dag
)

branch = BranchPythonOperator(
    task_id='check_if_items_exist',
    python_callable=should_continue,
    provide_context=True,
    dag=dag
)

skip = EmptyOperator(task_id='skip_load', dag=dag)

update_ts = PythonOperator(
    task_id='update_last_loaded_ts',
    python_callable=update_timestamp,
    provide_context=True,
    dag=dag
)

# Task Flow
get_range >> etl >> branch
branch >> skip
branch >> update_ts
