## Repository Structure
```
├── sql
│   ├── ddl_source.sql
│   └── ddl_dw.sql
├── dags
│   ├── etl_landing_dag.py
│   ├── etl_ods_dag.py
│   └── etl_datamart_dag.py
├── tests
│   └── test_dags.py
└── README.md
```

---

### sql/ddl_source.sql
```sql
-- Create source MySQL schema and tables
CREATE DATABASE IF NOT EXISTS ounass_source;
USE ounass_source;

DROP TABLE IF EXISTS customer;
CREATE TABLE customer (
  id INT PRIMARY KEY,
  first_name VARCHAR(100) NOT NULL,
  last_name VARCHAR(100) NOT NULL,
  email VARCHAR(255) NOT NULL,
  gender ENUM('Female','Male') NOT NULL,
  billing_address TEXT,
  shipping_address TEXT NOT NULL
);

DROP TABLE IF EXISTS salesorder;
CREATE TABLE salesorder (
  id INT PRIMARY KEY,
  customer_id INT NOT NULL,
  order_number VARCHAR(50) UNIQUE NOT NULL,
  created_at DATETIME NOT NULL,
  modified_at DATETIME NOT NULL,
  order_total DECIMAL(10,2) NOT NULL,
  total_qty_ordered INT NOT NULL,
  FOREIGN KEY (customer_id) REFERENCES customer(id)
);

DROP TABLE IF EXISTS salesorderitem;
CREATE TABLE salesorderitem (
  item_id INT PRIMARY KEY,
  order_id INT NOT NULL,
  product_id INT NOT NULL,
  product_sku VARCHAR(50) NOT NULL,
  product_name VARCHAR(255),
  qty_ordered INT NOT NULL,
  price DECIMAL(10,2) NOT NULL,
  line_total DECIMAL(12,2) NOT NULL,
  created_at DATETIME NOT NULL,
  modified_at DATETIME NOT NULL,
  FOREIGN KEY (order_id) REFERENCES salesorder(id)
);
```

### sql/ddl_dw.sql
```sql
-- Create destination PostgreSQL schema and table
CREATE SCHEMA IF NOT EXISTS dw;

DROP TABLE IF EXISTS dw.sales_order_item_flat;
CREATE TABLE dw.sales_order_item_flat (
  item_id INT NOT NULL,
  order_id INT NOT NULL,
  order_number VARCHAR(50) NOT NULL,
  order_created_at TIMESTAMP NOT NULL,
  order_total DOUBLE PRECISION NOT NULL,
  total_qty_ordered INT NOT NULL,
  customer_id INT NOT NULL,
  customer_name VARCHAR(200) NOT NULL,
  customer_gender VARCHAR(10) NOT NULL,
  customer_email VARCHAR(255) NOT NULL,
  product_id INT NOT NULL,
  product_sku VARCHAR(50) NOT NULL,
  product_name VARCHAR(255),
  item_price DOUBLE PRECISION NOT NULL,
  item_qty_order INT NOT NULL,
  item_unit_total DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (order_id, item_id)
);
```

---

## DAGS
All DAGs assume Airflow connections:
- `mysql_default` for MySQL source
- `postgres_default` for Postgres warehouse

### dags/etl_landing_dag.py
```python
from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

DEFAULT_ARGS = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def extract_and_load(**ctx):
    mysql = MySqlHook(mysql_conn_id='mysql_default')
    pg = PostgresHook(postgres_conn_id='postgres_default')

    # Tables to copy
    tables = ['customer', 'salesorder', 'salesorderitem']
    for table in tables:
        df = mysql.get_pandas_df(f"SELECT * FROM ounass_source.{table} WHERE DATE(created_at) <= CURRENT_DATE() - INTERVAL 1 DAY;")
        # Write to landing zone
        pg.insert_rows(table=f'lz_{table}', rows=df.values.tolist(), target_fields=list(df.columns), commit_every=1000)

with DAG(
    'etl_landing',
    default_args=DEFAULT_ARGS,
    description='Load raw data into landing zone',
    schedule_interval='@daily',
    start_date=datetime(2025,5,20),
    catchup=False
) as dag:
    task = PythonOperator(
        task_id='extract_and_load',
        python_callable=extract_and_load
    )
```

### dags/etl_ods_dag.py
```python
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import re

DEFAULT_ARGS = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

EMAIL_REGEX = r'^[\w\.-]+@[\w\.-]+\.\w+$'

def clean_and_load(**ctx):
    pg = PostgresHook(postgres_conn_id='postgres_default')
    # Clean customer
    df_cust = pg.get_pandas_df('SELECT * FROM lz_customer;')
    df_cust = df_cust[df_cust['email'].str.match(EMAIL_REGEX)]
    # Load
    pg.insert_rows('ods_customer', df_cust.values.tolist(), list(df_cust.columns), commit_every=500)

    # Clean salesorder
    df_ord = pg.get_pandas_df('SELECT * FROM lz_salesorder;')
    df_ord = df_ord.dropna(subset=['order_total','total_qty_ordered'])
    pg.insert_rows('ods_salesorder', df_ord.values.tolist(), list(df_ord.columns), commit_every=500)

    # Clean salesorderitem
    df_item = pg.get_pandas_df('SELECT * FROM lz_salesorderitem;')
    df_item = df_item.dropna(subset=['price','qty_ordered'])
    pg.insert_rows('ods_salesorderitem', df_item.values.tolist(), list(df_item.columns), commit_every=500)

with DAG(
    'etl_ods',
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
```

### dags/etl_datamart_dag.py
```python
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

DEFAULT_ARGS = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def build_datamart(**ctx):
    pg = PostgresHook(postgres_conn_id='postgres_default')
    # Read ODS tables
    df_c = pg.get_pandas_df('SELECT id AS customer_id, first_name, last_name, gender, email FROM ods_customer;')
    df_o = pg.get_pandas_df('SELECT * FROM ods_salesorder;')
    df_i = pg.get_pandas_df('SELECT * FROM ods_salesorderitem;')

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
```

---

### tests/test_dags.py
```python
import pytest
from airflow.models import DagBag

@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="dags", include_examples=False)

@ pytest.mark.parametrize("dag_id", ["etl_landing", "etl_ods", "etl_datamart"])
def test_dag_loaded(dagbag, dag_id):
    dag = dagbag.get_dag(dag_id)
    assert dag is not None, f"DAG {dag_id} failed to load"
```

---

### README.md
```md
# Ounass Data Engineering Case Study

## Setup
1. Create Docker network: `docker network create mysql_default`
2. `docker-compose up -d`
3. Load source SQL: `mysql -h localhost -P 3306 -u root -p < sql/ddl_source.sql`
4. Load DW SQL: `psql -h localhost -U postgres -d postgres -f sql/ddl_dw.sql`

## Airflow
- Place DAGs in `dags/`
- Webserver at `localhost:8080`
- Ensure connections `mysql_default` & `postgres_default` are configured

## Testing
```
pytest tests/
```
