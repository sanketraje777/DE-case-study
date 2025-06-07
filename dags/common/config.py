from airflow.models import Variable
import os

CHUNK_SIZE = int(Variable.get("chunk_size", default_var=10000))
MIN_CHUNK_SIZE = int(Variable.get("min_chunk_size", default_var=1000))
MIN_CHUNK_MINUTES = int(Variable.get("min_chunk_minutes", default_var=6*60))
PAGE_SIZE = int(Variable.get("page_size", default_var=10000))
BATCH_SIZE = int(Variable.get("batch_size", default_var=1000))
DATETIME_INTERVAL = int(Variable.get("datetime_interval", default_var=1))
TESTING_TS = Variable.get("testing_ts", default_var="false").lower() == "true"
CORE_PARALLELISM = int(os.environ.get("AIRFLOW__CORE__PARALLELISM", 16))
DAG_CONCURRENCY = int(os.environ.get("AIRFLOW__CORE__DAG_CONCURRENCY", 16))
MYSQL_CONN = Variable.get("mysql_conn", deserialize_json=True, default_var={})
POSTGRES_CONN = Variable.get("postgres_conn", deserialize_json=True, default_var={})
MYSQL_CONN.setdefault("conn_id", "mysql_default")
POSTGRES_CONN.setdefault("conn_id", "postgres_default")
TEXT_CLEAN_REGEX = Variable.get("text_clean_regex", default_var="([[:cntrl:]]|Â| )+")
DEFAULT_VAR_DICT = {"chunk_size": CHUNK_SIZE, "page_size": PAGE_SIZE, 
                    "batch_size": BATCH_SIZE, "text_clean_regex": TEXT_CLEAN_REGEX}
DEFAULT_SOURCE_CONFIG = {
    "schema": "ounass_source",
    "tables": "salesorderitem,salesorder,customer",
    "sys_folder": "mysql_source"
  }
DEFAULT_LZ_CONFIG = {
    "schema": "lz",
    "tables": "salesorderitem,salesorder,customer",
    "sys_folder": "postgres_data_warehouse"
  }
DEFAULT_ODS_CONFIG = {
    "schema": "ods",
    "tables": "salesorderitem,salesorder,customer,product",
    "sys_folder": "postgres_data_warehouse"
  }
DEFAULT_DM_CONFIG = {
    "schema": "dm",
    "tables": "sales_order_item_flat",
    "sys_folder": "postgres_data_warehouse"
  }
