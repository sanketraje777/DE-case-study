from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from common.tasks import get_load_range, generate_limit_offset_task, \
    process_data, update_last_loaded_ts, zip_dicts, get_variables
from common.config import DATETIME_INTERVAL, POSTGRES_CONN, \
    CORE_PARALLELISM, DAG_CONCURRENCY, DEFAULT_VAR_DICT, \
    DEFAULT_SOURCE_CONFIG, DEFAULT_LZ_CONFIG
from datetime import timedelta

DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': days_ago(DATETIME_INTERVAL),
    'retries': 0,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='etl_landing',
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily',
    max_active_runs=1,
    concurrency=DAG_CONCURRENCY,
    catchup=False,
    tags=['etl', 'landing'],
) as dag:
    variables = get_variables.override(task_id="get_airflow_variables")(
        keys=["chunk_size","page_size","batch_size"])
    db_pool = "db_pool_max"
    load_range = get_load_range()
    
    target = Variable.get("target_config", deserialize_json=True, default_var=DEFAULT_LZ_CONFIG)
    source = Variable.get("source_config", deserialize_json=True, default_var=DEFAULT_SOURCE_CONFIG)
    target['tables'] = [table.strip() for table in target['tables'].split(",")]
    source['tables'] = [table.strip() for table in source['tables'].split(",")]
    
    init_lz = EmptyOperator(task_id=f"init_{target['schema']}")
    end_lz = EmptyOperator(task_id=f"end_{target['schema']}")

    # processing ounass_source.salesorderitem -> lz.salesorderitem
    target_table = target['tables'][0]
    source_table = source['tables'][0]
    with TaskGroup(f"tg_{target_table}") as tg_salesorderitem:
        truncate_target_items = PostgresOperator(
            task_id=f'truncate_target_{target_table}_and_drop_indexes',
            postgres_conn_id=POSTGRES_CONN["conn_id"],
            sql=[f'TRUNCATE TABLE {target['schema']}.{target_table} RESTART IDENTITY CASCADE;',
                f"SELECT drop_special_constraints_and_indexes('{target['schema']}', '{target_table}', ARRAY[]::text[]);"]
        )
        chunk_items = generate_limit_offset_task.override(task_id=f"chunks_{target_table}")(
            query=f"""
                SELECT COUNT(*) AS count
                FROM (
                    SELECT DISTINCT si.* 
                    FROM {source['schema']}.{source_table} si      -- ounass_source.salesorderitem
                    JOIN salesorder s 
                    ON s.id = si.order_id 
                    WHERE (s.modified_at >= %(start_time)s AND s.modified_at < %(end_time)s) 
                    OR (si.modified_at >= %(start_time)s AND si.modified_at < %(end_time)s)
                ) AS distinct_items
                """, 
                    type_="dynamic", parameters = load_range)
        items_parameters = zip_dicts.override(task_id=f"prepare_{target_table}_parameters")(
            DEFAULT_VAR_DICT, variables, load_range, chunk_items, 
            prefixes = ["var_", "var_", "load_range_", f"{target_table}_"]
        )
        process_items = process_data.override(
            task_id=f"process_{source_table}_chunks",
            pool=db_pool, max_active_tis_per_dag=CORE_PARALLELISM).partial(
            query=f"""
                SELECT DISTINCT si.* 
                FROM {source['schema']}.{source_table} si      -- ounass_source.salesorderitem
                JOIN salesorder s 
                ON s.id = si.order_id 
                WHERE (s.modified_at >= %(load_range_start_time)s AND s.modified_at < %(load_range_end_time)s) 
                OR (si.modified_at >= %(load_range_start_time)s AND si.modified_at < %(load_range_end_time)s)
                ORDER BY si.item_id, si.order_id
                LIMIT %({target_table}_limit)s OFFSET %({target_table}_offset)s
                """,
            target_table=f"{target['schema']}.{target_table}"
        ).expand(parameters = items_parameters)
        add_target_items_indexes = PostgresOperator(
            task_id=f'add_target_{target_table}_indexes',
            postgres_conn_id=POSTGRES_CONN["conn_id"],
            sql=f"""
                CREATE INDEX IF NOT EXISTS 
                    idx_{target_table}_item_id_modified_at 
                        ON {target['schema']}.{target_table} (item_id, modified_at DESC);
                CREATE INDEX IF NOT EXISTS 
                    idx_{target_table}_product_id_modified_at 
                        ON {target['schema']}.{target_table} (product_id, modified_at DESC);
                CREATE INDEX IF NOT EXISTS 
                    idx_{target_table}_product_sku_modified_at 
                        ON {target['schema']}.{target_table} (product_sku, modified_at DESC);
                """
        )
        [variables, load_range, chunk_items] >> items_parameters
        [items_parameters, truncate_target_items] >> process_items >> add_target_items_indexes

    # processing ounass_source.salesorder -> lz.salesorder
    target_table = target['tables'][1]
    source_table = source['tables'][1]
    with TaskGroup(f"tg_{target_table}") as tg_salesorder:
        truncate_target_orders = PostgresOperator(
            task_id=f'truncate_target_{target_table}_and_drop_indexes',
            postgres_conn_id=POSTGRES_CONN["conn_id"],
            sql=[f'TRUNCATE TABLE {target['schema']}.{target_table} RESTART IDENTITY CASCADE;',
                f"SELECT drop_special_constraints_and_indexes('{target['schema']}', '{target_table}', ARRAY[]::text[]);"]
        )
        chunk_orders = generate_limit_offset_task.override(task_id=f"chunks_{target_table}")(
            query=f"""
                SELECT COUNT(*) AS count
                FROM (
                    SELECT DISTINCT s.* 
                    FROM {source['schema']}.{source_table} s       -- ounass_source.salesorder
                    JOIN salesorderitem si 
                    ON si.order_id = s.id 
                    WHERE (si.modified_at >= %(start_time)s AND si.modified_at < %(end_time)s) 
                    OR (s.modified_at >= %(start_time)s AND s.modified_at < %(end_time)s)
                ) AS distinct_orders
                """,
            type_="dynamic", parameters=load_range)
        orders_parameters = zip_dicts.override(task_id=f"prepare_{target_table}_parameters")(
            DEFAULT_VAR_DICT, variables, load_range, chunk_orders, 
            prefixes = ["var_", "var_", "load_range_", f"{target_table}_"]
        )
        process_orders = process_data.override(
            task_id=f"process_{source_table}_chunks",
            pool=db_pool, max_active_tis_per_dag=CORE_PARALLELISM).partial(
            query=f"""
                SELECT DISTINCT s.* 
                FROM {source['schema']}.{source_table} s       -- ounass_source.salesorder 
                JOIN salesorderitem si 
                ON si.order_id = s.id 
                WHERE (si.modified_at >= %(load_range_start_time)s AND si.modified_at < %(load_range_end_time)s) 
                OR (s.modified_at >= %(load_range_start_time)s AND s.modified_at < %(load_range_end_time)s)
                ORDER BY s.id
                LIMIT %({target_table}_limit)s OFFSET %({target_table}_offset)s
                """,
            target_table=f"{target['schema']}.{target_table}"
        ).expand(parameters = orders_parameters)
        add_target_orders_indexes = PostgresOperator(
            task_id=f'add_target_{target_table}_indexes',
            postgres_conn_id=POSTGRES_CONN["conn_id"],
            sql=f"""
                CREATE INDEX IF NOT EXISTS 
                    idx_{target_table}_id_modified_at 
                        ON {target['schema']}.{target_table} (id, modified_at DESC);
                CREATE INDEX IF NOT EXISTS 
                    idx_{target_table}_order_number_modified_at 
                        ON {target['schema']}.{target_table} (order_number, modified_at DESC);
                """
        )
        [variables, load_range, chunk_orders] >> orders_parameters
        [orders_parameters, truncate_target_orders] >> process_orders >> add_target_orders_indexes

    # processing ounass_source.customer -> lz.customer
    target_table = target['tables'][2]
    source_table = source['tables'][2]
    with TaskGroup(f"tg_{target_table}") as tg_customer:
        chunk_customers = generate_limit_offset_task.override(task_id=f"chunks_{target_table}")(
            query=f"""
                SELECT COUNT(*) AS count
                FROM (
                    SELECT DISTINCT c.* 
                    FROM {source['schema']}.{source_table} c       -- ounass_source.customer 
                    JOIN salesorder s 
                    ON s.customer_id = c.id 
                    JOIN salesorderitem si 
                    ON si.order_id = s.id 
                    WHERE (s.modified_at >= %(start_time)s AND s.modified_at < %(end_time)s)
                    OR (si.modified_at >= %(start_time)s AND si.modified_at < %(end_time)s)
                ) AS distinct_customers
                """, 
            type_="dynamic", parameters=load_range)
        truncate_target_customer = PostgresOperator(
            task_id=f'truncate_target_{target_table}_and_drop_indexes',
            postgres_conn_id=POSTGRES_CONN["conn_id"],
            sql=[f'TRUNCATE TABLE {target['schema']}.{target_table} RESTART IDENTITY CASCADE;',
                f"SELECT drop_special_constraints_and_indexes('{target['schema']}', '{target_table}', ARRAY[]::text[]);"]
        )
        customers_parameters = zip_dicts.override(task_id=f"prepare_{target_table}_parameters")(
            DEFAULT_VAR_DICT, variables, load_range, chunk_customers, 
            prefixes = ["var_", "var_", "load_range_", f"{target_table}_"]
        )
        process_customers = process_data.override(
            task_id=f"process_{source_table}_chunks",
            pool=db_pool, max_active_tis_per_dag=CORE_PARALLELISM).partial(
            query=f"""
                SELECT DISTINCT c.* 
                FROM {source['schema']}.{source_table} c       -- ounass_source.customer 
                JOIN salesorder s 
                ON s.customer_id = c.id 
                JOIN salesorderitem si 
                ON si.order_id = s.id 
                WHERE (s.modified_at >= %(load_range_start_time)s AND s.modified_at < %(load_range_end_time)s)
                OR (si.modified_at >= %(load_range_start_time)s AND si.modified_at < %(load_range_end_time)s)
                ORDER BY c.id
                LIMIT %({target_table}_limit)s OFFSET %({target_table}_offset)s
                """,
            target_table=f"{target['schema']}.{target_table}"
        ).expand(parameters = customers_parameters)
        add_target_customer_indexes = PostgresOperator(
            task_id=f'add_target_{target_table}_indexes',
            postgres_conn_id=POSTGRES_CONN["conn_id"],
            sql=f"""
                CREATE INDEX IF NOT EXISTS 
                    idx_{target_table}_id 
                        ON {target['schema']}.{target_table} (id);
                """
        )
        [variables, load_range, chunk_customers] >> customers_parameters
        [customers_parameters, truncate_target_customer] >> process_customers >> add_target_customer_indexes

    update_last_loaded = update_last_loaded_ts(load_range=load_range)

    # trigger the etl_ods dag
    trigger_etl_ods_dag = TriggerDagRunOperator(
        task_id='trigger_etl_ods_dag',
        trigger_dag_id='etl_ods'
    )

    init_lz >> load_range >> [tg_salesorderitem, tg_salesorder, tg_customer] >> \
        update_last_loaded >> end_lz >> trigger_etl_ods_dag
    init_lz >> variables >> [tg_salesorderitem, tg_salesorder, tg_customer]

# set trigger_rule for all tasks
for task in dag.tasks:
    task.trigger_rule = "none_failed"
