from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from common.tasks import get_load_range, generate_limit_offset_task, \
    process_chunk, update_last_loaded_ts, run_queries_with_task_params, \
        zip_dicts, get_variables
from common.tasks import DATETIME_INTERVAL, MYSQL_CONN, POSTGRES_CONN
from datetime import timedelta

DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': days_ago(DATETIME_INTERVAL),
    'retries': 0,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='etl_landing2',
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'landing'],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    variables = get_variables.override(task_id="get_airflow_variables")(keys=["chunk_size","batch_size"])
    load_range = get_load_range()
    target_schema = "lz"

    target_table = "salesorderitem"
    with TaskGroup(f"tg_{target_table}") as tg_items:
        truncate_target_items = PostgresOperator(
            task_id=f'truncate_target_{target_table}_and_drop_indexes',
            postgres_conn_id=POSTGRES_CONN["conn_id"],
            sql=[f'TRUNCATE TABLE {target_schema}.{target_table} RESTART IDENTITY CASCADE;',
                f"SELECT drop_special_constraints_and_indexes('{target_schema}', '{target_table}', ARRAY[]::text[]);"]
        )
        chunk_items = generate_limit_offset_task.override(task_id=f"chunks_{target_table}")(
            query=f"""
                SELECT COUNT(*) AS count
                FROM (
                    SELECT DISTINCT si.* 
                    FROM salesorderitem si 
                    JOIN salesorder s 
                    ON s.id = si.order_id 
                    WHERE (s.modified_at >= %(start_time)s AND s.modified_at < %(end_time)s) 
                    OR (si.modified_at >= %(start_time)s AND si.modified_at < %(start_time)s)
                ) AS distinct_items
                """, 
                    type_="dynamic", parameters = load_range)
        items_parameters = zip_dicts.override(task_id=f"prepare_{target_table}_parameters")(
            variables, load_range, chunk_items, prefixes = ["var_", "load_range_", f"{target_table}s_"]
        )
        process_items = process_chunk.partial(
            query=f"""
                SELECT DISTINCT si.* 
                FROM salesorderitem si 
                JOIN salesorder s 
                ON s.id = si.order_id 
                WHERE (s.modified_at >= %(load_range_start_time)s AND s.modified_at < %(load_range_end_time)s) 
                OR (si.modified_at >= %(load_range_start_time)s AND si.modified_at < %(load_range_end_time)s)
                ORDER BY si.item_id, si.order_id
                LIMIT %({target_table}s_limit)s OFFSET %({target_table}s_offset)s
                """,
            target_table=f"{target_schema}.{target_table}"
        ).expand(parameters = items_parameters)
        add_target_items_indexes = PostgresOperator(
            task_id=f'add_target_{target_table}_indexes',
            postgres_conn_id=POSTGRES_CONN["conn_id"],
            sql=f"""
                CREATE INDEX IF NOT EXISTS 
                    idx_{target_table}_item_id ON {target_schema}.{target_table} (item_id);
                CREATE INDEX IF NOT EXISTS 
                    idx_{target_table}_order_id ON {target_schema}.{target_table} (order_id);
                CREATE INDEX IF NOT EXISTS 
                    idx_{target_table}_product_id ON {target_schema}.{target_table} (product_id);
                CREATE INDEX IF NOT EXISTS 
                    idx_{target_table}_modified_at ON {target_schema}.{target_table} (modified_at);
                CREATE INDEX IF NOT EXISTS 
                    idx_{target_table}_product_id_modified_at ON {target_schema}.{target_table} (product_id, modified_at DESC);
                """
        )
        [variables, load_range, chunk_items] >> items_parameters
        [items_parameters, truncate_target_items] >> process_items >> add_target_items_indexes

    target_table = "salesorder"
    with TaskGroup(f"tg_{target_table}") as tg_orders:
        truncate_target_orders = PostgresOperator(
            task_id=f'truncate_target_{target_table}_and_drop_indexes',
            postgres_conn_id=POSTGRES_CONN["conn_id"],
            sql=[f'TRUNCATE TABLE {target_schema}.{target_table} RESTART IDENTITY CASCADE;',
                f"SELECT drop_special_constraints_and_indexes('{target_schema}', '{target_table}', ARRAY[]::text[]);"]
        )
        chunk_orders = generate_limit_offset_task.override(task_id=f"chunks_{target_table}")(
            query=f"""
                SELECT COUNT(*) AS count
                FROM (
                    SELECT DISTINCT s.* 
                    FROM salesorder s 
                    JOIN salesorderitem si 
                    ON si.order_id = s.id 
                    WHERE (si.modified_at >= %(start_time)s AND si.modified_at < %(end_time)s) 
                    OR (s.modified_at >= %(start_time)s AND s.modified_at < %(end_time)s)
                ) AS distinct_orders
                """,
            type_="dynamic", parameters=load_range)
        orders_parameters = zip_dicts.override(task_id=f"prepare_{target_table}_parameters")(
            variables, load_range, chunk_orders, prefixes = ["var_", "load_range_", f"{target_table}s_"]
        )
        process_orders = process_chunk.partial(
            query=f"""
                SELECT DISTINCT s.* 
                FROM salesorder s 
                JOIN salesorderitem si 
                ON si.order_id = s.id 
                WHERE (si.modified_at >= %(load_range_start_time)s AND si.modified_at < %(load_range_start_time)s) 
                OR (s.modified_at >= %(load_range_start_time)s AND s.modified_at < %(load_range_start_time)s)
                ORDER BY s.id
                LIMIT %({target_table}s_limit)s OFFSET %({target_table}s_offset)s
                """,
            target_table=f"{target_schema}.{target_table}",
        ).expand(parameters = orders_parameters)
        add_target_orders_indexes = PostgresOperator(
            task_id=f'add_target_{target_table}_indexes',
            postgres_conn_id=POSTGRES_CONN["conn_id"],
            sql=f"""
                CREATE INDEX IF NOT EXISTS 
                    idx_{target_table}_id ON {target_schema}.{target_table} (id);
                CREATE INDEX IF NOT EXISTS 
                    idx_{target_table}_modified_at ON {target_schema}.{target_table} (modified_at);
                """
        )
        [variables, load_range, chunk_orders] >> orders_parameters
        [orders_parameters, truncate_target_orders] >> process_orders >> add_target_orders_indexes

    # target_table = "customer"
    # with TaskGroup(f"tg_{target_table}") as tg_customers2:
    #     staging_table_name = "staging_customer"
    #     create_staging_customer = MySqlOperator(
    #         task_id=f'create_staging_{target_table}',
    #         mysql_conn_id=MYSQL_CONN["conn_id"],
    #         sql=[
    #             f'DROP TABLE IF EXISTS `{staging_table_name}`;',
    #             f"""
    #             CREATE TABLE `{staging_table_name}` AS
    #             SELECT *
    #             FROM customer c
    #             WHERE EXISTS
    #                 (SELECT 1
    #                 FROM
    #                 (SELECT s.customer_id 
    #                 FROM salesorder s
    #                 WHERE EXISTS
    #                     (SELECT 1
    #                     FROM
    #                         (SELECT order_id FROM salesorderitem WHERE modified_at >= %(start_time)s AND modified_at < %(end_time)s
    #                         UNION
    #                         SELECT id AS order_id FROM salesorder WHERE modified_at >= %(start_time)s AND modified_at < %(end_time)s)
    #                     AS ssi
    #                     WHERE ssi.order_id = s.id))
    #                 AS sc
    #                 WHERE sc.customer_id = c.id);
    #             """,
    #             f'ALTER TABLE `{staging_table_name}` ADD PRIMARY KEY (id);'
    #         ],
    #         parameters = load_range
    #     )
    #     """
    #     Can also use alternate query:
    #     SELECT DISTINCT c.*
    #     FROM customer c
    #     JOIN salesorder s
    #     ON s.customer_id = c.id
    #     JOIN salesorderitem si
    #     ON si.order_id = s.id
    #     WHERE 
    #     (si.modified_at >= "{partial_params["start_time"]}" AND si.modified_at < "{partial_params["end_time"]}"
    #     OR s.modified_at >= "{partial_params["start_time"]}" AND s.modified_at < "{partial_params["end_time"]}")
    #     """
    #     chunk_customers = generate_limit_offset_task.override(task_id=f"chunks_{target_table}")(
    #         query=f"""
    #             SELECT COUNT(*) AS count 
    #             FROM `{staging_table_name}`
    #             """, 
    #         type_="dynamic")
    #     truncate_target_customer = PostgresOperator(
    #         task_id=f'truncate_target_{target_table}_and_drop_indexes',
    #         postgres_conn_id=POSTGRES_CONN["conn_id"],
    #         sql=[f'TRUNCATE TABLE {target_schema}.{target_table} RESTART IDENTITY CASCADE;',
    #             f"SELECT drop_special_constraints_and_indexes('{target_schema}', '{target_table}', ARRAY[]::text[]);"]
    #     )
    #     process_customers = process_chunk.partial(
    #         query=f"""
    #             SELECT * 
    #             FROM `{staging_table_name}` 
    #             ORDER BY id
    #             LIMIT %(limit)s OFFSET %(offset)s
    #             """,
    #         target_table=f"{target_schema}.{target_table}"
    #     ).expand(parameters=chunk_customers)
    #     drop_staging_customer = MySqlOperator(
    #         task_id=f'drop_staging_{target_table}',
    #         mysql_conn_id=MYSQL_CONN["conn_id"],
    #         sql=f'DROP TABLE IF EXISTS `{staging_table_name}`'
    #     )
    #     add_target_customer_indexes = PostgresOperator(
    #         task_id=f'add_target_{target_table}_indexes',
    #         postgres_conn_id=POSTGRES_CONN["conn_id"],
    #         sql=f"""
    #             CREATE INDEX IF NOT EXISTS 
    #                 idx_{target_table}_id ON {target_schema}.{target_table} (id);
    #             """
    #     )
    #     [create_staging_customer >> chunk_customers, truncate_target_customer] >> process_customers >> [drop_staging_customer, add_target_customer_indexes]

    target_table = "customer"
    with TaskGroup(f"tg_{target_table}") as tg_customers:
        chunk_customers = generate_limit_offset_task.override(task_id=f"chunks_{target_table}")(
            query=f"""
                SELECT COUNT(*) AS count
                FROM (
                    SELECT DISTINCT c.* 
                    FROM customer c 
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
            sql=[f'TRUNCATE TABLE {target_schema}.{target_table} RESTART IDENTITY CASCADE;',
                f"SELECT drop_special_constraints_and_indexes('{target_schema}', '{target_table}', ARRAY[]::text[]);"]
        )
        customers_parameters = zip_dicts.override(task_id=f"prepare_{target_table}_parameters")(
            variables, load_range, chunk_customers, prefixes = ["var_", "load_range_", f"{target_table}s_"]
        )
        process_customers = process_chunk.partial(
            query=f"""
                SELECT DISTINCT c.* 
                FROM customer c 
                JOIN salesorder s 
                ON s.customer_id = c.id 
                JOIN salesorderitem si 
                ON si.order_id = s.id 
                WHERE (s.modified_at >= %(load_range_start_time)s AND s.modified_at < %(load_range_end_time)s)
                OR (si.modified_at >= %(load_range_start_time)s AND si.modified_at < %(load_range_end_time)s)
                ORDER BY c.id
                LIMIT %({target_table}s_limit)s OFFSET %({target_table}s_offset)s
                """,
            target_table=f"{target_schema}.{target_table}",
        ).expand(parameters = customers_parameters)
        add_target_customer_indexes = PostgresOperator(
            task_id=f'add_target_{target_table}_indexes',
            postgres_conn_id=POSTGRES_CONN["conn_id"],
            sql=f"""
                CREATE INDEX IF NOT EXISTS 
                    idx_{target_table}_id ON {target_schema}.{target_table} (id);
                """
        )
        [variables, load_range, chunk_customers] >> customers_parameters
        [customers_parameters, truncate_target_customer] >> process_customers >> add_target_customer_indexes

    update_last_loaded = update_last_loaded_ts(load_range=load_range)

    start >> load_range >> [tg_items, tg_orders, tg_customers] >> update_last_loaded >> end
    start >> variables >> [tg_items, tg_orders, tg_customers]

# set trigger_rule for all tasks
for task in dag.tasks:
    task.trigger_rule = "none_failed"
