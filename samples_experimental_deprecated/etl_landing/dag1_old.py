from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from common.tasks import get_load_range, generate_datetime_chunks_task, \
    generate_num_chunks_task, process_chunk, update_last_loaded_ts
from common.tasks import DATETIME_INTERVAL, DAG_CONCURRENCY
from datetime import timedelta

DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': days_ago(DATETIME_INTERVAL),
    'retries': 0,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='etl_landing1',
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'landing'],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    load_range = get_load_range()
    chunk_datetimes = generate_datetime_chunks_task(
        type_="dynamic", range_time=load_range)
    target_schema = "lz"

    with TaskGroup("tg_items") as tg_items:
        target_table = "salesorderitem"
        truncate_target_items = PostgresOperator(
            task_id='truncate_target_items_and_drop_indexes',
            postgres_conn_id='postgres_default',
            sql=[f'TRUNCATE TABLE {target_schema}.{target_table} RESTART IDENTITY CASCADE;',
                f"SELECT drop_special_constraints_and_indexes('{target_schema}', '{target_table}', ARRAY[]::text[]);"]
        )
        process_items = process_chunk.partial(
            query="""
                SELECT DISTINCT si.* 
                FROM salesorderitem si 
                JOIN salesorder s 
                ON s.id = si.order_id 
                WHERE (s.modified_at >= "{chunk_query_params["start_val"]}" AND s.modified_at < "{chunk_query_params["end_val"]}") 
                OR (si.modified_at >= "{chunk_query_params["start_val"]}" AND si.modified_at < "{chunk_query_params["end_val"]}")
                """,
            target_table=f"{target_schema}.{target_table}"
        ).expand(chunk_query_params=chunk_datetimes)
        add_target_items_indexes = PostgresOperator(
            task_id='add_target_items_indexes',
            postgres_conn_id='postgres_default',
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
                CREATE INDEX IF NOT EXISTS 
                    idx_{target_table}_item_id_modified_at ON {target_schema}.{target_table} (item_id, modified_at DESC);
                """
        )
        [chunk_datetimes, truncate_target_items] >> process_items >> add_target_items_indexes

    with TaskGroup("tg_orders") as tg_orders:
        target_table = "salesorder"
        truncate_target_orders = PostgresOperator(
            task_id='truncate_target_orders_and_drop_indexes',
            postgres_conn_id='postgres_default',
            sql=[f'TRUNCATE TABLE {target_schema}.{target_table} RESTART IDENTITY CASCADE;',
                f"SELECT drop_special_constraints_and_indexes('{target_schema}', '{target_table}', ARRAY[]::text[]);"]
        )
        process_orders = process_chunk.partial(
            query="""
                SELECT DISTINCT s.* 
                FROM salesorder s 
                JOIN salesorderitem si 
                ON si.order_id = s.id 
                WHERE (si.modified_at >= "{chunk_query_params["start_val"]}" AND si.modified_at < "{chunk_query_params["end_val"]}") 
                OR (s.modified_at >= "{chunk_query_params["start_val"]}" AND s.modified_at < "{chunk_query_params["end_val"]}")
                """,
            target_table=f"{target_schema}.{target_table}",
        ).expand(chunk_query_params=chunk_datetimes)
        add_target_orders_indexes = PostgresOperator(
            task_id='add_target_orders_indexes',
            postgres_conn_id='postgres_default',
            sql=f"""
                CREATE INDEX IF NOT EXISTS 
                    idx_{target_table}_id ON {target_schema}.{target_table} (id);
                CREATE INDEX IF NOT EXISTS 
                    idx_{target_table}_modified_at ON {target_schema}.{target_table} (modified_at);
                CREATE INDEX IF NOT EXISTS
                    idx_{target_table}_id_modified_at ON {target_schema}.{target_table} (id, modified_at DESC);
                """
        )
        [chunk_datetimes, truncate_target_orders] >> process_orders >> add_target_orders_indexes

    # with TaskGroup("tg_customers") as tg_customers:
    #     staging_table_name = "staging_customer"
    #     target_table = "customer"
    #     create_staging_customer = MySqlOperator(
    #         task_id='create_staging_customer',
    #         mysql_conn_id='mysql_default',
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
    #     chunk_customers = generate_num_chunks_task.override(task_id="chunks_customers")(
    #         query=f"""
    #             SELECT MIN(id) AS min_val, MAX(id) AS max_val 
    #             FROM `{staging_table_name}`
    #             """, 
    #         no_of_chunks=max(DAG_CONCURRENCY // 4, 1))
    #     truncate_target_customer = PostgresOperator(
    #         task_id='truncate_target_customer_and_drop_indexes',
    #         postgres_conn_id='postgres_default',
    #         sql=[f'TRUNCATE TABLE {target_schema}.{target_table} RESTART IDENTITY CASCADE;',
    #             f"SELECT drop_special_constraints_and_indexes('{target_schema}', '{target_table}', ARRAY[]::text[]);"]
    #     )
    #     process_customers = process_chunk.partial(
    #         query="""
    #             SELECT * 
    #             FROM `{normal_params["staging_table_name"]}` 
    #             WHERE id >= {chunk_query_params["start_val"]} 
    #             AND id < {chunk_query_params["end_val"]}
    #             """,
    #         target_table=f"{target_schema}.{target_table}",
    #         normal_params=dict(staging_table_name=staging_table_name)
    #     ).expand(chunk_query_params=chunk_customers)
    #     drop_staging_customer = MySqlOperator(
    #         task_id='drop_staging_customer',
    #         mysql_conn_id='mysql_default',
    #         sql=f'DROP TABLE IF EXISTS `{staging_table_name}`'
    #     )
    #     add_target_customer_indexes = PostgresOperator(
    #         task_id='add_target_customer_indexes',
    #         postgres_conn_id='postgres_default',
    #         sql=f"""
    #             CREATE INDEX IF NOT EXISTS 
    #                 idx_{target_table}_id ON {target_schema}.{target_table} (id);
    #             """
    #     )
    #     [create_staging_customer >> chunk_customers, truncate_target_customer] >> process_customers >> [drop_staging_customer, add_target_customer_indexes]

    with TaskGroup("tg_customers2") as tg_customers2:
        target_table = "customer"
        chunk_customers = generate_num_chunks_task.override(task_id="chunks_customers")(
            query="""
                SELECT MIN(c.id) AS min_val, MAX(c.id) AS max_val 
                FROM customer c 
                JOIN salesorder s 
                ON s.customer_id = c.id 
                JOIN salesorderitem si 
                ON si.order_id = s.id 
                WHERE (s.modified_at >= "{partial_params["start_time"]}" AND s.modified_at < "{partial_params["end_time"]}")
                OR (si.modified_at >= "{partial_params["start_time"]}" AND si.modified_at < "{partial_params["end_time"]}")
                """, 
            no_of_chunks= max(DAG_CONCURRENCY // 4, 1), partial_params = load_range)
        truncate_target_customer = PostgresOperator(
            task_id='truncate_target_customer_and_drop_indexes',
            postgres_conn_id='postgres_default',
            sql=[f'TRUNCATE TABLE {target_schema}.{target_table} RESTART IDENTITY CASCADE;',
                f"SELECT drop_special_constraints_and_indexes('{target_schema}', '{target_table}', ARRAY[]::text[]);"]
        )
        process_customers = process_chunk.partial(
            query="""
                SELECT DISTINCT c.* 
                FROM customer c 
                JOIN salesorder s 
                ON s.customer_id = c.id 
                JOIN salesorderitem si 
                ON si.order_id = s.id 
                WHERE 
                (s.modified_at >= "{partial_params["start_time"]}" AND s.modified_at < "{partial_params["end_time"]}" 
                OR si.modified_at >= "{partial_params["start_time"]}" AND si.modified_at < "{partial_params["end_time"]}") 
                AND c.id >= {chunk_query_params["start_val"]} AND c.id < {chunk_query_params["end_val"]}
                """,
            target_table=f"{target_schema}.{target_table}",
            partial_params=load_range
        ).expand(chunk_query_params = chunk_customers)
        add_target_customer_indexes = PostgresOperator(
            task_id='add_target_customer_indexes',
            postgres_conn_id='postgres_default',
            sql=f"""
                CREATE INDEX IF NOT EXISTS 
                    idx_{target_table}_id ON {target_schema}.{target_table} (id);
                """
        )
        [chunk_customers, truncate_target_customer] >> process_customers >> add_target_customer_indexes

    update_last_loaded = update_last_loaded_ts(load_range=load_range)

    start >> load_range >> [tg_items, tg_orders, tg_customers2] >> update_last_loaded >> end

# set trigger_rule for all tasks
for task in dag.tasks:
    task.trigger_rule = "none_failed"
