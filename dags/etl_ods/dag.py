from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from datetime import timedelta
from common.tasks import get_variables, generate_limit_offset_task, \
    zip_dicts, run_queries
from common.tasks import POSTGRES_CONN, CORE_PARALLELISM

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_ods",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    catchup=False,
    tags=["etl","ods"],
) as dag:
    init_ods = EmptyOperator(task_id="init_ods")
    end_ods = EmptyOperator(task_id="end_ods")
    variables = get_variables.override(task_id="get_airflow_variables")(
        keys=["chunk_size","page_size","batch_size"])
    db_pool = "db_pool_max"
    target_schema = "ods"
    source_schema = "lz"

    target_table = "salesorderitem"
    source_table = "salesorderitem"
    with TaskGroup(f"tg_{target_table}") as tg_salesorderitem:
        truncate_target_salesorderitem = PostgresOperator(
            task_id=f"truncate_target_{target_table}",
            postgres_conn_id=POSTGRES_CONN["conn_id"],
            sql=f"""
                TRUNCATE TABLE {target_schema}.{target_table} RESTART IDENTITY CASCADE;
                SELECT drop_special_constraints_and_indexes('{target_schema}', '{target_table}', ARRAY['p','f','u']);
                """
        )

        chunk_salesorderitem = generate_limit_offset_task.override(task_id=f"chunk_{target_table}")(
            query=f"""
                SELECT COUNT(*)
                FROM {source_schema}.{source_table}     -- lz.salesorderitem
                WHERE item_id    IS NOT NULL
                AND order_id     IS NOT NULL
                AND product_id   IS NOT NULL
                AND qty_ordered  IS NOT NULL   AND qty_ordered  >= 0
                AND price        IS NOT NULL   AND price        >= 0
                AND line_total   IS NOT NULL   AND line_total   >= 0
                AND created_at   IS NOT NULL
                AND modified_at  IS NOT NULL   AND modified_at >= created_at
                """,
            type_="dynamic",
            hook_class=PostgresHook, hook_kwargs=dict(postgress_conn_id = POSTGRES_CONN["conn_id"])
        )

        salesorderitem_parameters = zip_dicts.override(task_id=f"prepare_{target_table}_parameters")(
            variables, chunk_salesorderitem, 
            dict(source_schema = source_schema, source_table = source_table), 
            dict(target_schema = target_schema, target_table = target_table), 
            prefixes = ["var_", "outer_", "", ""]
        )
        process_salesorderitem = run_queries.override(
            task_id=f"process_{target_table}",
            pool=db_pool, max_active_tis_per_dag=CORE_PARALLELISM).partial(
            hook_class=PostgresHook, hook_kwargs=dict(postgress_conn_id = POSTGRES_CONN["conn_id"]),
            sql=f"/opt/airflow/sql/postgres_data_warehouse/{target_schema}/" \
                f"{target_table}/{target_schema}_process_{target_table}.sql",
        ).expand(parameters=salesorderitem_parameters)
        
        process_fk_salesorderitem = run_queries.override(
            task_id=f"process_fk_{target_table}",
            pool=db_pool, max_active_tis_per_dag=CORE_PARALLELISM).partial(
            hook_class=PostgresHook, hook_kwargs=dict(postgress_conn_id = POSTGRES_CONN["conn_id"]),
            sql=f"/opt/airflow/sql/postgres_data_warehouse/{target_schema}/" \
                f"{target_table}/{target_schema}_process_fk_{target_table}.sql",
        ).expand(parameters=salesorderitem_parameters)

        add_constraints_target_salesorderitem = run_queries.override(
            task_id=f"add_constraints_target_{target_table}")(
            hook_class=PostgresHook, hook_kwargs=dict(postgress_conn_id = POSTGRES_CONN["conn_id"]),
            handler="run",
            sql=f"/opt/airflow/sql/postgres_data_warehouse/{target_schema}/" \
                f"{target_table}/ods_create_{target_table}_constraints_indexes.sql",
            parameters=dict(target_schema = target_schema, target_table = target_table)
        )

        data_quality_check_salesorderitem = run_queries.override(
            task_id=f"data_quality_check_{target_table}")(
            hook_class=PostgresHook, hook_kwargs=dict(postgress_conn_id = POSTGRES_CONN["conn_id"]),
            handler="get_records", log_enabled=True,
            sql=f"/opt/airflow/sql/postgres_data_warehouse/{target_schema}/" \
                f"{target_table}/ods_{target_table}_data_quality_check.sql",
            parameters=dict(target_schema = target_schema, target_table = target_table)
            )

        # Mark last load timestamp (audit/log)
        mark_loaded_salesorderitem = run_queries.override(
            task_id=f"mark_last_loaded_{target_table}")(
            hook_class=PostgresHook, hook_kwargs=dict(postgress_conn_id = POSTGRES_CONN["conn_id"]),
            handler="run",
            sql="/opt/airflow/sql/postgres_data_warehouse/insert_load_audit.sql",
            parameters = (f"{target_schema}.{target_table}", 
                            str(data_quality_check_salesorderitem))
        )

        init_ods >> [truncate_target_salesorderitem, chunk_salesorderitem]
        [variables, chunk_salesorderitem] >> salesorderitem_parameters
        [salesorderitem_parameters, truncate_target_salesorderitem] >> process_salesorderitem
        process_salesorderitem >> process_fk_salesorderitem >> \
            add_constraints_target_salesorderitem >> data_quality_check_salesorderitem >> \
            mark_loaded_salesorderitem

    target_table = "salesorder"
    source_table = "salesorder"
    with TaskGroup(f"tg_{target_table}") as tg_salesorder:
        truncate_target_salesorder = PostgresOperator(
            task_id=f"truncate_target_{target_table}",
            postgres_conn_id=POSTGRES_CONN["conn_id"],
            sql=f"""
                TRUNCATE TABLE {target_schema}.{target_table} RESTART IDENTITY CASCADE;
                SELECT drop_special_constraints_and_indexes('{target_schema}', '{target_table}', ARRAY['p','f','u']);
            """
        )

        chunk_salesorder = generate_limit_offset_task.override(task_id=f"chunk_{target_table}")(
            query=f"""
                SELECT COUNT(*)
                FROM {source_schema}.{source_table}     -- lz.salesorder
                WHERE id IS NOT NULL
                AND customer_id IS NOT NULL
                AND order_number IS NOT NULL
                AND created_at IS NOT NULL
                AND modified_at IS NOT NULL
                AND modified_at >= created_at
                AND order_total IS NOT NULL 
                AND order_total >= 0
                AND total_qty_ordered IS NOT NULL
                AND total_qty_ordered >= 0
                """,
            type_="dynamic",
            hook_class=PostgresHook, hook_kwargs=dict(postgress_conn_id = POSTGRES_CONN["conn_id"])
        )

        salesorder_parameters = zip_dicts.override(task_id=f"prepare_{target_table}_parameters")(
        variables, chunk_salesorder, 
        dict(source_schema = source_schema, source_table = source_table), 
        dict(target_schema = target_schema, target_table = target_table), 
        prefixes = ["var_", "outer_", "", ""]
        )
        process_salesorder = run_queries.override(
            task_id=f"process_{target_table}",
            pool=db_pool, max_active_tis_per_dag=CORE_PARALLELISM).partial(
            hook_class=PostgresHook, hook_kwargs=dict(postgress_conn_id = POSTGRES_CONN["conn_id"]),
            sql=f"/opt/airflow/sql/postgres_data_warehouse/{target_schema}/" \
                f"{target_table}/{target_schema}_process_{target_table}.sql",
        ).expand(parameters=salesorder_parameters)

        process_fk_salesorder = run_queries.override(
            task_id=f"process_fk_{target_table}",
            pool=db_pool, max_active_tis_per_dag=CORE_PARALLELISM).partial(
            hook_class=PostgresHook, hook_kwargs=dict(postgress_conn_id = POSTGRES_CONN["conn_id"]),
            sql=f"/opt/airflow/sql/postgres_data_warehouse/{target_schema}/" \
                f"{target_table}/{target_schema}_process_fk_{target_table}.sql",
        ).expand(parameters=salesorder_parameters)

        add_constraints_target_salesorder = run_queries.override(
            task_id=f"add_constraints_target_{target_table}")(
            hook_class=PostgresHook, hook_kwargs=dict(postgress_conn_id = POSTGRES_CONN["conn_id"]),
            handler="run",
            sql=f"/opt/airflow/sql/postgres_data_warehouse/{target_schema}/" \
                f"{target_table}/ods_create_{target_table}_constraints_indexes.sql",
            parameters=dict(target_schema = target_schema, target_table = target_table)
        )
        
        data_quality_check_salesorder = run_queries.override(
            task_id=f"data_quality_check_{target_table}")(
            hook_class=PostgresHook, hook_kwargs=dict(postgress_conn_id = POSTGRES_CONN["conn_id"]),
            handler="get_records", log_enabled=True,
            sql=f"/opt/airflow/sql/postgres_data_warehouse/{target_schema}/" \
                f"{target_table}/ods_{target_table}_data_quality_check.sql",
            parameters=dict(target_schema = target_schema, target_table = target_table)
            )

        # Mark last load timestamp (audit/log)
        mark_loaded_salesorder = run_queries.override(
            task_id=f"mark_last_loaded_{target_table}")(
            hook_class=PostgresHook, hook_kwargs=dict(postgress_conn_id = POSTGRES_CONN["conn_id"]),
            handler="run",
            sql="/opt/airflow/sql/postgres_data_warehouse/insert_load_audit.sql",
            parameters = (f"{target_schema}.{target_table}", 
                            str(data_quality_check_salesorder))
        )

        init_ods >> [truncate_target_salesorder, chunk_salesorder]
        [variables, chunk_salesorder] >> salesorder_parameters
        [salesorder_parameters, truncate_target_salesorder] >> process_salesorder
        process_salesorder >> process_fk_salesorder >> \
            add_constraints_target_salesorder >> data_quality_check_salesorder >> \
            mark_loaded_salesorder

    target_table = "customer"
    source_table = "customer"
    with TaskGroup(f"tg_{target_table}") as tg_customer:
        truncate_target_customer = PostgresOperator(
            task_id=f"truncate_target_{target_table}",
            postgres_conn_id=POSTGRES_CONN["conn_id"],
            sql=f"""
                TRUNCATE TABLE {target_schema}.{target_table} RESTART IDENTITY CASCADE;
                SELECT drop_special_constraints_and_indexes('{target_schema}', '{target_table}', ARRAY['p','f','u']);
                ALTER TABLE ods.customer 
                    DROP CONSTRAINT IF EXISTS customer_email_check CASCADE;
            """
        )

        chunk_customer = generate_limit_offset_task.override(task_id=f"chunk_{target_table}")(
            query=f"""
                SELECT COUNT(*)
                FROM {source_schema}.{source_table}   -- lz.customer
                WHERE id             IS NOT NULL
                AND first_name       IS NOT NULL
                AND last_name        IS NOT NULL
                AND (gender IS NULL 
                    OR LOWER(gender) IN ('female','male')) 
                AND email            IS NOT NULL
                AND shipping_address IS NOT NULL
                AND email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{{2,}}$'
                AND 1 = 1;
                """,
            type_="dynamic",
            hook_class=PostgresHook, hook_kwargs=dict(postgress_conn_id = POSTGRES_CONN["conn_id"])
        )

        customer_parameters = zip_dicts.override(task_id=f"prepare_{target_table}_parameters")(
            variables, chunk_customer, 
            dict(source_schema = source_schema, source_table = source_table), 
            dict(target_schema = target_schema, target_table = target_table), 
            prefixes = ["var_", "outer_", "", ""]
        )
        process_customer = run_queries.override(
            task_id=f"process_{target_table}",
            pool=db_pool, max_active_tis_per_dag=CORE_PARALLELISM).partial(
            hook_class=PostgresHook, hook_kwargs=dict(postgress_conn_id = POSTGRES_CONN["conn_id"]),
            sql=f"/opt/airflow/sql/postgres_data_warehouse/{target_schema}/" \
                f"{target_table}/{target_schema}_process_{target_table}.sql",
        ).expand(parameters=customer_parameters)

        add_constraints_target_customer = run_queries.override(
            task_id=f"add_constraints_target_{target_table}")(
            hook_class=PostgresHook, hook_kwargs=dict(postgress_conn_id = POSTGRES_CONN["conn_id"]),
            handler="run",
            sql=f"/opt/airflow/sql/postgres_data_warehouse/{target_schema}/" \
                f"{target_table}/ods_create_{target_table}_constraints_indexes.sql",
            parameters=dict(target_schema = target_schema, target_table = target_table)
        )

        data_quality_check_customer = run_queries.override(
            task_id=f"data_quality_check_{target_table}")(
            hook_class=PostgresHook, hook_kwargs=dict(postgress_conn_id = POSTGRES_CONN["conn_id"]),
            handler="get_records", log_enabled=True,
            sql=f"/opt/airflow/sql/postgres_data_warehouse/{target_schema}/" \
                f"{target_table}/ods_{target_table}_data_quality_check.sql",
            parameters=dict(target_schema = target_schema, target_table = target_table)
            )

        # Mark last load timestamp (audit/log)
        mark_loaded_customer = run_queries.override(
            task_id=f"mark_last_loaded_{target_table}")(
            hook_class=PostgresHook, hook_kwargs=dict(postgress_conn_id = POSTGRES_CONN["conn_id"]),
            handler="run",
            sql="/opt/airflow/sql/postgres_data_warehouse/insert_load_audit.sql",
            parameters = (f"{target_schema}.{target_table}", 
                            str(data_quality_check_customer))
        )

        init_ods >> [truncate_target_customer, chunk_customer]
        [variables, chunk_customer] >> customer_parameters
        [customer_parameters, truncate_target_customer] >> process_customer
        process_customer >> add_constraints_target_customer >> \
            data_quality_check_customer >> mark_loaded_customer

    target_table = "product"
    source_table = "salesorderitem"
    with TaskGroup(f"tg_{target_table}") as tg_product:
        truncate_target_product = PostgresOperator(
            task_id=f"truncate_target_{target_table}",
            postgres_conn_id=POSTGRES_CONN["conn_id"],
            sql=f"""
                TRUNCATE TABLE {target_schema}.{target_table} RESTART IDENTITY CASCADE;
                SELECT drop_special_constraints_and_indexes('{target_schema}', '{target_table}', ARRAY['p','f','u']);
            """
        )

        chunk_product = generate_limit_offset_task.override(task_id=f"chunk_{target_table}")(
            query=f"""
                    SELECT COUNT(*)
                    FROM {source_schema}.{source_table}   -- lz.salesorderitem
                    WHERE product_id IS NOT NULL
                    AND product_sku IS NOT NULL
                    AND product_name IS NOT NULL
                """,
            type_="dynamic",
            hook_class=PostgresHook, hook_kwargs=dict(postgress_conn_id = POSTGRES_CONN["conn_id"])
        )
        
        product_parameter = zip_dicts.override(task_id=f"prepare_{target_table}_parameters")(
            variables, chunk_product, 
            dict(source_schema = source_schema, source_table = source_table), 
            dict(target_schema = target_schema, target_table = target_table), 
            prefixes = ["var_", "outer_", "", ""]
        )
        process_product = run_queries.override(
            task_id=f"process_{target_table}",
            pool=db_pool, max_active_tis_per_dag=CORE_PARALLELISM).partial(
            hook_class=PostgresHook, hook_kwargs=dict(postgress_conn_id = POSTGRES_CONN["conn_id"]),
            sql=f"/opt/airflow/sql/postgres_data_warehouse/{target_schema}/" \
                f"{target_table}/{target_schema}_process_{target_table}.sql",
        ).expand(parameters=product_parameter)

        add_constraints_target_product = run_queries.override(
            task_id=f"add_constraints_target_{target_table}")(
            hook_class=PostgresHook, hook_kwargs=dict(postgress_conn_id = POSTGRES_CONN["conn_id"]),
            handler="run",
            sql=f"/opt/airflow/sql/postgres_data_warehouse/{target_schema}/" \
                f"{target_table}/ods_create_{target_table}_constraints_indexes.sql",
            parameters=dict(target_schema = target_schema, target_table = target_table)
        )

        data_quality_check_product = run_queries.override(
            task_id=f"data_quality_check_{target_table}")(
            hook_class=PostgresHook, hook_kwargs=dict(postgress_conn_id = POSTGRES_CONN["conn_id"]),
            handler="get_records", log_enabled=True,
            sql=f"/opt/airflow/sql/postgres_data_warehouse/{target_schema}/" \
                f"{target_table}/ods_{target_table}_data_quality_check.sql",
            parameters=dict(target_schema = target_schema, target_table = target_table)
            )

        # Mark last load timestamp (audit/log)
        mark_loaded_product = run_queries.override(
            task_id=f"mark_last_loaded_{target_table}")(
            hook_class=PostgresHook, hook_kwargs=dict(postgress_conn_id = POSTGRES_CONN["conn_id"]),
            handler="run",
            sql="/opt/airflow/sql/postgres_data_warehouse/insert_load_audit.sql",
            parameters = (f"{target_schema}.{target_table}", 
                            str(data_quality_check_product))
        )

        init_ods >> [truncate_target_product, chunk_product]
        [variables, chunk_product] >> product_parameter
        [product_parameter, truncate_target_product] >> process_product
        process_product >> add_constraints_target_product >> \
            data_quality_check_product >> mark_loaded_product

# truncate tables and drop constraints & indexes in referential order
truncate_target_salesorderitem >> [truncate_target_product, truncate_target_salesorder]
truncate_target_salesorder >> truncate_target_customer

# process foreign key deduplication in referential order
# add target tables' constraints in referential order
add_constraints_target_customer >> process_fk_salesorder
[add_constraints_target_salesorder, 
 add_constraints_target_product] >> process_fk_salesorderitem

# full chain
init_ods >> [tg_salesorderitem, tg_salesorder, tg_customer, tg_product] >> end_ods

# set trigger_rule for all tasks
for task in dag.tasks:
    task.trigger_rule = "none_failed"
