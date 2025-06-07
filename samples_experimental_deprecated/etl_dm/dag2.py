from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from datetime import timedelta
from common.tasks import get_variables, zip_dicts, run_queries
from common.config import POSTGRES_CONN, CORE_PARALLELISM, \
    DAG_CONCURRENCY, DEFAULT_VAR_DICT, DEFAULT_ODS_CONFIG, DEFAULT_DM_CONFIG

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_dm2",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    max_active_runs=1,
    concurrency=DAG_CONCURRENCY,
    catchup=False,
    tags=["etl","dm"],
) as dag:
    variables = get_variables.override(task_id="get_airflow_variables")(
        keys=["chunk_size","page_size","batch_size","text_clean_regex"])
    db_pool = "db_pool_max"
    
    target = Variable.get("dm_config", deserialize_json=True, default_var=DEFAULT_DM_CONFIG)
    source = Variable.get("ods_config", deserialize_json=True, default_var=DEFAULT_ODS_CONFIG)
    target["tables"] = [t.strip() for t in target["tables"].split(",")]
    source["tables"] = [t.strip() for t in source["tables"].split(",")]

    init_dm = EmptyOperator(task_id=f"init_{target['schema']}")
    end_dm = EmptyOperator(task_id=f"end_{target['schema']}")

    target_table = target['tables'][0]
    source_table = source['tables']
    target_sys_folder = target['sys_folder']
    with TaskGroup(f"tg_{target_table}") as tg_soif:
        truncate_target_soif = PostgresOperator(
            task_id=f"truncate_target_{target_table}",
            postgres_conn_id=POSTGRES_CONN["conn_id"],
            sql=f"""
                TRUNCATE TABLE {target['schema']}.{target_table} RESTART IDENTITY CASCADE;
                SELECT drop_special_constraints_and_indexes('{target['schema']}', '{target_table}', ARRAY['p','f','u']);
                ALTER TABLE dm.sales_order_item_flat
                    DROP CONSTRAINT IF EXISTS 
                        sales_order_item_flat_customer_email_check CASCADE;
                """,
        )

        soif_parameters = zip_dicts.override(task_id=f"prepare_{target_table}_parameters")(
            DEFAULT_VAR_DICT, variables, # chunk_soif, 
            dict(source_schema = source['schema'], source_table = source_table), 
            dict(target_schema = target['schema'], target_table = target_table), 
            prefixes = ["var_", "var_", "", ""]
        )
        process_soif = run_queries.override(
            task_id=f"process_{target_table}",
            pool=db_pool, max_active_tis_per_dag=CORE_PARALLELISM).partial(
            hook_class=PostgresHook, hook_kwargs=dict(postgress_conn_id = POSTGRES_CONN["conn_id"]),
            sql=f"/opt/airflow/sql/{target_sys_folder}/{target['schema']}/" \
                f"{target_table}/{target['schema']}_process_{target_table}.sql",
        ).expand(parameters=soif_parameters)
        
        process_fk_soif = EmptyOperator(task_id=f"process_fk_{target_table}")

        add_constraints_target_soif = run_queries.override(
            task_id=f"add_constraints_target_{target_table}")(
            hook_class=PostgresHook, hook_kwargs=dict(postgress_conn_id = POSTGRES_CONN["conn_id"]),
            handler="run",
            sql=f"/opt/airflow/sql/{target_sys_folder}/{target['schema']}/" \
                f"{target_table}/{target['schema']}_create_{target_table}_constraints_indexes.sql",
            parameters=dict(target_schema = target['schema'], target_table = target_table)
        )

        data_quality_check_soif = run_queries.override(
            task_id=f"data_quality_check_{target_table}")(
            hook_class=PostgresHook, hook_kwargs=dict(postgress_conn_id = POSTGRES_CONN["conn_id"]),
            handler="get_records", log_enabled=True,
            sql=f"/opt/airflow/sql/{target_sys_folder}/{target['schema']}/" \
                f"{target_table}/{target['schema']}_{target_table}_data_quality_check.sql",
            parameters=dict(target_schema = target['schema'], target_table = target_table)
            )

        # Mark last load timestamp (audit/log)
        mark_loaded_soif = run_queries.override(
            task_id=f"mark_last_loaded_{target_table}")(
            hook_class=PostgresHook, hook_kwargs=dict(postgress_conn_id = POSTGRES_CONN["conn_id"]),
            handler="run",
            sql="/opt/airflow/sql/{target_sys_folder}/insert_load_audit.sql",
            parameters = (f"{target['schema']}.{target_table}", 
                            str(data_quality_check_soif))
        )

        init_dm >> truncate_target_soif
        variables >> soif_parameters
        [soif_parameters, truncate_target_soif] >> process_soif
        process_soif >> process_fk_soif >> \
            add_constraints_target_soif >> data_quality_check_soif >> \
            mark_loaded_soif

    # full chain
    init_dm >> variables >>tg_soif >> end_dm

# set trigger_rule for all tasks
for task in dag.tasks:
    task.trigger_rule = "none_failed"
