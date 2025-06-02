from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from common.tasks import (
    get_load_range, generate_limit_offset_task, process_data,
    update_last_loaded_ts, zip_dicts, get_variables)
from common.config import (
    DATETIME_INTERVAL, POSTGRES_CONN, CORE_PARALLELISM, DAG_CONCURRENCY,
    DEFAULT_VAR_DICT, DEFAULT_SOURCE_CONFIG, DEFAULT_LZ_CONFIG)
from datetime import timedelta
from etl_landing.data import sql

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": days_ago(DATETIME_INTERVAL),
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="etl_landing",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    max_active_runs=1,
    concurrency=DAG_CONCURRENCY,
    catchup=False,
    tags=["etl", "landing"],
) as dag:

    # 1. Retrieve Airflow variables and constants
    variables = get_variables.override(task_id="get_airflow_variables")(
        keys=["chunk_size", "page_size", "batch_size", "text_clean_regex"])
    db_pool = "db_pool_max"
    load_range = get_load_range()

    target = Variable.get("target_config", deserialize_json=True, default_var=DEFAULT_LZ_CONFIG)
    source = Variable.get("source_config", deserialize_json=True, default_var=DEFAULT_SOURCE_CONFIG)
    target["tables"] = [t.strip() for t in target["tables"].split(",")]
    source["tables"] = [t.strip() for t in source["tables"].split(",")]

    init_lz = EmptyOperator(task_id=f"init_{target['schema']}")
    end_lz = EmptyOperator(task_id=f"end_{target['schema']}")

    # 2. Dictionary to hold instantiated task references for later wiring
    tasks = {}

    # 3. Loop over target tables, create TaskGroup for each, and populate task_refs
    for i, tbl in enumerate(target["tables"]):
        src_tbl = source['tables'][i]  # same order assumed: target['tables'] <==> source['tables']
        if tbl not in sql:
            raise ValueError(f"No SQL templates defined for target table '{tbl}'")
        sql_defs = sql[tbl]

        # Format SQL strings with the current schemas and table names
        prepare_sql = sql_defs["prepare"].format(
            target_schema=target["schema"], target_table=tbl
        ).strip()
        count_sql = sql_defs["count"].format(
            source_schema=source["schema"], source_table=src_tbl
        ).strip()
        process_sql = sql_defs["process"].format(
            source_schema=source["schema"], source_table=src_tbl
        ).strip()
        index_sql = sql_defs["index"].format(
            target_schema=target["schema"], target_table=tbl,
            text_clean_regex=DEFAULT_VAR_DICT["text_clean_regex"]
        ).strip()

        # 4. Create a TaskGroup for this table
        with TaskGroup(group_id=f"tg_{tbl}") as tg:
            
            with TaskGroup(group_id=f"tg_preprocess_{tbl}") as tg_pre:
                # 4a) Truncate + drop indexes using PostgresOperator
                prepare_target = PostgresOperator(
                    task_id=f'prepare_target_{tbl}',
                    postgres_conn_id=POSTGRES_CONN["conn_id"],
                    sql=prepare_sql
                )

                # 4b) Generate limit/offset values
                chunk_source = generate_limit_offset_task.override(task_id=f"chunk_{src_tbl}")(
                    query=count_sql, type_="dynamic", parameters=load_range)

                # 4c) Zip parameters into a list for expand()
                prepare_params = zip_dicts.override(task_id=f"prepare_{src_tbl}_parameters")(
                    DEFAULT_VAR_DICT, variables, load_range, chunk_source,
                    prefixes=["var_", "var_", "load_range_", f"{src_tbl}_"]
                )

                # Wire internal dependencies
                [variables, load_range, chunk_source] >> prepare_params

            with TaskGroup(group_id=f"tg_process_{tbl}") as tg_proc:
                # 4d) Parallel processing with expand()
                process = process_data.override(
                    task_id=f"process_{src_tbl}",
                    pool=db_pool, max_active_tis_per_dag=CORE_PARALLELISM
                ).partial(
                    query=process_sql, target_table=f"{target['schema']}.{tbl}"
                ).expand(parameters=prepare_params)

            with TaskGroup(group_id=f"tg_postprocess_{tbl}") as tg_post:
                # 4e) Add back indexes
                index_target = PostgresOperator(
                    task_id=f"index_target_{tbl}",
                    postgres_conn_id=POSTGRES_CONN["conn_id"],
                    sql=index_sql
                )

            # 4f) Wire internal dependencies
            tg_pre >> tg_proc >> tg_post

            # 4g) Store references in task_refs
            tasks[tbl] = {
                "task_group": tg,
                "tg_pre": tg_pre,
                "tg_proc": tg_proc,
                "tg_post": tg_post,
                "prepare": prepare_target,
                "chunk": chunk_source,
                "params": prepare_params,
                "process": process,
                "index": index_target,
            }

    update_last_loaded = update_last_loaded_ts(load_range=load_range)

    # Downstream / final steps
    trigger_etl_ods_dag = TriggerDagRunOperator(
        task_id="trigger_etl_ods_dag", trigger_dag_id="etl_ods")
    
    # DAG‐level wiring
    taskgroups = [refs["task_group"] for refs in tasks.values()]
    init_lz >> load_range >> taskgroups >> update_last_loaded >> end_lz >> trigger_etl_ods_dag
    init_lz >> variables >> taskgroups

    # (Optional) Example of chaining one process into another
    # If you wanted salesorderitem → salesorder → customer at the process level:
    # tasks["salesorderitem"]["process"] >> tasks["salesorder"]["process"] >> tasks["customer"]["process"]

# Set trigger_rule="none_failed" for all tasks
for task in dag.tasks:
    task.trigger_rule = "none_failed"
