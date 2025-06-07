from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from common.tasks import (
    check_sanity, get_load_range, process_data,
    update_last_loaded_ts, zip_dicts, get_variables, run_queries)
from common.utils import get_kwargs
from common.config import (
    DATETIME_INTERVAL, POSTGRES_CONN, CORE_PARALLELISM, DAG_CONCURRENCY,
    DEFAULT_VAR_DICT, DEFAULT_SOURCE_CONFIG, DEFAULT_LZ_CONFIG)
from datetime import timedelta
from etl_landing.data_final import sql
import pendulum

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": pendulum.today("UTC").subtract(days=abs(DATETIME_INTERVAL)),
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="etl_landing",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    max_active_runs=1,
    max_active_tasks=DAG_CONCURRENCY,
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
    for i, tgt in enumerate(target["tables"]):
        src_tbl = source['tables'][i]  # same order assumed: target['tables'] <==> source['tables']
        if tgt not in sql:
            raise ValueError(f"No SQL templates defined for target table '{tgt}'")
        sql_defs = sql[tgt]

        # Format SQL strings with the current schemas and table names
        prepare_sql = sql_defs["prepare"].format(
            target_schema=target["schema"], target_table=tgt
        ).strip()
        
        chunk_meta = sql_defs["chunk"]
        if chunk_meta["kws"].get("query"):
            chunk_meta["kws"]["query"] = chunk_meta["kws"]["query"].format(
                source_schema=source["schema"], source_table=src_tbl
            ).strip()

        process_sql = sql_defs["process"].format(
            source_schema=source["schema"], source_table=src_tbl
        ).strip()
        
        index_sql = sql_defs["index"].format(
            target_schema=target["schema"], target_table=tgt,
            text_clean_regex=DEFAULT_VAR_DICT["text_clean_regex"]
        ).strip()

        mark_loaded_sql = sql_defs["mark_loaded"].format(
            target_sys_folder=target['sys_folder']).strip()

        # 4. Create a TaskGroup for this table
        with TaskGroup(group_id=f"tg_{tgt}") as tg:
            
            with TaskGroup(group_id=f"tg_preprocess_{tgt}") as tg_pre:
                # 4a) Truncate + drop target indexes
                prepare_target = SQLExecuteQueryOperator(
                    task_id=f'prepare_target_{tgt}',
                    conn_id=POSTGRES_CONN["conn_id"],
                    sql=prepare_sql
                )

                # 4b) Generate chunk values
                chunk_source = chunk_meta["handler"].override(task_id=f"chunk_{src_tbl}")(
                    **chunk_meta["kws"], **get_kwargs(chunk_meta["kwd"], load_range))

                # Check sanity of returned chunk values
                chunk_sanity = check_sanity.override(task_id=f"{src_tbl}_chunk_sanity")(
                    chunk_source, 
                    route_valid=f"tg_{tgt}.tg_preprocess_{tgt}.prepare_{src_tbl}_parameters", 
                    route_invalid=f"tg_{tgt}.tg_postprocess_{tgt}.index_target_{tgt}"
                )

                # 4c) Zip parameters into a list for expand()
                prepare_params = zip_dicts.override(task_id=f"prepare_{src_tbl}_parameters")(
                    DEFAULT_VAR_DICT, variables, load_range, chunk_source,
                    prefixes=["var_", "var_", "load_range_", f"{src_tbl}_"]
                )

                # Wire preprocess dependencies
                [variables, load_range] >> prepare_params
                chunk_source >> chunk_sanity >> prepare_params

            with TaskGroup(group_id=f"tg_process_{tgt}") as tg_proc:
                # 4d) Parallel processing with expand()
                process = process_data.override(
                    task_id=f"process_{src_tbl}",
                    pool=db_pool, max_active_tis_per_dag=CORE_PARALLELISM
                ).partial(
                    query=process_sql, target_table=f"{target['schema']}.{tgt}"
                ).expand(parameters=prepare_params)

            with TaskGroup(group_id=f"tg_postprocess_{tgt}") as tg_post:
                # 4e) Add back indexes
                index_target = SQLExecuteQueryOperator(
                    task_id=f"index_target_{tgt}",
                    conn_id=POSTGRES_CONN["conn_id"],
                    sql=index_sql
                )

                # 4f) Mark load audit for this target table
                mark_loaded = run_queries.override(task_id=f"mark_last_loaded_{tgt}")(
                    hook_class=PostgresHook,
                    hook_kwargs={"postgres_conn_id": POSTGRES_CONN["conn_id"]},
                    handler="run",
                    sql=mark_loaded_sql,
                    parameters=(f"{target['schema']}.{tgt}", None)
                )

                # 3h) wire postprocess dependencies
                process >> index_target >> mark_loaded
                chunk_sanity >> index_target

            # 4g) Wire internal dependencies
            tg_pre >> tg_proc >> tg_post

            # 4h) Store references in task_refs
            tasks[tgt] = {
                "task_group": tg,
                "tg_pre": tg_pre,
                "tg_proc": tg_proc,
                "tg_post": tg_post,
                "prepare": prepare_target,
                "chunk": chunk_source,
                "check": chunk_sanity,
                "params": prepare_params,
                "process": process,
                "index": index_target,
                "mark_loaded": mark_loaded
            }

    update_last_loaded = update_last_loaded_ts(load_range=load_range)

    # Downstream / final steps
    trigger_etl_ods_dag = TriggerDagRunOperator(
        task_id="trigger_etl_ods_dag", trigger_dag_id="etl_ods")
    
    # 5. DAG‐level wiring
    taskgroups = [refs["task_group"] for refs in tasks.values()]
    init_lz >> load_range >> taskgroups >> update_last_loaded >> end_lz >> trigger_etl_ods_dag
    init_lz >> variables >> taskgroups

    # (Optional) Example of chaining one process into another
    # If you wanted salesorderitem -> salesorder -> customer at the process level:
    # tasks["salesorderitem"]["process"] >> tasks["salesorder"]["process"] >> tasks["customer"]["process"]

# ─── Safely set trigger_rule on each operator ─────────────────────────────────
# 6. Ensure that no downstream task runs if any upstream has failed
for task in dag.tasks:
    try:
        task.trigger_rule = "none_failed"
    except (AttributeError, TypeError):
        pass
