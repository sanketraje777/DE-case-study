from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from datetime import timedelta
from common.tasks import (
    get_variables, generate_limit_offset_task,
    zip_dicts, run_queries)
from common.config import (
    POSTGRES_CONN, CORE_PARALLELISM, DAG_CONCURRENCY, DEFAULT_VAR_DICT,
    DEFAULT_LZ_CONFIG, DEFAULT_ODS_CONFIG)
from etl_ods.data import sql

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
    max_active_runs=1,
    concurrency=DAG_CONCURRENCY,
    catchup=False,
    tags=["etl", "ods"],
) as dag:
    # 1. Fetch Airflow variables and constants
    variables = get_variables.override(task_id="get_airflow_variables")(
        keys=["chunk_size", "page_size", "batch_size", "text_clean_regex"])
    db_pool = "db_pool_max"

    target = Variable.get("ods_config", deserialize_json=True, default_var=DEFAULT_ODS_CONFIG)
    source = Variable.get("lz_config", deserialize_json=True, default_var=DEFAULT_LZ_CONFIG)
    target["tables"] = [t.strip() for t in target["tables"].split(",")]
    source["tables"] = [t.strip() for t in source["tables"].split(",")]
    # add mapping to source[tables] for corresponding target table product
    source["tables"].append(source["tables"][0])    # lz.salesorderitem -> ods.product

    init_ods = EmptyOperator(task_id=f"init_{target['schema']}")
    end_ods = EmptyOperator(task_id=f"end_{target['schema']}")

    # 2. Dictionary to hold instantiated task references for later wiring
    tasks = {}

    # 3. Loop over target_tables to create TaskGroups dynamically
    for idx, tgt in enumerate(target['tables']):
        src = source['tables'][idx]
        if tgt not in sql:
            raise ValueError(f"No SQL templates defined for target table '{tgt}'")
        sql_defs = sql[tgt]

        # 3a) Format each SQL/template path
        prepare_sql = sql_defs["prepare"].format(
            target_schema=target['schema'], target_table=tgt
        ).strip()

        process_sql = sql_defs["process"].format(
            target_schema=target['schema'], target_table=tgt,
            target_sys_folder=target['sys_folder']
        ).strip()

        process_fk_sql = (
            sql_defs["process_fk"].format(
                target_schema=target['schema'], target_table=tgt,
                target_sys_folder=target['sys_folder']
            ).strip()
            if sql_defs["process_fk"] else None
        )

        add_constraints_sql = sql_defs["add_constraints"].format(
            target_schema=target['schema'], target_table=tgt,
            target_sys_folder=target['sys_folder']
        ).strip()

        dq_check_sql = sql_defs["dq_check"].format(
            target_schema=target['schema'], target_table=tgt,
            target_sys_folder=target['sys_folder']
        ).strip()

        # 3b) Create TaskGroup for this table
        with TaskGroup(group_id=f"tg_{tgt}") as tg:

            # ─── Truncate / Count / Prepare Params ─────────────────────────────
            with TaskGroup(group_id=f"tg_preprocess_{tgt}") as tg_pre:

                # 3c) Truncate + drop constraints/indexes
                truncate_target = PostgresOperator(
                    task_id=f"truncate_target_{tgt}",
                    postgres_conn_id=POSTGRES_CONN["conn_id"],
                    sql=prepare_sql
                )

                # 3d) Zip parameters for .expand()
                prepare_params = zip_dicts.override(task_id=f"prepare_{tgt}_parameters")(
                    DEFAULT_VAR_DICT, variables,
                    dict(source_schema=source['schema'], source_table=src),
                    dict(target_schema=target['schema'], target_table=tgt),
                    prefixes=["var_", "var_", "", ""],
                )

                # 3e) internal dependency: variables -> params_task
                variables >> prepare_params

            # ─── Main Processing ───────────────────────────────────────────────
            with TaskGroup(group_id=f"tg_process_{tgt}") as tg_proc:

                # 3f) run main process SQL
                process = run_queries.override(
                    task_id=f"process_{tgt}",
                    pool=db_pool,
                    max_active_tis_per_dag=CORE_PARALLELISM,
                ).partial(
                    hook_class=PostgresHook,
                    hook_kwargs={"postgres_conn_id": POSTGRES_CONN["conn_id"]},
                    sql=process_sql
                ).expand(parameters=prepare_params)

                # 3g) run FK‐deduplication (if applicable)
                if process_fk_sql:
                    process_fk = run_queries.override(
                        task_id=f"process_fk_{tgt}",
                        pool=db_pool,
                        max_active_tis_per_dag=CORE_PARALLELISM,
                    ).partial(
                        hook_class=PostgresHook,
                        hook_kwargs={"postgres_conn_id": POSTGRES_CONN["conn_id"]},
                        sql=process_fk_sql,
                    ).expand(parameters=prepare_params)
                else:
                    process_fk = EmptyOperator(task_id=f"process_fk_{tgt}")

                # 3h) internal dependency: process_task -> process_fk_task
                process >> process_fk

            # ─── Post‐processing: Constraints, DQ, Mark Loaded ───────────────
            with TaskGroup(group_id=f"tg_postprocess_{tgt}") as tg_post:

                # 3i) add constraints to the target table
                add_constraints = run_queries.override(task_id=f"add_constraints_{tgt}")(
                    hook_class=PostgresHook,
                    hook_kwargs={"postgres_conn_id": POSTGRES_CONN["conn_id"]},
                    handler="run",
                    sql=add_constraints_sql,
                    parameters=dict(target_schema=target['schema'], target_table=tgt)
                )

                # 3j) perform data quality check
                dq_check = run_queries.override(task_id=f"data_quality_check_{tgt}")(
                    hook_class=PostgresHook,
                    hook_kwargs={"postgres_conn_id": POSTGRES_CONN["conn_id"]},
                    handler="get_records",
                    log_enabled=True,
                    sql=dq_check_sql,
                    parameters=dict(target_schema=target['schema'], target_table=tgt)
                )

                # 3k) mark load audit for this table
                mark_loaded = run_queries.override(task_id=f"mark_last_loaded_{tgt}")(
                    hook_class=PostgresHook,
                    hook_kwargs={"postgres_conn_id": POSTGRES_CONN["conn_id"]},
                    handler="run",
                    sql="/opt/airflow/sql/postgres_data_warehouse/insert_load_audit.sql",
                    parameters=(f"{target['schema']}.{tgt}", str(dq_check))
                )

                # 3l) wire post‐processing
                process_fk >> add_constraints >> dq_check >> mark_loaded

            # ─── Chain the three subgroups ──────────────────────────────────
            tg_pre >> tg_proc >> tg_post

            # 3m) Save references for cross‐table wiring
            tasks[tgt] = {
                "task_group": tg,
                "tg_pre": tg_pre,
                "tg_proc": tg_proc,
                "tg_post": tg_post,
                "truncate": truncate_target,
                "params": prepare_params,
                "process": process,
                "process_fk": process_fk,
                "add_constraints": add_constraints,
                "dq_check": dq_check,
                "mark_loaded": mark_loaded,
            }

    trigger_etl_dm = TriggerDagRunOperator(
        task_id="trigger_etl_dm_dag", trigger_dag_id="etl_dm"
    )

    # 5. DAG‐level wiring
    all_tgs = [refs["task_group"] for refs in tasks.values()]
    init_ods >> variables >> all_tgs >> end_ods >> trigger_etl_dm

    # 6. Cross‐table dependencies (referential order)
    # 6a) Truncate in proper sequence:
    #   salesorderitem must truncate before product and salesorder
    tasks["salesorderitem"]["truncate"] >> (tasks["product"]["truncate"], 
                                                tasks["salesorder"]["truncate"])
    tasks["salesorder"]["truncate"] >> tasks["customer"]["truncate"]

    # 6b) Foreign‐key deduplication chaining:
    #   add_constraints_customer -> process_fk_salesorder
    tasks["customer"]["add_constraints"] >> tasks["salesorder"]["process_fk"]

    # 6c) add_constraints_salesorder & add_constraints_product -> process_fk_salesorderitem
    (tasks["salesorder"]["add_constraints"],
     tasks["product"]["add_constraints"],
    ) >> tasks["salesorderitem"]["process_fk"]

# 7. Set trigger_rule="none_failed" for all tasks
for task in dag.tasks:
    task.trigger_rule = "none_failed"
