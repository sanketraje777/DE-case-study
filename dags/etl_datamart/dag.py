from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from datetime import timedelta
from common.tasks import get_variables, zip_dicts, run_queries
from common.config import (POSTGRES_CONN, CORE_PARALLELISM, 
                            DAG_CONCURRENCY, DEFAULT_VAR_DICT, 
                            DEFAULT_DM_CONFIG, DEFAULT_ODS_CONFIG)
from etl_datamart.data import sql
import pendulum

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": pendulum.today("UTC"),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_dm",
    default_args=DEFAULT_ARGS,
    schedule=None,
    max_active_runs=1,
    max_active_tasks=DAG_CONCURRENCY,
    catchup=False,
    tags=["etl", "dm"],
) as dag:

    # 1. Fetch variables & constants
    variables = get_variables.override(task_id="get_airflow_variables")(
        keys="text_clean_regex")
    db_pool = "db_pool_max"

    target = Variable.get("dm_config", deserialize_json=True, default_var=DEFAULT_DM_CONFIG)
    source = Variable.get("ods_config", deserialize_json=True, default_var=DEFAULT_ODS_CONFIG)

    target["tables"] = [t.strip() for t in target["tables"].split(",")]
    source["tables"] = [t.strip() for t in source["tables"].split(",")]
    # all source tables mapped to the first target table
    source["tables"] = [source["tables"]]

    init_dm = EmptyOperator(task_id=f"init_{target['schema']}")
    end_dm = EmptyOperator(task_id=f"end_{target['schema']}")

    # 2. Dictionary to hold instantiated task references for cross‐table wiring
    tasks = {}

    # 3. Loop over each target table, create a TaskGroup, and store refs
    for idx, tgt in enumerate(target["tables"]):
        # assume one‐to‐one mapping between target["tables"] and source["tables"]
        src = source["tables"][idx]
        if tgt not in sql:
            raise ValueError(f"No SQL templates defined for target table '{tgt}'")
        sql_defs = sql[tgt]

        # Format inline or path templates with actual values
        prepare_sql = sql_defs["prepare"].format(
            target_schema=target['schema'], target_table=tgt
        ).strip()

        process_sql = sql_defs["process"].format(
            target_sys_folder=target['sys_folder'], 
            target_schema=target['schema'], target_table=tgt
        ).strip()

        process_fk_sql = (
            sql_defs["process_fk"].format(
                target_sys_folder=target['sys_folder'], 
                target_schema=target['schema'], target_table=tgt
            ).strip()
            if sql_defs["process_fk"]
            else None
        )

        add_constraints_sql = sql_defs["add_constraints"].format(
            target_sys_folder=target['sys_folder'], 
            target_schema=target['schema'], target_table=tgt
        ).strip()

        dq_check_sql = sql_defs["dq_check"].format(
            target_sys_folder=target['sys_folder'], 
            target_schema=target['schema'], target_table=tgt
        ).strip()

        mark_loaded_sql = sql_defs["mark_loaded"].format(
            target_sys_folder=target['sys_folder']).strip()

        # 3a) Build TaskGroup for this table
        with TaskGroup(group_id=f"tg_{tgt}") as tg:

            # ─── Preprocess: Truncate + Prepare Params ───────────────────────────
            with TaskGroup(group_id=f"tg_preprocess_{tgt}") as tg_pre:

                # 3b) Truncate & drop indexes/constraints
                truncate_target = SQLExecuteQueryOperator(
                    task_id=f"truncate_target_{tgt}",
                    conn_id=POSTGRES_CONN["conn_id"],
                    sql=prepare_sql
                )

                # 3c) Zip parameters (no chunking here since DM logic reads entire ODS if applicable)
                prepare_params = zip_dicts.override(task_id=f"prepare_{tgt}_parameters")(
                    dict(text_clean_regex = DEFAULT_VAR_DICT['text_clean_regex']), 
                    variables,
                    # pass in ODS->DM mapping info:
                    dict(source_schema=source['schema'], source_table=src),
                    dict(target_schema=target['schema'], target_table=tgt),
                    prefixes=["var_", "var_", "", ""]
                )

                # 3d) No chunk dependency in this DAG—only zip on variables
                variables >> prepare_params

            # ─── Main Process: Run SQL + optional FK step ────────────────────────
            with TaskGroup(group_id=f"tg_process_{tgt}") as tg_proc:

                # 3e) Run main "load" SQL
                process = run_queries.override(
                    task_id=f"process_{tgt}",
                    pool=db_pool,
                    max_active_tis_per_dag=CORE_PARALLELISM
                ).partial(
                    hook_class=PostgresHook,
                    hook_kwargs={"postgres_conn_id": POSTGRES_CONN["conn_id"]},
                    sql=process_sql
                ).expand(parameters=prepare_params)

                # 3f) Optional FK step
                if process_fk_sql:
                    process_fk = run_queries.override(
                        task_id=f"process_fk_{tgt}",
                        pool=db_pool,
                        max_active_tis_per_dag=CORE_PARALLELISM
                    ).partial(
                        hook_class=PostgresHook,
                        hook_kwargs={"postgres_conn_id": POSTGRES_CONN["conn_id"]},
                        sql=process_fk_sql
                    ).expand(parameters=prepare_params)
                else:
                    process_fk = EmptyOperator(task_id=f"process_fk_{tgt}")

                # ensure FK runs after main process
                process >> process_fk

            # ─── Postprocess: Add Constraints, DQ Check, Mark Loaded ─────────────
            with TaskGroup(group_id=f"tg_postprocess_{tgt}") as tg_post:

                # 3g) Add constraints on target table
                add_constraints = run_queries.override(task_id=f"add_constraints_{tgt}")(
                    hook_class=PostgresHook,
                    hook_kwargs={"postgres_conn_id": POSTGRES_CONN["conn_id"]},
                    handler="run",
                    sql=add_constraints_sql,
                    parameters=dict(target_schema=target['schema'], target_table=tgt)
                )

                # 3h) Data quality check on target table
                dq_check = run_queries.override(task_id=f"data_quality_check_{tgt}")(
                    hook_class=PostgresHook,
                    hook_kwargs={"postgres_conn_id": POSTGRES_CONN["conn_id"]},
                    handler="get_records",
                    log_enabled=True,
                    sql=dq_check_sql,
                    parameters=dict(target_schema=target['schema'], target_table=tgt)
                )

                # 3i) Mark load audit for this target table
                mark_loaded = run_queries.override(task_id=f"mark_last_loaded_{tgt}")(
                    hook_class=PostgresHook,
                    hook_kwargs={"postgres_conn_id": POSTGRES_CONN["conn_id"]},
                    handler="run",
                    sql=mark_loaded_sql,
                    parameters=(f"{target['schema']}.{tgt}", str(dq_check))
                )

                # 3j) wire postprocess dependencies
                process_fk >> add_constraints >> \
                    dq_check >> mark_loaded

            # ─── Chain subgroups ───────────────────────────────────────────────
            tg_pre >> tg_proc >> tg_post

            # 3k) Store references for cross‐table wiring
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
                "mark_loaded": mark_loaded
            }

    # 4. End of DAG–level downstream
    # (no additional cross‐table dependencies in this case)
    all_tgs = [refs["task_group"] for refs in tasks.values()]
    init_dm >> variables >> all_tgs >> end_dm

# ─── Safely set trigger_rule on each operator ─────────────────────────────────
# 5. Ensure that no downstream task runs if any upstream has failed
for task in dag.tasks:
    try:
        task.trigger_rule = "none_failed"
    except (AttributeError, TypeError):
        pass
