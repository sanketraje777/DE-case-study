from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
from common.utils import (
    fetch_min_max_dt, fetch_min_max_num, fetch_count, 
    insert_in_batches_copy_expert, fetch_in_chunks_cursor, 
    has_psycopg2_placeholders, zip_dictionaries, fetch_all, 
    insert_all_copy_expert)
from common.chunking import (generate_datetime_chunks, generate_num_chunks, 
    generate_limit_offset_chunks)
from common.config import (MYSQL_CONN, CHUNK_SIZE, MIN_CHUNK_MINUTES, 
    MIN_CHUNK_SIZE, PAGE_SIZE, BATCH_SIZE, POSTGRES_CONN)
from jinja2 import Template
from pathlib import Path
from typing import Any
import logging
import sqlparse
import pandas as pd

@task()
def get_load_range(datetime_interval = None):
    if not datetime_interval:
        datetime_interval = int(Variable.get("datetime_interval", default_var=1))
    TESTING_TS = Variable.get("testing_ts", default_var="false").lower() == "true"
    current_ts = get_current_context()['execution_date']
    last_ts = (current_ts - timedelta(days=datetime_interval))
    if TESTING_TS:
        last_ts = Variable.get(
            "last_loaded_ts",
            default_var=last_ts.strftime('%Y-%m-%d %H:%M:%S')
        )
        last_ts = datetime.strptime(last_ts, "%Y-%m-%d %H:%M:%S")
        current_ts = last_ts + timedelta(days=datetime_interval)
    return {"start_time": last_ts, "end_time": current_ts}

@task()
def generate_datetime_chunks_task(
    type_: str = "fixed", no_of_chunks: int = None, chunk_size: int = None, 
    min_chunk_minutes: int = None, range_time: dict = None, query = None, parameters = None, 
    hook_class = MySqlHook, hook_kwargs = {'mysql_conn_id': MYSQL_CONN['conn_id']}):
    """
    Generalized chunk generator that handles both 'fixed' and 'dynamic' types.
    :param type_: 'fixed' or 'dynamic'
    :param no_of_chunks: Used in 'fixed' type to determine chunk count
    :param chunk_size: Used in 'dynamic' type for fixed chunk size in minutes
    :param min_chunk_minutes: Minimum chunk size in minutes for 'fixed' type
    :return: List of chunks with start and end datetimes
    """
    if query and isinstance(parameters, dict):
        # Render using Airflow's Jinja template helper
        query = Template(query).render(parameters)

    min_dt, max_dt = fetch_min_max_dt(range_time, query, parameters, hook_class, hook_kwargs)
    if not chunk_size:
        chunk_size = int(Variable.get("chunk_size", default_var=CHUNK_SIZE))
    if not min_chunk_minutes:
        min_chunk_minutes = int(Variable.get("min_chunk_minutes", default_var=MIN_CHUNK_MINUTES))
    return generate_datetime_chunks(min_dt, max_dt, type_, no_of_chunks, chunk_size, min_chunk_minutes)

@task()
def generate_num_chunks_task(
    type_: str = "fixed", no_of_chunks: int = None, chunk_size: int = None, 
    min_chunk_size: int = None, is_integer = True, range_val: dict = None, 
    query = None, parameters = None, hook_class = MySqlHook, hook_kwargs = {'mysql_conn_id': MYSQL_CONN['conn_id']}):
    """
    Generalized chunk generator for numeric values (int or float).
    :param type_: 'fixed' or 'dynamic'
    :param no_of_chunks: Number of chunks if type is 'fixed'
    :param chunk_size: Size of each chunk for 'dynamic'
    :param min_chunk_size: Minimum chunk size for 'fixed'
    :param is_integer: return chunks as rounded integers instead of floats
    :return: List of chunks with start and end numeric values
    """
    if query and isinstance(parameters, dict):
        # Render using Airflow's Jinja template helper
        query = Template(query).render(parameters)
    min_num, max_num = fetch_min_max_num(range_val, query, parameters, is_integer, hook_class, hook_kwargs)
    if not chunk_size:
        chunk_size = int(Variable.get("chunk_size", default_var=CHUNK_SIZE))
    if not min_chunk_size:
        min_chunk_size = int(Variable.get("min_chunk_size", default_var=MIN_CHUNK_SIZE))
    return generate_num_chunks(min_num, max_num, type_, no_of_chunks, chunk_size, min_chunk_size, is_integer)

@task()
def generate_limit_offset_task(
    type_: str = "fixed", no_of_chunks: int = None, chunk_size: int = None, 
    min_chunk_size: int = None, count: int = None, 
    query = None, parameters = None, hook_class = MySqlHook, hook_kwargs = {'mysql_conn_id': MYSQL_CONN['conn_id']}):
    """
    Generalized chunk generator for numeric values (int or float).
    :param type_: 'fixed' or 'dynamic'
    :param no_of_chunks: Number of chunks if type is 'fixed'
    :param chunk_size: Size of each chunk for 'dynamic'
    :param min_chunk_size: Minimum chunk size for 'fixed'
    :return: List of chunks with limit and offset numeric values
    """
    if query and isinstance(parameters, dict):
        # Render using Airflow's Jinja template helper
        query = Template(query).render(parameters)
    count = fetch_count(count, query, parameters, hook_class, hook_kwargs)
    if not chunk_size:
        chunk_size = int(Variable.get("chunk_size", default_var=CHUNK_SIZE))
    if not min_chunk_size:
        min_chunk_size = int(Variable.get("min_chunk_size", default_var=MIN_CHUNK_SIZE))
    return generate_limit_offset_chunks(count, type_, no_of_chunks, chunk_size, min_chunk_size)

# helper function for task function process_data()
def process_chunk(query, target_table, source_hook, target_hook, 
                  parameters = None, page_size = None, batch_size = None, 
                  insert_handler = insert_in_batches_copy_expert, process_handler = None):
    if not page_size:
        page_size = int(Variable.get("page_size", default_var=PAGE_SIZE))
    if not batch_size:
        batch_size = int(Variable.get("batch_size", default_var=BATCH_SIZE))
    for df in fetch_in_chunks_cursor(source_hook, query, parameters, page_size):
        if process_handler:
            df = process_handler(df)
        # replaces NaT with None, and NaN with None before writing data
        df = df.replace({pd.NaT: None}).where(pd.notna(df), None)
        insert_handler(target_hook, target_table, df, batch_size)

# helper function for task function process_data()
def process_all(query, target_table, source_hook, target_hook, 
                  parameters = None, batch_size = None, 
                  insert_handler = insert_all_copy_expert, process_handler = None):
    if not batch_size:
        batch_size = int(Variable.get("batch_size", default_var=BATCH_SIZE))
    df = fetch_all(source_hook, query=query, params=parameters)
    if process_handler:
        df = process_handler(df)
    # replaces NaT with None, and NaN with None before writing data
    df = df.replace({pd.NaT: None}).where(pd.notna(df), None)
    insert_handler(target_hook, target_table, df, batch_size)

@task()
def process_data(query, target_table, type_ = "chunk", source_hook_class = MySqlHook, target_hook_class = PostgresHook, 
                  parameters = None, page_size = None, batch_size = None, insert_handler = insert_in_batches_copy_expert,
                  process_handler = None, source_hook_kwargs = {'mysql_conn_id':MYSQL_CONN['conn_id']}, 
                  target_hook_kwargs = {'postgres_conn_id':POSTGRES_CONN['conn_id']}):
    # Instantiate hooks
    source_hook = source_hook_class(**source_hook_kwargs)
    target_hook = target_hook_class(**target_hook_kwargs)
    # Render using Airflow's Jinja template helper
    if isinstance(parameters, dict):
        query = Template(query).render(parameters)
    if type_ == "chunk":
        process_chunk(query, target_table, source_hook, target_hook, 
                  parameters, page_size, batch_size, 
                  insert_handler, process_handler)
    elif type_ == "all":
        process_all(query, target_table, source_hook, target_hook, 
                  parameters, batch_size, 
                  insert_handler, process_handler)
    else:
        raise ValueError("`type_` must be either 'chunk' or 'all'.")
    return f"Inserted {query} rows for parameters {parameters}"

@task()
def update_last_loaded_ts(load_range):
    if not Variable.get("testing_ts", default_var="false").lower() == "true":
        Variable.set("last_loaded_ts", load_range["end_time"])

@task()
def run_queries_with_task_params(hook_class, sql, parameters = None, handler:str = "run", hook_kwargs = {}):
    # Render using Airflow's Jinja template helper
    sql = Template(sql).render(parameters or {})

    # Instantiate hook
    hook = hook_class(**hook_kwargs)
    # Get handler (e.g., run, get_records)
    if not hasattr(hook, handler):
        raise AttributeError(f"Hook {hook_class.__name__} has no handler '{handler}'")
    handler = getattr(hook, handler)

    return handler(sql=sql, parameters=parameters)

@task()
def run_queries(hook_class, sql, parameters=None, handler: str="run", 
                hook_kwargs={}, log_enabled=False, raise_error=False):
    """
    Airflow task to:
    Accept hook class (PostgresHook, MySqlHook, etc.)
    Accept handler name (like 'run', 'get_records', etc.)
    Accept sql (str, list of str, or .sql file paths)
    Accept parameters (dict or sequence(list/tuple))
    Accept hook_kwargs to create hook at runtime
    Render Jinja templates using Template().render
    Support multiple statements in .sql files
    Log results conditionally
    Return list of all results
    Raises error is raise_error = True
    """
    # Instantiate hook
    hook = hook_class(**hook_kwargs)

    # Get handler (e.g., run, get_records)
    if not hasattr(hook, handler):
        raise AttributeError(f"Hook {hook_class.__name__} has no handler '{handler}'")
    handler = getattr(hook, handler)

    # Normalize sql to list
    if isinstance(sql, str):
        sql = [sql]

    all_results = []

    for single_sql in sql:
        if single_sql.endswith(".sql") and Path(single_sql).exists():
            logging.info("Loading SQL from file: %s", single_sql)
            # Read .sql file content
            single_sql = Path(single_sql).read_text()

        # Render using Airflow's Jinja template helper
        if isinstance(parameters, dict):
            single_sql = Template(single_sql).render(parameters)
            logging.info(f"processed query: {single_sql}, parameters: {parameters}")

        # Use sqlparse to split into individual statements
        statements = [
            stmt.strip()
            for stmt in sqlparse.split(single_sql)
            if stmt.strip()
        ]
        logging.info("Split into %d individual SQL statements", len(statements))
    
        # Execute each statement separately
        for stmt in statements:
            logging.info("Executing:\n%s\n with parameters: %s", stmt, parameters)
            try:
                if has_psycopg2_placeholders(stmt):
                    result = handler(sql=stmt, parameters=parameters)
                else:
                    result = handler(sql=stmt)
                if log_enabled:
                    logging.info("Result: %s", result)
                all_results.append(result)
            except Exception as e:
                logging.error("Error executing statement:\n%s\nError:\n%s", 
                              stmt, str(e), exc_info=True)    # exc_info = True prints the full stack trace
                all_results.append(None)
                if raise_error:
                    raise

    return all_results

@task()
def zip_dicts(*dicts, **kwargs):
    prefixes = kwargs.get("prefixes", None)
    return zip_dictionaries(*dicts, prefixes=prefixes)

@task()
def get_variables(keys: list[str] | str | None = None, deserialize_json: bool = False) -> dict[str, Any]:
    """
    Retrieve Airflow Variables and return them as a dict.
    
    - If `keys` is passed (a list of variable names), only those will be fetched.
    - If `keys` is None (the default), fetch ALL variables, 
      applying `deserialize_json` to each value.
    """
    variables: dict[str, Any] = {}
    
    if keys:
        if isinstance(keys, str):
            keys = [keys]
        # Fetch only the specified keys:
        for key in keys:
            # default_var=None → means “if the key doesn’t exist, get back None”
            value = Variable.get(key, deserialize_json=deserialize_json, default_var=None)
            if value is not None:
                variables[key] = value
        return variables
        # return {key: Variable.get(key, deserialize_json=deserialize_json) for key in keys}
    
    # If no specific keys are given, fetch ALL variable names:
    all_vars = Variable.get_all()  # returns a dict[str, str], no JSON deserialization here
    for key, _ in all_vars.items():
        # Now re‐fetch each one through Variable.get so that JSON can be deserialized
        # (note: default_var=None won’t be used here because key must exist if get_all() returned it)
        value = Variable.get(key, deserialize_json=deserialize_json)
        variables[key] = value
    
    return variables
