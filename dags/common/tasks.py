from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
from common.utils import fetch_min_max_dt, fetch_min_max_num, fetch_count, \
    insert_in_batches_copy_expert, fetch_in_chunks_cursor, has_psycopg2_placeholders
from common.chunking import generate_datetime_chunks, generate_num_chunks, \
    generate_limit_offset_chunks
from jinja2 import Template
from pathlib import Path
import os
import logging
import sqlparse
import pandas as pd

CHUNK_SIZE = int(Variable.get("chunk_size", default_var=10000))
MIN_CHUNK_SIZE = int(Variable.get("min_chunk_size", default_var=1000))
MIN_CHUNK_MINUTES = int(Variable.get("min_chunk_minutes", default_var=6*60))
PAGE_SIZE = int(Variable.get("page_size", default_var=10000))
BATCH_SIZE = int(Variable.get("batch_size", default_var=1000))
DATETIME_INTERVAL = int(Variable.get("datetime_interval", default_var=1))
TESTING_TS = Variable.get("testing_ts", default_var="false").lower() == "true"
CORE_PARALLELISM = int(os.environ.get("AIRFLOW__CORE__PARALLELISM", 16))
MYSQL_CONN = Variable.get("mysql_conn", deserialize_json=True, default_var={})
POSTGRES_CONN = Variable.get("postgres_conn", deserialize_json=True, default_var={})
MYSQL_CONN.setdefault("conn_id", "mysql_default")
POSTGRES_CONN.setdefault("conn_id", "postgres_default")

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
    if query:
        # Render using Airflow's Jinja template helper
        query = Template(query).render(parameters or {})

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
    if query:
        # Render using Airflow's Jinja template helper
        query = Template(query).render(parameters or {})
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
    if query:
        # Render using Airflow's Jinja template helper
        query = Template(query).render(parameters or {})
    count = fetch_count(count, query, parameters, hook_class, hook_kwargs)
    if not chunk_size:
        chunk_size = int(Variable.get("chunk_size", default_var=CHUNK_SIZE))
    if not min_chunk_size:
        min_chunk_size = int(Variable.get("min_chunk_size", default_var=MIN_CHUNK_SIZE))
    return generate_limit_offset_chunks(count, type_, no_of_chunks, chunk_size, min_chunk_size)

@task()
def process_chunk(query, target_table, source_hook_class = MySqlHook, target_hook_class = PostgresHook, 
                  parameters = None, page_size = None, batch_size = None, insert_handler = insert_in_batches_copy_expert,
                  process_handler = None, source_hook_kwargs = {'mysql_conn_id':MYSQL_CONN['conn_id']}, 
                  target_hook_kwargs = {'postgres_conn_id':POSTGRES_CONN['conn_id']}):
    # Instantiate hooks
    source_hook = source_hook_class(**source_hook_kwargs)
    target_hook = target_hook_class(**target_hook_kwargs)
    # Render using Airflow's Jinja template helper
    query = Template(query).render(parameters or {})
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
    return f"Inserted {query} rows for params {parameters}"

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
    """
    This task function takes multiple arguments, which can be either a single dict or a list of dicts,
    and returns a list of merged dictionaries along with optional prefixes for each argument.
    """
    prefixes = kwargs.get("prefixes", None)

    # First, convert single dicts to repeated lists
    processed_args = []
    max_len = 0

    for arg in dicts:
        if isinstance(arg, dict):
            processed_args.append([arg])
            max_len = max(max_len, 1)
        elif isinstance(arg, list):
            processed_args.append(arg)
            max_len = max(max_len, len(arg))
        else:
            raise TypeError("Each argument must be either a dict or a list of dicts.")

    # Repeat single dicts to match the max_len
    for idx, arg in enumerate(processed_args):
        if len(arg) == 1:
            processed_args[idx] = arg * max_len
        elif len(arg) != max_len:
            raise ValueError(f"Argument {idx} has mismatched length {len(arg)} (expected {max_len}).")

    # Validate and process prefixes
    if prefixes is not None:
        if isinstance(prefixes, str):
            prefixes = [prefixes] * len(dicts)
        if not isinstance(prefixes, list) or len(prefixes) != len(dicts):
            raise ValueError(
                f"prefixes must be a list of strings with length {len(dicts)}."
            )
    else:
        prefixes = [""] * len(dicts)  # No prefix if not provided

    # Zip and merge dictionaries with prefixes
    result = []
    for group in zip(*processed_args):
        merged_dict = {}
        for idx, d in enumerate(group):
            prefix = prefixes[idx]
            for k, v in d.items():
                merged_dict[f"{prefix}{k}"] = v
        result.append(merged_dict)

    return result

@task()
def get_variables(keys=None, deserialize_json=True):
    """
    This task function retrieves all Airflow variables and returns them as a dictionary.
    """
    if keys:
        return {key: Variable.get(key, deserialize_json=deserialize_json) for key in keys}
    else:
        return Variable.get_all()
