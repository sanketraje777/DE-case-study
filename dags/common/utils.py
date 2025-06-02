import sys
import io
import re
import logging
import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook
import MySQLdb.cursors
from psycopg2.extras import execute_values
from datetime import timedelta

def fetch_min_max_dt(range_time: dict = None, query=None, params=None, 
                     hook_class = MySqlHook, hook_kwargs = {'mysql_conn_id': 'mysql_default'}):
    min_dt = range_time.get("start_time") if range_time else None
    max_dt = range_time.get("end_time") if range_time else None
    if query:
        hook = hook_class(**hook_kwargs)
        result = hook.get_first(query, parameters=params)
        min_dt, max_dt = result
    if min_dt is None or max_dt is None:
        return None, None
    if query:
        max_dt += timedelta(seconds=1)
    return min_dt, max_dt

def fetch_min_max_num(range_val: dict = None, query=None, params=None, is_integer=True, 
                     hook_class = MySqlHook, hook_kwargs = {'mysql_conn_id': 'mysql_default'}):
    min_num = range_val.get("start_val") if range_val else None
    max_num = range_val.get("end_val") if range_val else None
    if query:
        hook = hook_class(**hook_kwargs)
        result = hook.get_first(query, parameters=params)
        min_num, max_num = result
    if min_num is None or max_num is None:
        return None, None
    min_num = float(min_num)
    max_num = float(max_num)
    if query:
        max_num += 1 if is_integer else sys.float_info.epsilon
    return min_num, max_num

def fetch_count(count: int = None, query=None, params=None, 
                hook_class = MySqlHook, hook_kwargs = {'mysql_conn_id': 'mysql_default'}):
    if query:
        hook = hook_class(**hook_kwargs)
        result, = hook.get_first(query, parameters=params)
        count = result
    if count is None:
        return None
    return int(count)

def insert_in_batches(db_hook, table_name, df, batch_size):
    cols = df.columns.tolist()
    for i in range(0, len(df), batch_size):
        try:
            db_hook.insert_rows(
                table=table_name,
                # use list data generator using itertuples()
                rows=(tuple(row) for row in df.iloc[i:i+batch_size].itertuples(index=False, name=None)),
                target_fields=cols,
                commit_every=batch_size
            )
            logging.info(f"Inserted batch {i//batch_size+1} into {table_name}")
        except Exception as e:
            logging.error(f"Error inserting batch into {table_name}: {e}", exc_info=True)
            raise

def insert_in_batches_execute_values(db_hook, table_name, df, batch_size):
    cols = df.columns.tolist()
    col_names = ", ".join([f'"{col}"' for col in cols])
    # Build the SQL statement
    sql = f"INSERT INTO {table_name} ({col_names}) VALUES %s"
    data_gen = (tuple(row) for row in df.itertuples(index=False, name=None))
    conn = db_hook.get_conn()
    try:
        with conn.cursor() as cursor:
            execute_values(cursor, sql, data_gen, page_size=batch_size)
        conn.commit()
        logging.info(f"Inserted batch of {len(df)} rows into {table_name}")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error inserting into {table_name}: {e}", exc_info=True)
        raise
    finally:
        conn.close()

def insert_in_batches_copy_expert(db_hook, table_name, df: pd.DataFrame, batch_size: int):
    """
    Use COPY for bulk insert. Works best with large datasets.
    """
    cols = df.columns.tolist()
    col_str = ', '.join(cols)
    # Build COPY SQL statement
    copy_sql = f"""
            COPY {table_name} ({col_str})
            FROM STDIN WITH (FORMAT CSV, DELIMITER E'\t', NULL '\\N')
            """    
    # Use pg_hook.get_conn() to get the connection, then a cursor
    conn = db_hook.get_conn()
    try:
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i:i+batch_size]
            # Write batch_df to a CSV buffer in memory
            buffer = io.StringIO()
            batch_df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')  # Use tab-separated values for efficiency
            buffer.seek(0)
            try:
                with conn.cursor() as cursor:
                    cursor.copy_expert(sql=copy_sql, file=buffer)
                conn.commit()
                logging.info(f"Inserted batch {i//batch_size+1} of {len(batch_df)} rows into {table_name}")
            except Exception as e:
                conn.rollback()
                logging.error(f"Error inserting batch into {table_name}: {e}", exc_info=True)
                raise
    finally:
        conn.close()

def insert_all(db_hook, table_name, df, *args, **kwargs):
    try:
        db_hook.insert_rows(
            table=table_name,
            # use list data generator using itertuples()
            rows=(tuple(row) for row in df.itertuples(index=False, name=None)),
            target_fields=df.columns.tolist()
        )
        logging.info(f"Inserted entire df of {len(df)} rows into {table_name}")
    except Exception as e:
        logging.error(f"Error inserting df into {table_name}: {e}", exc_info=True)
        raise

def insert_all_execute_values(db_hook, table_name, df, *args, **kwargs):
    insert_in_batches_execute_values(db_hook, table_name, df, None)

def insert_all_copy_expert(db_hook, table_name, df: pd.DataFrame, *args, **kwargs):
    """
    Use COPY for bulk insert. Works best with large datasets.
    """
    cols = df.columns.tolist()
    col_str = ', '.join(cols)
    # Build COPY SQL statement
    copy_sql = f"""
            COPY {table_name} ({col_str})
            FROM STDIN WITH (FORMAT CSV, DELIMITER E'\t', NULL '\\N')
            """    
    # Use pg_hook.get_conn() to get the connection, then a cursor
    conn = db_hook.get_conn()
    try:
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')  # Use tab-separated values for efficiency
        buffer.seek(0)
        with conn.cursor() as cursor:
            cursor.copy_expert(sql=copy_sql, file=buffer)
        conn.commit()
        logging.info(f"Inserted entire df of {len(df)} rows into {table_name}")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error inserting df into {table_name}: {e}", exc_info=True)
        raise
    finally:
        conn.close()

def fetch_in_chunks_cursor(db_hook, query=None, params=None, chunk_size=None, cursor_class = MySQLdb.cursors.SSDictCursor):
    conn = db_hook.get_conn()
    cursor = conn.cursor(cursor_class)  # unbuffered, dict-like rows
    cursor.execute(query, params)
    while True:
        rows = cursor.fetchmany(chunk_size)
        if not rows:
            break
        yield pd.DataFrame(rows)
    cursor.close()
    conn.close()

def fetch_all(db_hook, query=None, params=None):
    return db_hook.get_pandas_df(query, parameters=params)

PSYCOPG_REGEX = re.compile(r"%\([^)]+\)s|%s")

def has_psycopg2_placeholders(sql: str) -> bool:
    return bool(PSYCOPG_REGEX.search(sql))

def zip_dictionaries(*dicts, prefixes = None):
    """
    This task function takes multiple arguments, which can be either a single dict or a list of dicts,
    and returns a list of merged dictionaries along with optional prefixes for each argument.
    """
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

def replace_to_jinja(match):
    content = match.group(1)
    return "{{" + content.strip() + "}}"

JINJA_REGEX = re.compile(r'{\s*([^{}]+?)\s*}')

def convert_to_jinja(template: str) -> str:
    # Replace any { ... } with {{ ... }}
    # Note: We need to handle nested {} carefully if there’s any.
    # Here we’ll simply replace all { and } that aren’t already doubled.    
    # Regex to match { ... } but not {{ ... }}
    return JINJA_REGEX.sub(replace_to_jinja, template)
