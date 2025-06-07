import math
import logging
from datetime import timedelta

def generate_datetime_chunks(min_dt, max_dt, type_, no_of_chunks, chunk_size, min_chunk_minutes):
    chunks = []
    if min_dt is None or max_dt is None:
        return chunks
    if type_ == "fixed":
        total_minutes = (max_dt - min_dt).total_seconds() // 60
        if total_minutes <= min_chunk_minutes:
            chunks.append({
                "start_val": min_dt,
                "end_val": max_dt
            })
            return chunks
        chunk_size_minutes = math.ceil(total_minutes / no_of_chunks)
    elif type_ == "dynamic":
        chunk_size_minutes = chunk_size
    else:
        raise ValueError(f"Unknown type: {type_}")
    current_start = min_dt
    while current_start < max_dt:
        current_end = min(current_start + timedelta(minutes=chunk_size_minutes), max_dt)
        chunks.append({
            "start_val": current_start,
            "end_val": current_end
        })
        current_start = current_end
    logging.info(f"Generated datetime chunks: {chunks}")
    return chunks

def generate_num_chunks(min_num, max_num, type_, no_of_chunks, chunk_size, min_chunk_size, is_integer=True):
    chunks = []
    if min_num is None or max_num is None:
        return chunks
    if type_ == "fixed":
        total_size = max_num - min_num
        if total_size <= min_chunk_size:
            chunks.append({
                "start_val": round(min_num) if is_integer else min_num,
                "end_val": round(max_num) if is_integer else max_num
            })
            return chunks
        chunk_size_val = total_size / no_of_chunks
        if is_integer:
            chunk_size_val = math.ceil(chunk_size_val)
    elif type_ == "dynamic":
        chunk_size_val = chunk_size
    else:
        raise ValueError(f"Unknown type: {type_}")
    current_start = min_num
    while current_start < max_num:
        current_end = min(current_start + chunk_size_val, max_num)
        chunks.append({
            "start_val": round(current_start) if is_integer else current_start,
            "end_val": round(current_end) if is_integer else current_end
        })
        current_start = current_end
    logging.info(f"Generated numeric chunks: {chunks}")
    return chunks

def generate_limit_offset_chunks(count, type_, no_of_chunks, chunk_size, min_chunk_size):
    chunks = []
    if not count:
        return chunks
    if type_ == "fixed":
        if count <= min_chunk_size:
            chunks.append({
                "offset": 0,
                "limit": count
            })
            return chunks
        chunk_size_val = math.ceil(count / no_of_chunks)
    elif type_ == "dynamic":
        chunk_size_val = chunk_size
    else:
        raise ValueError(f"Unknown type: {type_}")
    for current_offset in range(0, count, chunk_size_val):
        chunks.append({
            "offset": current_offset,
            "limit": min(chunk_size_val, count-current_offset)
        })
    logging.info(f"Generated numeric chunks: {chunks}")
    return chunks
