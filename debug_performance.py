#!/usr/bin/env python3

import time
import pandas as pd
import numpy as np
import pyarrow as pa
from pytemporal import BitemporalTimeseriesProcessor

def create_dataset(n_records=10000):
    """Create a smaller dataset for debugging"""
    np.random.seed(42)
    
    ids = np.random.randint(1, 100, n_records) 
    fields = np.random.choice(['field_a', 'field_b'], n_records)
    prices = np.random.uniform(100, 1000, n_records)
    volumes = np.random.uniform(10, 100, n_records)
    
    base_date = pd.Timestamp('2020-01-01')
    effective_from = [base_date + pd.Timedelta(days=np.random.randint(0, 365)) for _ in range(n_records)]
    effective_to = [ef + pd.Timedelta(days=np.random.randint(30, 365)) for ef in effective_from]
    
    as_of_from = [pd.Timestamp.now(tz='UTC').tz_localize(None) for _ in range(n_records)]
    as_of_to = [pd.Timestamp.max for _ in range(n_records)]
    
    df = pd.DataFrame({
        'id': ids,
        'field': fields, 
        'mv': prices,
        'price': volumes,
        'effective_from': effective_from,
        'effective_to': effective_to,
        'as_of_from': as_of_from,
        'as_of_to': as_of_to
    })
    
    return df

def debug_performance_bottlenecks():
    print("=== PERFORMANCE DEBUG ===")
    
    # Create small datasets first
    print("Creating 10k record datasets...")
    start = time.time()
    current_state = create_dataset(8000)
    updates = create_dataset(2000)
    create_time = time.time() - start
    print(f"Dataset creation: {create_time:.3f}s")
    
    processor = BitemporalTimeseriesProcessor(
        id_columns=['id', 'field'],
        value_columns=['mv', 'price']
    )
    
    print("\n=== TIMING BREAKDOWN ===")
    
    # Time the conversion steps
    print("1. Preparing DataFrames...")
    start = time.time()
    current_prepared = processor._prepare_dataframe(current_state)
    updates_prepared = processor._prepare_dataframe(updates)
    prepare_time = time.time() - start
    print(f"   _prepare_dataframe: {prepare_time:.3f}s")
    
    print("2. Converting to Arrow...")
    start = time.time()
    current_batch = pa.RecordBatch.from_pandas(current_prepared)
    updates_batch = pa.RecordBatch.from_pandas(updates_prepared)
    arrow_time = time.time() - start
    print(f"   pandas -> Arrow: {arrow_time:.3f}s")
    
    print("3. Converting timestamps...")
    start = time.time()
    current_batch = processor._convert_timestamps_to_microseconds(current_batch)
    updates_batch = processor._convert_timestamps_to_microseconds(updates_batch)
    timestamp_time = time.time() - start
    print(f"   timestamp conversion: {timestamp_time:.3f}s")
    
    print("4. Calling Rust function...")
    start = time.time()
    from pytemporal.pytemporal import compute_changes as _compute_changes
    expire_indices, insert_batch, expired_batch = _compute_changes(
        current_batch,
        updates_batch,
        ['id', 'field'],
        ['mv', 'price'],
        '2025-08-30',
        'delta'
    )
    rust_time = time.time() - start
    print(f"   Rust processing: {rust_time:.3f}s")
    
    print("5. Converting results back...")
    start = time.time()
    
    # Process expired records
    if expired_batch:
        expired_dfs = []
        for batch in expired_batch:
            data = {}
            col_names = batch.column_names
            
            for i in range(batch.num_columns):
                col_name = col_names[i]
                column = batch.column(i)
                col_data = column.to_pylist()  # <-- POTENTIAL BOTTLENECK
                data[col_name] = col_data
            
            expired_dfs.append(pd.DataFrame(data))
        
        rows_to_expire = pd.concat(expired_dfs, ignore_index=True) if expired_dfs else pd.DataFrame()
    else:
        rows_to_expire = pd.DataFrame()
    
    # Process insert records
    if insert_batch:
        insert_dfs = []
        for batch in insert_batch:
            data = {}
            col_names = batch.column_names
            
            for i in range(batch.num_columns):
                col_name = col_names[i]
                column = batch.column(i)
                col_data = column.to_pylist()  # <-- POTENTIAL BOTTLENECK
                data[col_name] = col_data
            
            insert_dfs.append(pd.DataFrame(data))
        
        rows_to_insert = pd.concat(insert_dfs, ignore_index=True) if insert_dfs else pd.DataFrame()
    else:
        rows_to_insert = pd.DataFrame()
    
    convert_back_time = time.time() - start
    print(f"   Arrow -> pandas: {convert_back_time:.3f}s")
    
    print(f"\n=== TOTAL BREAKDOWN ===")
    print(f"Dataset creation: {create_time:.3f}s")
    print(f"Prepare DataFrames: {prepare_time:.3f}s")
    print(f"Pandas -> Arrow: {arrow_time:.3f}s") 
    print(f"Timestamp conversion: {timestamp_time:.3f}s")
    print(f"Rust processing: {rust_time:.3f}s")
    print(f"Arrow -> Pandas: {convert_back_time:.3f}s")
    
    total_overhead = prepare_time + arrow_time + timestamp_time + convert_back_time
    print(f"Total Python overhead: {total_overhead:.3f}s")
    print(f"Rust processing time: {rust_time:.3f}s")
    print(f"Overhead ratio: {total_overhead/rust_time:.1f}x")
    
    print(f"\nResults: {len(rows_to_expire)} expired, {len(rows_to_insert)} inserted")

if __name__ == "__main__":
    debug_performance_bottlenecks()