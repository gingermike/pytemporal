#!/usr/bin/env python3

import time
import pandas as pd
import numpy as np
import pyarrow as pa
from pytemporal import BitemporalTimeseriesProcessor
from pytemporal.pytemporal import compute_changes as _compute_changes

def create_small_dataset(n_records=1000):
    """Create a very small dataset for type debugging"""
    np.random.seed(42)
    
    ids = np.random.randint(1, 10, n_records) 
    fields = ['field_a'] * n_records
    prices = np.random.uniform(100, 1000, n_records)
    volumes = np.random.uniform(10, 100, n_records)
    
    base_date = pd.Timestamp('2020-01-01')
    effective_from = [base_date] * n_records
    effective_to = [base_date + pd.Timedelta(days=30)] * n_records
    
    as_of_from = [pd.Timestamp.now(tz='UTC').tz_localize(None)] * n_records
    as_of_to = [pd.Timestamp.max] * n_records
    
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

def debug_arrow_types():
    print("=== ARROW TYPE DEBUG ===")
    
    current_state = create_small_dataset(800)
    updates = create_small_dataset(200)
    
    processor = BitemporalTimeseriesProcessor(
        id_columns=['id', 'field'],
        value_columns=['mv', 'price']
    )
    
    # Prepare and convert as the processor does
    current_prepared = processor._prepare_dataframe(current_state)
    updates_prepared = processor._prepare_dataframe(updates)
    
    current_batch = pa.RecordBatch.from_pandas(current_prepared)
    updates_batch = pa.RecordBatch.from_pandas(updates_prepared)
    
    current_batch = processor._convert_timestamps_to_microseconds(current_batch)
    updates_batch = processor._convert_timestamps_to_microseconds(updates_batch)
    
    print("Calling Rust function...")
    expire_indices, insert_batch, expired_batch = _compute_changes(
        current_batch,
        updates_batch,
        ['id', 'field'],
        ['mv', 'price'],
        '2025-08-30',
        'delta'
    )
    
    print(f"\n=== RETURN TYPES ===")
    print(f"expire_indices type: {type(expire_indices)}")
    print(f"insert_batch type: {type(insert_batch)}")
    print(f"expired_batch type: {type(expired_batch)}")
    
    if insert_batch:
        print(f"insert_batch[0] type: {type(insert_batch[0])}")
        batch = insert_batch[0]
        print(f"batch type: {type(batch)}")
        print(f"batch attributes: {dir(batch)}")
        
        print(f"\n=== CONVERSION COMPARISON ===")
        
        # Method 1: to_pandas() (what we're using now)
        start = time.time()
        try:
            df1 = batch.to_pandas()
            method1_time = time.time() - start
            print(f"Method 1 (to_pandas): {method1_time:.3f}s - SUCCESS")
            print(f"Result shape: {df1.shape}")
        except Exception as e:
            print(f"Method 1 (to_pandas): FAILED - {e}")
        
        # Method 2: Manual conversion (what we were using before)
        start = time.time()
        try:
            data = {}
            col_names = batch.column_names
            
            for i in range(batch.num_columns):
                col_name = col_names[i]
                column = batch.column(i)
                col_data = column.to_pylist()
                data[col_name] = col_data
            
            df2 = pd.DataFrame(data)
            method2_time = time.time() - start
            print(f"Method 2 (manual): {method2_time:.3f}s - SUCCESS")
            print(f"Result shape: {df2.shape}")
        except Exception as e:
            print(f"Method 2 (manual): FAILED - {e}")
        
        # Method 3: Try using PyArrow table
        start = time.time()
        try:
            table = pa.Table.from_batches([batch])
            df3 = table.to_pandas()
            method3_time = time.time() - start
            print(f"Method 3 (via Table): {method3_time:.3f}s - SUCCESS")
            print(f"Result shape: {df3.shape}")
        except Exception as e:
            print(f"Method 3 (via Table): FAILED - {e}")

if __name__ == "__main__":
    debug_arrow_types()