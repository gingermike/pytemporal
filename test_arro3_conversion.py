#!/usr/bin/env python3

import time
import pandas as pd
import numpy as np
import pyarrow as pa
from pytemporal import BitemporalTimeseriesProcessor
from pytemporal.pytemporal import compute_changes as _compute_changes

def create_small_dataset(n_records=1000):
    """Create a very small dataset"""
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

def test_arro3_conversions():
    print("=== ARRO3 CONVERSION METHODS ===")
    
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
    
    if insert_batch:
        batch = insert_batch[0]
        print(f"Working with batch of shape: {batch.shape}")
        
        # Try different conversion methods
        methods = []
        
        # Method 1: Current slow method (to_pylist)
        start = time.time()
        data = {}
        for i in range(batch.num_columns):
            col_name = batch.column_names[i]
            column = batch.column(i)
            col_data = column.to_pylist()
            data[col_name] = col_data
        df1 = pd.DataFrame(data)
        time1 = time.time() - start
        methods.append(("to_pylist", time1, df1.shape))
        
        # Method 2: Try using __arrow_c_array__ interface
        try:
            start = time.time()
            # Use the Arrow C Data interface
            arrays = []
            schema_fields = []
            
            for i in range(batch.num_columns):
                col_name = batch.column_names[i]
                column = batch.column(i)
                
                # Convert via Arrow C interface
                array_capsule = column.__arrow_c_array__()
                schema_capsule = column.__arrow_c_schema__()
                
                # Import back to PyArrow
                arrow_array = pa.Array._import_from_c(*array_capsule)
                arrays.append(arrow_array)
                schema_fields.append(pa.field(col_name, arrow_array.type))
            
            # Create PyArrow RecordBatch
            pyarrow_batch = pa.RecordBatch.from_arrays(arrays, schema=pa.schema(schema_fields))
            df2 = pyarrow_batch.to_pandas()
            time2 = time.time() - start
            methods.append(("C interface", time2, df2.shape))
        except Exception as e:
            methods.append(("C interface", float('inf'), f"FAILED: {e}"))
        
        # Method 3: Try batch-level C interface
        try:
            start = time.time()
            # Get the whole batch via C interface
            batch_capsule = batch.__arrow_c_array__()
            schema_capsule = batch.__arrow_c_schema__()
            
            # Import to PyArrow RecordBatch
            pyarrow_batch = pa.RecordBatch._import_from_c(*batch_capsule)
            df3 = pyarrow_batch.to_pandas()
            time3 = time.time() - start
            methods.append(("Batch C interface", time3, df3.shape))
        except Exception as e:
            methods.append(("Batch C interface", float('inf'), f"FAILED: {e}"))
        
        print("\n=== CONVERSION RESULTS ===")
        for method, timing, result in methods:
            if isinstance(result, str):
                print(f"{method:20}: {result}")
            else:
                print(f"{method:20}: {timing:.4f}s - Shape: {result}")
        
        # Find the fastest working method
        valid_methods = [(m, t, r) for m, t, r in methods if not isinstance(r, str) and t != float('inf')]
        if valid_methods:
            fastest = min(valid_methods, key=lambda x: x[1])
            print(f"\nFastest method: {fastest[0]} ({fastest[1]:.4f}s)")
            speedup = methods[0][1] / fastest[1]  # Compare to to_pylist
            print(f"Speedup vs to_pylist: {speedup:.1f}x")

if __name__ == "__main__":
    test_arro3_conversions()