#!/usr/bin/env python3

import time
import pandas as pd
import numpy as np
from pytemporal import BitemporalTimeseriesProcessor

def create_dataset(n_records=50000):
    """Create a larger dataset"""
    np.random.seed(42)
    
    ids = np.random.randint(1, 1000, n_records)  # 1000 unique IDs
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

def debug_batch_fragmentation():
    print("=== BATCH FRAGMENTATION DEBUG ===")
    
    current_state = create_dataset(40000)
    updates = create_dataset(10000)
    
    print(f"Input: {len(current_state)} current, {len(updates)} updates")
    
    processor = BitemporalTimeseriesProcessor(
        id_columns=['id', 'field'],
        value_columns=['mv', 'price']
    )
    
    start = time.time()
    expire, insert = processor.compute_changes(
        current_state,
        updates,
        update_mode='delta'
    )
    total_time = time.time() - start
    
    print(f"Results: {len(expire)} expired, {len(insert)} inserted")
    print(f"Total time: {total_time:.2f}s")
    
    # Now let's instrument the conversion process to see batch counts
    print("\n=== DETAILED TIMING ===")
    
    # Call the internal method with timing
    current_prepared = processor._prepare_dataframe(current_state)
    updates_prepared = processor._prepare_dataframe(updates)
    
    import pyarrow as pa
    current_batch = pa.RecordBatch.from_pandas(current_prepared)
    updates_batch = pa.RecordBatch.from_pandas(updates_prepared)
    
    current_batch = processor._convert_timestamps_to_microseconds(current_batch)
    updates_batch = processor._convert_timestamps_to_microseconds(updates_batch)
    
    from pytemporal.pytemporal import compute_changes as _compute_changes
    
    start = time.time()
    expire_indices, insert_batch, expired_batch = _compute_changes(
        current_batch,
        updates_batch,
        ['id', 'field'],
        ['mv', 'price'],
        '2025-08-30',
        'delta'
    )
    rust_time = time.time() - start
    
    print(f"Rust processing: {rust_time:.3f}s")
    print(f"Expired batches: {len(expired_batch) if expired_batch else 0}")
    print(f"Insert batches: {len(insert_batch) if insert_batch else 0}")
    
    if expired_batch:
        expired_rows = sum(batch.num_rows for batch in expired_batch)
        print(f"Expired batch sizes: {[batch.num_rows for batch in expired_batch]}")
        print(f"Total expired rows: {expired_rows}")
        
    if insert_batch:
        insert_rows = sum(batch.num_rows for batch in insert_batch)
        print(f"Insert batch sizes: {[batch.num_rows for batch in insert_batch]}")
        print(f"Total insert rows: {insert_rows}")
    
    # Time the conversion step with batch info
    start = time.time()
    conversion_count = 0
    total_rows = 0
    
    if expired_batch:
        for batch in expired_batch:
            conversion_count += 1
            total_rows += batch.num_rows
            # Simulate our current conversion
            data = {}
            for i in range(batch.num_columns):
                col_name = batch.column_names[i]
                column = batch.column(i)
                col_data = column.to_pylist()
                data[col_name] = col_data
    
    if insert_batch:
        for batch in insert_batch:
            conversion_count += 1
            total_rows += batch.num_rows
            # Simulate our current conversion
            data = {}
            for i in range(batch.num_columns):
                col_name = batch.column_names[i]
                column = batch.column(i)
                col_data = column.to_pylist()
                data[col_name] = col_data
    
    conversion_time = time.time() - start
    print(f"\nConversion details:")
    print(f"Total conversions: {conversion_count}")
    print(f"Total rows converted: {total_rows}")
    print(f"Conversion time: {conversion_time:.3f}s")
    print(f"Time per batch: {conversion_time/conversion_count:.4f}s")
    print(f"Time per row: {conversion_time/total_rows*1000:.4f}ms")

if __name__ == "__main__":
    debug_batch_fragmentation()