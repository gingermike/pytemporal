#!/usr/bin/env python3

import time
import pandas as pd
import numpy as np
from pytemporal import BitemporalTimeseriesProcessor

def create_large_dataset(n_records=500000):
    """Create a large dataset for benchmarking"""
    np.random.seed(42)
    
    # Generate data
    ids = np.random.randint(1, 1000, n_records)  # 1000 unique IDs
    fields = np.random.choice(['field_a', 'field_b', 'field_c'], n_records)
    prices = np.random.uniform(100, 1000, n_records)
    volumes = np.random.uniform(10, 100, n_records)
    
    # Temporal data
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

def benchmark_python_wrapper():
    print("Creating dataset...")
    start = time.time()
    
    # Create datasets
    current_state = create_large_dataset(400000)  # 400k current
    updates = create_large_dataset(100000)       # 100k updates
    
    create_time = time.time() - start
    print(f"Dataset creation: {create_time:.2f}s")
    
    # Create processor
    processor = BitemporalTimeseriesProcessor(
        id_columns=['id', 'field'],
        value_columns=['mv', 'price']
    )
    
    print("Running Python wrapper...")
    start = time.time()
    
    # Process
    expire, insert = processor.compute_changes(
        current_state,
        updates,
        update_mode='delta'
    )
    
    python_time = time.time() - start
    print(f"Python wrapper processing: {python_time:.2f}s")
    print(f"Records expired: {len(expire)}")
    print(f"Records inserted: {len(insert)}")
    
    return python_time

if __name__ == "__main__":
    benchmark_python_wrapper()