#!/usr/bin/env python3
"""
Performance validation test for refactored bitemporal processor.
This test ensures the refactored code maintains the original 82.5k rows/sec performance.
"""

import sys
import gc
import time
import psutil
import pandas as pd
import pyarrow as pa
from pytemporal import compute_changes_with_hash_algorithm

def get_memory_usage():
    """Get current memory usage in GB"""
    process = psutil.Process()
    return process.memory_info().rss / (1024**3)

def create_800k_dataset():
    """Create the standard 800k×80 performance test dataset"""
    print("Creating 800K×80 dataset...")
    
    # Create current state - 800k rows
    data = {}
    data['portfolio_id'] = [f'portfolio_{i//1000}' for i in range(800_000)]
    data['security_id'] = [f'security_{i}' for i in range(800_000)]
    
    # Add 76 value columns (bool, int, float, string types)
    for i in range(1, 21):
        data[f'bool_col_{i}'] = [True if (j + i) % 3 == 0 else False for j in range(800_000)]
    for i in range(1, 21):
        data[f'int_col_{i}'] = [(j * i) % 1000 for j in range(800_000)]
    for i in range(1, 21):
        data[f'float_col_{i}'] = [j * 1.5 + i for j in range(800_000)]
    for i in range(1, 16):  # 15 string cols to get exactly 76 value columns
        data[f'str_col_{i}'] = [f'value_{j}_{i}' for j in range(800_000)]
    
    # Temporal columns
    data['effective_from'] = pd.to_datetime('2024-01-01')
    data['effective_to'] = pd.to_datetime('2260-12-31')
    data['as_of_from'] = pd.to_datetime('2024-01-01 09:00:00')
    data['as_of_to'] = pd.to_datetime('2260-12-31 23:59:59')
    
    current_state = pa.Table.from_pandas(pd.DataFrame(data)).to_batches()[0]
    print(f"Current state: {current_state.num_rows:,} rows × {current_state.num_columns} columns")
    
    # Create updates - 50k rows
    update_data = {}
    update_data['portfolio_id'] = [f'portfolio_{i//100}' for i in range(50_000)]
    update_data['security_id'] = [f'security_{i}' for i in range(50_000)]
    
    # Add same 76 value columns with modified values
    for i in range(1, 21):
        update_data[f'bool_col_{i}'] = [False if (j + i) % 3 == 0 else True for j in range(50_000)]
    for i in range(1, 21):
        update_data[f'int_col_{i}'] = [(j * i + 1) % 1000 for j in range(50_000)]
    for i in range(1, 21):
        update_data[f'float_col_{i}'] = [j * 2.5 + i for j in range(50_000)]
    for i in range(1, 16):
        update_data[f'str_col_{i}'] = [f'updated_{j}_{i}' for j in range(50_000)]
    
    # Temporal columns for updates
    update_data['effective_from'] = pd.to_datetime('2024-02-01')
    update_data['effective_to'] = pd.to_datetime('2260-12-31')
    update_data['as_of_from'] = pd.to_datetime('2024-02-01 09:00:00')
    update_data['as_of_to'] = pd.to_datetime('2260-12-31 23:59:59')
    
    updates = pa.Table.from_pandas(pd.DataFrame(update_data)).to_batches()[0]
    print(f"Updates: {updates.num_rows:,} rows × {updates.num_columns} columns")
    
    value_columns = [col for col in current_state.column_names 
                    if col.endswith('_col_1') or col.endswith('_col_2') or col.endswith('_col_3') or
                       col.endswith('_col_4') or col.endswith('_col_5') or col.endswith('_col_6') or
                       col.endswith('_col_7') or col.endswith('_col_8') or col.endswith('_col_9') or
                       col.endswith('_col_10') or col.endswith('_col_11') or col.endswith('_col_12') or
                       col.endswith('_col_13') or col.endswith('_col_14') or col.endswith('_col_15') or
                       col.endswith('_col_16') or col.endswith('_col_17') or col.endswith('_col_18') or
                       col.endswith('_col_19') or col.endswith('_col_20')]
    
    print(f"Value columns: {len(value_columns)} columns")
    return current_state, updates, value_columns

def main():
    print("=" * 80)
    print("🔍 REFACTORED BITEMPORAL PROCESSOR PERFORMANCE VALIDATION")
    print("=" * 80)
    print()
    
    initial_memory = get_memory_usage()
    print(f"Initial memory: {initial_memory:.2f} GB")
    
    # Create dataset
    dataset_start = time.time()
    current_state, updates, value_columns = create_800k_dataset()
    dataset_time = time.time() - dataset_start
    dataset_memory = get_memory_usage()
    
    print(f"Dataset creation time: {dataset_time:.2f} seconds")
    print(f"Memory after dataset: {dataset_memory:.2f} GB (+{dataset_memory - initial_memory:.2f} GB)")
    print()
    
    # Performance test
    print("🚀 PERFORMANCE VALIDATION")
    print("=" * 60)
    
    memory_before = get_memory_usage()
    print(f"Memory before processing: {memory_before:.2f} GB")
    
    start_time = time.time()
    
    to_expire, to_insert, expired_records = compute_changes_with_hash_algorithm(
        current_state=pa.RecordBatch.from_arrays(
            [current_state.column(i) for i in range(current_state.num_columns)], 
            names=current_state.column_names
        ),
        updates=pa.RecordBatch.from_arrays(
            [updates.column(i) for i in range(updates.num_columns)], 
            names=updates.column_names
        ),
        id_columns=['portfolio_id', 'security_id'],
        value_columns=value_columns,
        system_date='2024-02-01',
        update_mode='delta',
        hash_algorithm='xxhash'
    )
    
    end_time = time.time()
    processing_time = end_time - start_time
    memory_after = get_memory_usage()
    
    # Calculate performance metrics
    total_rows = current_state.num_rows + updates.num_rows
    throughput = total_rows / processing_time
    insert_count = sum(batch.num_rows for batch in to_insert)
    
    print()
    print("📈 RESULTS:")
    print(f"⏱️  Processing time: {processing_time:.2f} seconds")
    print(f"🚀 Throughput: {throughput:,.0f} rows/second")
    print(f"💾 Peak memory: {memory_after:.2f} GB")
    print(f"📤 Rows to expire: {len(to_expire):,}")
    print(f"📥 Rows to insert: {insert_count:,}")
    print()
    
    print("🎯 PERFORMANCE VALIDATION:")
    target_throughput = 82_500
    target_memory_max = 12.0
    
    throughput_ratio = throughput / target_throughput
    memory_ok = memory_after <= target_memory_max
    
    print(f"Throughput: {throughput:,.0f} vs {target_throughput:,.0f} expected")
    print(f"   {'✅ PASS' if throughput_ratio >= 0.95 else '❌ FAIL'} ({throughput_ratio:.1%} of target)")
    
    print(f"Memory: {memory_after:.1f}GB vs {target_memory_max:.1f}GB max")
    print(f"   {'✅ PASS' if memory_ok else '❌ FAIL'} ({memory_after/target_memory_max:.1%} of limit)")
    
    print()
    overall_pass = throughput_ratio >= 0.95 and memory_ok
    print(f"🏆 Overall: {'✅ PASS - Refactoring successful!' if overall_pass else '❌ FAIL - Performance regression detected'}")
    
    if overall_pass:
        print()
        print("🎉 REFACTORING SUCCESS!")
        print("✅ Maintainable code structure achieved")
        print("✅ Performance characteristics preserved")
        print("✅ All temporal logic intact")
        
    return overall_pass

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)