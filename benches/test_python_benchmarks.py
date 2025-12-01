"""
Python Benchmark Suite for PyTemporal

This benchmark suite mirrors the Rust criterion benchmarks to provide
comparable performance data for the Python API.

Run with:
    uv run pytest benches/test_python_benchmarks.py --benchmark-only -v

Save JSON for CI:
    uv run pytest benches/test_python_benchmarks.py --benchmark-only --benchmark-json=benchmark_results.json

Compare against baseline:
    uv run pytest benches/test_python_benchmarks.py --benchmark-compare
"""

import pytest
import pandas as pd
import numpy as np
from typing import Tuple, List

from pytemporal import BitemporalTimeseriesProcessor


# =============================================================================
# Test Data Generators (mirroring Rust benchmark data)
# =============================================================================

def create_small_dataset() -> Tuple[pd.DataFrame, pd.DataFrame, List[str], List[str]]:
    """Small dataset matching Rust bench_small_dataset (5 current, 2 updates)"""
    current_state = pd.DataFrame({
        'id': [1, 1, 1, 2, 2],
        'field': ['A', 'A', 'A', 'B', 'B'],
        'mv': [100, 200, 300, 150, 250],
        'price': [1000, 2000, 3000, 1500, 2500],
        'effective_from': pd.to_datetime(['2024-01-01', '2024-04-01', '2024-08-01', '2024-01-01', '2024-06-01']),
        'effective_to': pd.to_datetime(['2024-04-01', '2024-08-01', '2024-12-31', '2024-06-01', '2024-12-31']),
        'as_of_from': pd.to_datetime(['2024-01-01'] * 5),
        'as_of_to': pd.Timestamp('2260-12-31 23:59:59'),
        'value_hash': [''] * 5,
    })

    updates = pd.DataFrame({
        'id': [1, 2],
        'field': ['A', 'B'],
        'mv': [999, 888],
        'price': [9999, 8888],
        'effective_from': pd.to_datetime(['2024-03-01', '2024-05-01']),
        'effective_to': pd.to_datetime(['2024-09-01', '2024-07-01']),
        'as_of_from': pd.to_datetime(['2024-07-21', '2024-07-21']),
        'as_of_to': pd.Timestamp('2260-12-31 23:59:59'),
        'value_hash': [''] * 2,
    })

    return current_state, updates, ['id', 'field'], ['mv', 'price']


def create_medium_dataset() -> Tuple[pd.DataFrame, pd.DataFrame, List[str], List[str]]:
    """Medium dataset matching Rust bench_medium_dataset (100 current, 20 updates)"""
    n_current = 100
    n_updates = 20

    current_state = pd.DataFrame({
        'id': [i // 10 for i in range(n_current)],
        'field': ['field'] * n_current,
        'mv': [100 + i for i in range(n_current)],
        'price': [1000 + i for i in range(n_current)],
        'effective_from': pd.to_datetime(['2024-01-01'] * n_current),
        'effective_to': pd.to_datetime(['2024-12-31'] * n_current),
        'as_of_from': pd.to_datetime(['2024-01-01'] * n_current),
        'as_of_to': pd.Timestamp('2260-12-31 23:59:59'),
        'value_hash': [''] * n_current,
    })

    updates = pd.DataFrame({
        'id': [i // 2 for i in range(n_updates)],
        'field': ['field'] * n_updates,
        'mv': [999] * n_updates,
        'price': [9999] * n_updates,
        'effective_from': pd.to_datetime(['2024-06-01'] * n_updates),
        'effective_to': pd.to_datetime(['2024-08-01'] * n_updates),
        'as_of_from': pd.to_datetime(['2024-07-21'] * n_updates),
        'as_of_to': pd.Timestamp('2260-12-31 23:59:59'),
        'value_hash': [''] * n_updates,
    })

    return current_state, updates, ['id', 'field'], ['mv', 'price']


def create_scaling_dataset(size: int) -> Tuple[pd.DataFrame, pd.DataFrame, List[str], List[str]]:
    """Scaling dataset matching Rust bench_scaling_by_size"""
    n_updates = max(1, size // 5)  # 20% updates

    current_state = pd.DataFrame({
        'id': [i // 10 for i in range(size)],
        'field': ['field'] * size,
        'mv': [100 + i for i in range(size)],
        'price': [1000 + i for i in range(size)],
        'effective_from': pd.to_datetime(['2024-01-01'] * size),
        'effective_to': pd.to_datetime(['2024-12-31'] * size),
        'as_of_from': pd.to_datetime(['2024-01-01'] * size),
        'as_of_to': pd.Timestamp('2260-12-31 23:59:59'),
        'value_hash': [''] * size,
    })

    updates = pd.DataFrame({
        'id': [i // 2 for i in range(n_updates)],
        'field': ['field'] * n_updates,
        'mv': [999] * n_updates,
        'price': [9999] * n_updates,
        'effective_from': pd.to_datetime(['2024-06-01'] * n_updates),
        'effective_to': pd.to_datetime(['2024-08-01'] * n_updates),
        'as_of_from': pd.to_datetime(['2024-07-21'] * n_updates),
        'as_of_to': pd.Timestamp('2260-12-31 23:59:59'),
        'value_hash': [''] * n_updates,
    })

    return current_state, updates, ['id', 'field'], ['mv', 'price']


def create_parallel_dataset(
    num_ids: int, records_per_id: int
) -> Tuple[pd.DataFrame, pd.DataFrame, List[str], List[str]]:
    """Parallel effectiveness dataset matching Rust bench_parallel_effectiveness"""
    total_records = num_ids * records_per_id
    updates_per_id = max(1, records_per_id // 10)
    total_updates = num_ids * updates_per_id

    # Current state
    ids = []
    records = []
    for id_val in range(num_ids):
        for record in range(records_per_id):
            ids.append(id_val)
            records.append(record)

    current_state = pd.DataFrame({
        'id': ids,
        'field': ['field'] * total_records,
        'mv': [100 + r for r in records],
        'price': [1000 + r for r in records],
        'effective_from': pd.to_datetime(['2024-01-01'] * total_records),
        'effective_to': pd.to_datetime(['2024-12-31'] * total_records),
        'as_of_from': pd.to_datetime(['2024-01-01'] * total_records),
        'as_of_to': pd.Timestamp('2260-12-31 23:59:59'),
        'value_hash': [''] * total_records,
    })

    # Updates
    update_ids = []
    update_records = []
    for id_val in range(num_ids):
        for update in range(updates_per_id):
            update_ids.append(id_val)
            update_records.append(update)

    updates = pd.DataFrame({
        'id': update_ids,
        'field': ['field'] * total_updates,
        'mv': [999 + u for u in update_records],
        'price': [9999 + u for u in update_records],
        'effective_from': pd.to_datetime(['2024-06-01'] * total_updates),
        'effective_to': pd.to_datetime(['2024-08-01'] * total_updates),
        'as_of_from': pd.to_datetime(['2024-07-21'] * total_updates),
        'as_of_to': pd.Timestamp('2260-12-31 23:59:59'),
        'value_hash': [''] * total_updates,
    })

    return current_state, updates, ['id', 'field'], ['mv', 'price']


def create_wide_dataset(
    num_rows: int, num_value_columns: int
) -> Tuple[pd.DataFrame, pd.DataFrame, List[str], List[str]]:
    """Dataset with many value columns (realistic production scenario)"""
    num_id_groups = max(1, num_rows // 10)
    ids = np.arange(num_rows) % num_id_groups
    base_dates = pd.date_range('2024-01-01', periods=num_rows, freq='1h')

    value_data = {f'value_{i}': np.random.randn(num_rows) * 100 for i in range(num_value_columns)}
    value_columns = list(value_data.keys())

    current_state = pd.DataFrame({
        'entity_id': ids,
        'effective_from': base_dates,
        'effective_to': pd.Timestamp('2260-12-31 23:59:59'),
        'as_of_from': base_dates,
        'as_of_to': pd.Timestamp('2260-12-31 23:59:59'),
        'value_hash': '',
        **value_data
    })

    # 20% updates
    n_updates = max(1, num_rows // 5)
    update_indices = np.random.choice(num_rows, size=n_updates, replace=False)
    updates = current_state.iloc[update_indices].copy()
    for col in value_columns:
        updates[col] = updates[col] * 1.1 + np.random.randn(len(updates))
    updates['effective_from'] = pd.Timestamp('2024-06-01')
    updates['as_of_from'] = pd.Timestamp('2024-06-01')

    return current_state, updates, ['entity_id'], value_columns


# =============================================================================
# Benchmark Tests
# =============================================================================

class TestSmallDataset:
    """Benchmarks for small datasets (matching Rust bench_small_dataset)"""

    def test_small_dataset(self, benchmark):
        """Small dataset: 5 current records, 2 updates"""
        current, updates, id_cols, value_cols = create_small_dataset()
        processor = BitemporalTimeseriesProcessor(id_cols, value_cols)

        result = benchmark(
            processor.compute_changes,
            current, updates, '2024-07-21', 'delta'
        )

        # Sanity check
        rows_to_expire, rows_to_insert = result
        assert isinstance(rows_to_expire, pd.DataFrame)
        assert isinstance(rows_to_insert, pd.DataFrame)


class TestMediumDataset:
    """Benchmarks for medium datasets (matching Rust bench_medium_dataset)"""

    def test_medium_dataset(self, benchmark):
        """Medium dataset: 100 current records, 20 updates"""
        current, updates, id_cols, value_cols = create_medium_dataset()
        processor = BitemporalTimeseriesProcessor(id_cols, value_cols)

        benchmark(processor.compute_changes, current, updates, '2024-07-21', 'delta')


class TestScalingBySize:
    """Benchmarks for scaling by dataset size (matching Rust bench_scaling_by_size)"""

    # Match exact Rust benchmark sizes: [10, 50, 100, 500, 500_000]
    @pytest.mark.parametrize("size", [10, 50, 100, 500, 500_000])
    def test_scaling(self, benchmark, size):
        """Test scaling with increasing dataset sizes"""
        current, updates, id_cols, value_cols = create_scaling_dataset(size)
        processor = BitemporalTimeseriesProcessor(id_cols, value_cols)

        # Calculate throughput for reporting
        total_rows = len(current) + len(updates)

        benchmark.group = f"scaling/{size}_records"
        benchmark.extra_info['total_rows'] = total_rows
        benchmark.extra_info['current_rows'] = len(current)
        benchmark.extra_info['update_rows'] = len(updates)

        benchmark(processor.compute_changes, current, updates, '2024-07-21', 'delta')


class TestParallelEffectiveness:
    """Benchmarks for parallel processing effectiveness (matching Rust bench_parallel_effectiveness)"""

    @pytest.mark.parametrize("scenario,num_ids,records_per_id", [
        ("few_ids_many_records", 10, 1000),
        ("many_ids_few_records", 1000, 10),
        ("balanced_workload", 100, 100),
    ])
    def test_parallel_scenarios(self, benchmark, scenario, num_ids, records_per_id):
        """Test different ID distribution scenarios"""
        current, updates, id_cols, value_cols = create_parallel_dataset(num_ids, records_per_id)
        processor = BitemporalTimeseriesProcessor(id_cols, value_cols)

        total_rows = len(current) + len(updates)
        benchmark.group = f"parallel/{scenario}"
        benchmark.extra_info['total_rows'] = total_rows
        benchmark.extra_info['num_ids'] = num_ids
        benchmark.extra_info['records_per_id'] = records_per_id

        benchmark(processor.compute_changes, current, updates, '2024-07-21', 'delta')


class TestWideDatasets:
    """Benchmarks for wide datasets (many columns - realistic production)"""

    @pytest.mark.parametrize("num_rows,num_cols", [
        (1000, 10),
        (5000, 20),
        (10000, 40),
        (50000, 80),  # Large production-like dataset
    ])
    def test_wide_dataset(self, benchmark, num_rows, num_cols):
        """Test with varying number of value columns"""
        current, updates, id_cols, value_cols = create_wide_dataset(num_rows, num_cols)
        processor = BitemporalTimeseriesProcessor(id_cols, value_cols)

        total_rows = len(current) + len(updates)
        benchmark.group = f"wide/{num_rows}x{num_cols}"
        benchmark.extra_info['total_rows'] = total_rows
        benchmark.extra_info['num_columns'] = num_cols

        benchmark(processor.compute_changes, current, updates, '2024-07-21', 'delta')


class TestUpdateModes:
    """Benchmarks comparing delta vs full_state modes"""

    def test_delta_mode(self, benchmark):
        """Delta mode processing"""
        current, updates, id_cols, value_cols = create_medium_dataset()
        processor = BitemporalTimeseriesProcessor(id_cols, value_cols)

        benchmark.group = "update_mode/delta"
        benchmark(processor.compute_changes, current, updates, '2024-07-21', 'delta')

    def test_full_state_mode(self, benchmark):
        """Full state mode processing"""
        current, updates, id_cols, value_cols = create_medium_dataset()
        processor = BitemporalTimeseriesProcessor(id_cols, value_cols)

        benchmark.group = "update_mode/full_state"
        benchmark(processor.compute_changes, current, updates, '2024-07-21', 'full_state')


class TestConflationEffectiveness:
    """Benchmarks for conflation (matching Rust bench_conflation_effectiveness)"""

    def test_conflation_effectiveness(self, benchmark):
        """Test conflation with many adjacent same-value segments"""
        # Create dataset with adjacent same-value segments that can be conflated
        current_state = pd.DataFrame({
            'id': [1, 1, 1, 1, 1],
            'field': ['A', 'A', 'A', 'A', 'A'],
            'mv': [100, 100, 100, 100, 100],  # Same values
            'price': [1000, 1000, 1000, 1000, 1000],
            'effective_from': pd.to_datetime([
                '2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01', '2024-05-01'
            ]),
            'effective_to': pd.to_datetime([
                '2024-02-01', '2024-03-01', '2024-04-01', '2024-05-01', '2024-06-01'
            ]),
            'as_of_from': pd.to_datetime(['2024-01-01'] * 5),
            'as_of_to': pd.Timestamp('2260-12-31 23:59:59'),
            'value_hash': [''] * 5,
        })

        updates = pd.DataFrame({
            'id': [1],
            'field': ['A'],
            'mv': [999],
            'price': [9999],
            'effective_from': pd.to_datetime(['2024-01-15']),
            'effective_to': pd.to_datetime(['2024-05-15']),
            'as_of_from': pd.to_datetime(['2024-07-21']),
            'as_of_to': pd.Timestamp('2260-12-31 23:59:59'),
            'value_hash': [''],
        })

        processor = BitemporalTimeseriesProcessor(['id', 'field'], ['mv', 'price'])

        benchmark.group = "conflation"
        benchmark(processor.compute_changes, current_state, updates, '2024-07-21', 'delta')
