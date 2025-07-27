# Bitemporal Timeseries Library

A high-performance Rust library with Python bindings for processing bitemporal timeseries data. Optimized for financial services and applications requiring immutable audit trails with both business and system time dimensions.

## Features

- **High Performance**: 500k records processed in ~885ms with adaptive parallelization
- **Zero-Copy Processing**: Apache Arrow columnar data format for efficient memory usage
- **Parallel Processing**: Rayon-based parallelization with adaptive thresholds
- **Conflation**: Automatic merging of adjacent segments with identical values to reduce storage
- **Flexible Schema**: Dynamic ID and value column configuration
- **Python Integration**: Seamless PyO3 bindings for Python workflows

## Installation

Build from source (requires Rust):

```bash
git clone <your-repository-url>
cd bitemporal-timeseries
maturin develop --release
```

## Quick Start

```python
import pandas as pd
from bitemporal_timeseries import process_updates, UpdateMode
import pyarrow as pa
from datetime import datetime

# Convert pandas DataFrames to Arrow RecordBatches
def df_to_record_batch(df):
    table = pa.Table.from_pandas(df)
    return table.to_batches()[0]

# Current state
current_state = pd.DataFrame({
    'id': [1234, 1234],
    'field': ['test', 'fielda'], 
    'mv': [300, 400],
    'price': [400, 500],
    'effective_from': pd.to_datetime(['2020-01-01', '2020-01-01']),
    'effective_to': pd.to_datetime(['2021-01-01', '2021-01-01']),
    'as_of_from': pd.to_datetime(['2025-01-01', '2025-01-01']),
    'as_of_to': pd.to_datetime(['2262-04-11', '2262-04-11']),  # Max date
    'value_hash': [0, 0]  # Will be computed automatically
})

# Updates
updates = pd.DataFrame({
    'id': [1234],
    'field': ['test'],
    'mv': [400], 
    'price': [300],
    'effective_from': pd.to_datetime(['2020-06-01']),
    'effective_to': pd.to_datetime(['2020-09-01']),
    'as_of_from': pd.to_datetime(['2025-07-27']),
    'as_of_to': pd.to_datetime(['2262-04-11']),
    'value_hash': [0]
})

# Process updates
changeset = process_updates(
    df_to_record_batch(current_state),
    df_to_record_batch(updates),
    id_columns=['id', 'field'],
    value_columns=['mv', 'price'],
    system_date=datetime(2025, 7, 27).date(),
    update_mode=UpdateMode.Delta
)

print(f"Records to expire: {len(changeset.to_expire)}")
print(f"Records to insert: {len(changeset.to_insert)}")
```

## How It Works

### Bitemporal Model

Each record tracks two time dimensions:
- **Effective Time** (`effective_from`, `effective_to`): When the data is valid in the real world (Date32 precision)
- **As-Of Time** (`as_of_from`, `as_of_to`): When the data was known to the system (TimestampMicrosecond precision for audit accuracy)

### Update Processing

When processing updates that overlap existing records:

1. **Expire** original overlapping records by setting their `as_of_to` 
2. **Insert** new records to maintain temporal continuity:
   - Records before the update (preserving original values)
   - The update itself 
   - Records after the update (preserving original values)

### Conflation

The system automatically merges adjacent temporal segments with identical value hashes, reducing the number of database rows while maintaining temporal accuracy.

Example:
```
Before conflation: |--A--|--A--|--A--|--B--|
After conflation:  |------A------|--B--|
```

## Performance

Benchmarked on modern hardware:

- **500k records**: ~885ms processing time
- **Adaptive Parallelization**: Automatically uses multiple threads for large datasets
- **Parallel Thresholds**: >50 ID groups OR >10k total records triggers parallel processing
- **Conflation Efficiency**: Significant row reduction for datasets with temporal continuity

## Testing

Run the test suites:

```bash
# Rust tests
cargo test

# Python tests  
uv run python -m pytest tests/test_bitemporal_manual.py -v

# Benchmarks
cargo bench
```

## Development

### Project Structure

- `src/lib.rs` - Core bitemporal algorithm (870 lines)
- `tests/integration_tests.rs` - Rust integration tests 
- `tests/test_bitemporal_manual.py` - Python test suite
- `benches/bitemporal_benchmarks.rs` - Performance benchmarks
- `CLAUDE.md` - Project context and development notes

### Key Commands

```bash
# Build release version
cargo build --release

# Run benchmarks with HTML reports
cargo bench

# Build Python wheel
maturin build --release

# Development install
maturin develop
```

### Algorithm Details

The core algorithm:
1. Groups records by ID columns for independent processing
2. Processes each group to handle overlapping effective time periods
3. Computes value hashes for change detection
4. Applies conflation to merge adjacent identical segments
5. Returns changesets with records to expire and insert

## Dependencies

- **arrow** (53.4) - Columnar data processing
- **pyo3** (0.21) - Python bindings  
- **chrono** (0.4) - Date/time handling
- **xxhash-rust** (0.8) - Fast value hashing
- **rayon** (1.8) - Parallel processing
- **criterion** (0.5) - Benchmarking framework

## Architecture

### Rust Core
- Zero-copy Arrow array processing
- Parallel execution with Rayon
- Hash-based change detection with xxHash
- Post-processing conflation for optimal storage

### Python Interface
- PyO3 bindings for seamless integration
- Arrow RecordBatch input/output
- Compatible with pandas DataFrames via conversion

## Contributing

1. Check `CLAUDE.md` for project context and conventions
2. Run tests before submitting changes
3. Follow existing code style and patterns
4. Update benchmarks for performance-related changes

## License

MIT License - see LICENSE file for details.

## Built With

- [Apache Arrow](https://arrow.apache.org/) - Columnar data format
- [PyO3](https://pyo3.rs/) - Rust-Python bindings  
- [Rayon](https://github.com/rayon-rs/rayon) - Data parallelism
- [Criterion](https://github.com/bheisler/criterion.rs) - Benchmarking
- [xxHash](https://github.com/Cyan4973/xxHash) - Fast hashing algorithm