# Bitemporal Timeseries Library

A high-performance Rust library with Python bindings for processing bitemporal timeseries data. Optimized for financial services and applications requiring immutable audit trails with both business and system time dimensions.

## Features

- **High Performance**: 500k records processed in ~885ms with adaptive parallelization
- **Zero-Copy Processing**: Apache Arrow columnar data format for efficient memory usage
- **Parallel Processing**: Rayon-based parallelization with adaptive thresholds
- **Conflation**: Automatic merging of adjacent segments with identical values to reduce storage
- **Flexible Schema**: Dynamic ID and value column configuration
- **Python Integration**: Seamless PyO3 bindings for Python workflows
- **Modular Architecture**: Clean separation of concerns with dedicated modules

## Installation

Build from source (requires Rust):

```bash
git clone <your-repository-url>
cd bitemporal-timeseries
uv run maturin develop --release
```

## Quick Start

```python
import pandas as pd
from bitemporal_timeseries import compute_changes
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
expire_indices, insert_batches = compute_changes(
    df_to_record_batch(current_state),
    df_to_record_batch(updates),
    id_columns=['id', 'field'],
    value_columns=['mv', 'price'],
    system_date='2025-07-27',
    update_mode='delta'
)

print(f"Records to expire: {len(expire_indices)}")
print(f"Records to insert: {len(insert_batches)}")
```

## Algorithm Explanation with Examples

### Bitemporal Model

Each record tracks two time dimensions:
- **Effective Time** (`effective_from`, `effective_to`): When the data is valid in the real world
- **As-Of Time** (`as_of_from`, `as_of_to`): When the data was known to the system

Both use TimestampMicrosecond precision for maximum accuracy.

### Core Algorithm: Timeline Processing

The algorithm processes updates by creating a timeline of events and determining what should be active at each point in time.

#### Example 1: Simple Overwrite

**Current State:**
```
ID=123, effective: [2020-01-01, 2021-01-01], as_of: [2025-01-01, max], mv=100
```

**Update:**
```  
ID=123, effective: [2020-06-01, 2020-09-01], as_of: [2025-07-27, max], mv=200
```

**Timeline Processing:**

1. **Create Events:**
   - 2020-01-01: Current starts (mv=100)
   - 2020-06-01: Update starts (mv=200) 
   - 2020-09-01: Update ends
   - 2021-01-01: Current ends

2. **Process Timeline:**
   - [2020-01-01, 2020-06-01): Current active → emit mv=100
   - [2020-06-01, 2020-09-01): Update active → emit mv=200  
   - [2020-09-01, 2021-01-01): Current active → emit mv=100

3. **Result:**
   - **Expire:** Original record (index 0)
   - **Insert:** Three new records covering the split timeline

**Visual Representation:**
```
Before:
Current |=======mv=100========|
        2020-01-01      2021-01-01

Update       |==mv=200==|
             2020-06-01  2020-09-01

After:
New     |=100=|=mv=200=|=100=|
        2020   2020     2020  2021
        01-01  06-01    09-01 01-01
```

#### Example 2: Conflation (Adjacent Identical Values)

**Current State:**
```
ID=123, effective: [2020-01-01, 2020-06-01], as_of: [2025-01-01, max], mv=100
ID=123, effective: [2020-06-01, 2021-01-01], as_of: [2025-01-01, max], mv=100  
```

**Update:**
```
ID=123, effective: [2020-03-01, 2020-04-01], as_of: [2025-07-27, max], mv=100
```

Since the update has the same value (mv=100) as the current state, the algorithm detects this as a **no-change scenario** and skips processing entirely.

#### Example 3: Complex Multi-Update

**Current State:**
```
ID=123, effective: [2020-01-01, 2021-01-01], as_of: [2025-01-01, max], mv=100
```

**Updates:**
```
ID=123, effective: [2020-03-01, 2020-06-01], as_of: [2025-07-27, max], mv=200
ID=123, effective: [2020-09-01, 2020-12-01], as_of: [2025-07-27, max], mv=300
```

**Timeline Processing:**

1. **Events:** 2020-01-01 (current start), 2020-03-01 (update1 start), 2020-06-01 (update1 end), 2020-09-01 (update2 start), 2020-12-01 (update2 end), 2021-01-01 (current end)

2. **Result:**
   - [2020-01-01, 2020-03-01): mv=100 (current)
   - [2020-03-01, 2020-06-01): mv=200 (update1)  
   - [2020-06-01, 2020-09-01): mv=100 (current)
   - [2020-09-01, 2020-12-01): mv=300 (update2)
   - [2020-12-01, 2021-01-01): mv=100 (current)

### Post-Processing Conflation

After timeline processing, the algorithm merges adjacent segments with identical value hashes:

**Before Conflation:**
```
|--mv=100--|--mv=100--|--mv=200--|--mv=100--|--mv=100--|
```

**After Conflation:**
```
|--------mv=100--------|--mv=200--|--------mv=100--------|
```

This significantly reduces database row count while preserving temporal accuracy.

### Update Modes

1. **Delta Mode** (default): Only provided records are updates, existing state is preserved where not overlapped
2. **Full State Mode**: Provided records represent complete new state, all current records for matching IDs are expired

### Parallelization Strategy

The algorithm uses adaptive parallelization:
- **Serial Processing**: Small datasets (<50 ID groups AND <10k records) 
- **Parallel Processing**: Large datasets using Rayon for CPU-bound operations
- **ID Group Independence**: Each ID group processes independently, enabling perfect parallelization

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
uv run python -m pytest tests/test_bitemporal.py -v

# Benchmarks
cargo bench
```

## Development

### Project Structure

**Modular Architecture** (274 lines total in main file, down from 1,085):

- `src/lib.rs` - Main processing function and Python bindings (274 lines)
- `src/types.rs` - Core data structures and constants (88 lines)
- `src/overlap.rs` - Overlap detection and record categorization (68 lines) 
- `src/timeline.rs` - Timeline event processing algorithm (218 lines)
- `src/conflation.rs` - Record conflation and deduplication (157 lines)
- `src/batch_utils.rs` - Arrow RecordBatch utilities (122 lines)
- `tests/integration_tests.rs` - Rust integration tests (5 test scenarios)
- `tests/test_bitemporal_manual.py` - Python test suite (22 test scenarios)
- `benches/bitemporal_benchmarks.rs` - Performance benchmarks
- `CLAUDE.md` - Project context and development notes

### Key Commands

```bash
# Build release version
cargo build --release

# Run benchmarks with HTML reports
cargo bench

# Build Python wheel  
uv run maturin build --release

# Development install
uv run maturin develop
```

### Module Responsibilities

1. **`types.rs`** - Data structures (`BitemporalRecord`, `ChangeSet`, `UpdateMode`) and type conversions
2. **`overlap.rs`** - Determines which records overlap in time and need timeline processing vs direct insertion
3. **`timeline.rs`** - Core algorithm that processes overlapping records through event timeline
4. **`conflation.rs`** - Post-processes results to merge adjacent segments with identical values  
5. **`batch_utils.rs`** - Arrow utilities for RecordBatch creation and timestamp handling

## Dependencies

- **arrow** (53.4) - Columnar data processing
- **pyo3** (0.21) - Python bindings  
- **chrono** (0.4) - Date/time handling
- **blake3** (1.5) - Cryptographic hashing
- **rayon** (1.8) - Parallel processing
- **criterion** (0.5) - Benchmarking framework

## Architecture

### Rust Core
- Zero-copy Arrow array processing
- Parallel execution with Rayon
- Hash-based change detection with BLAKE3
- Post-processing conflation for optimal storage
- Modular design with clear separation of concerns

### Python Interface
- PyO3 bindings for seamless integration
- Arrow RecordBatch input/output
- Compatible with pandas DataFrames via conversion

## Contributing

1. Check `CLAUDE.md` for project context and conventions
2. Run tests before submitting changes
3. Follow existing code style and patterns
4. Update benchmarks for performance-related changes
5. Maintain modular architecture when adding features

## License

MIT License - see LICENSE file for details.

## Built With

- [Apache Arrow](https://arrow.apache.org/) - Columnar data format
- [PyO3](https://pyo3.rs/) - Rust-Python bindings  
- [Rayon](https://github.com/rayon-rs/rayon) - Data parallelism
- [Criterion](https://github.com/bheisler/criterion.rs) - Benchmarking
- [BLAKE3](https://github.com/BLAKE3-team/BLAKE3) - Cryptographic hashing algorithm