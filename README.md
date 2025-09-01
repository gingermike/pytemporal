# PyTemporal Library

A high-performance Rust library with Python bindings for processing bitemporal timeseries data. Optimized for financial services and applications requiring immutable audit trails with both business and system time dimensions.

## Features

- **High Performance**: 500k records processed in ~885ms with adaptive parallelization
- **Optimized Python Wrapper**: Batch consolidation reduces conversion overhead to <0.1 seconds
- **Zero-Copy Processing**: Apache Arrow columnar data format for efficient memory usage
- **Parallel Processing**: Rayon-based parallelization with adaptive thresholds
- **Conflation**: Automatic merging of adjacent segments with identical values to reduce storage
- **Full State Mode**: Complete state replacement with tombstone records for audit trails
- **Flexible Schema**: Dynamic ID and value column configuration
- **Python Integration**: High-level DataFrame API with seamless PyO3 bindings
- **Modular Architecture**: Clean separation of concerns with dedicated modules
- **Performance Monitoring**: Integrated flamegraph generation and GitHub Pages benchmark reports

## Installation

Build from source (requires Rust):

```bash
git clone <your-repository-url>
cd pytemporal
uv run maturin develop --release
```

## Quick Start

### High-Level DataFrame API (Recommended)

```python
import pandas as pd
from pytemporal import BitemporalTimeseriesProcessor

# Initialize processor
processor = BitemporalTimeseriesProcessor(
    id_columns=['id', 'field'],
    value_columns=['mv', 'price']
)

# Current state
current_state = pd.DataFrame({
    'id': [1234, 1234],
    'field': ['test', 'fielda'], 
    'mv': [300, 400],
    'price': [400, 500],
    'effective_from': pd.to_datetime(['2020-01-01', '2020-01-01']),
    'effective_to': pd.to_datetime(['2021-01-01', '2021-01-01']),
    'as_of_from': pd.to_datetime(['2025-01-01', '2025-01-01']),
    'as_of_to': pd.to_datetime(['2262-04-11', '2262-04-11'])
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
    'as_of_to': pd.to_datetime(['2262-04-11'])
})

# Process updates (delta mode - incremental updates)
rows_to_expire, rows_to_insert = processor.compute_changes(
    current_state, 
    updates,
    update_mode='delta'
)

print(f"Records to expire: {len(rows_to_expire)}")
print(f"Records to insert: {len(rows_to_insert)}")

# Process updates (full_state mode - complete state replacement)
rows_to_expire, rows_to_insert = processor.compute_changes(
    current_state, 
    updates,
    update_mode='full_state'
)
```

### Low-Level Arrow API

```python
import pandas as pd
from pytemporal import compute_changes
import pyarrow as pa

# Convert pandas DataFrames to Arrow RecordBatches
current_batch = pa.RecordBatch.from_pandas(current_state)
updates_batch = pa.RecordBatch.from_pandas(updates)

# Direct Arrow processing
expire_indices, insert_batches, expired_batches = compute_changes(
    current_batch,
    updates_batch,
    id_columns=['id', 'field'],
    value_columns=['mv', 'price'],
    system_date='2025-07-27',
    update_mode='delta'
)
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

The algorithm supports two distinct update modes that determine how updates interact with existing current state:

#### 1. Delta Mode (default)
- **Behavior**: Only provided records are treated as updates
- **Existing State**: Preserved where not temporally overlapped by updates
- **Use Case**: Incremental updates where you only want to modify specific time periods
- **Processing**: Uses timeline-based algorithm to handle temporal overlaps

#### 2. Full State Mode
- **Behavior**: Provided records represent the complete desired state for each ID group
- **Existing State**: Only preserved if identical records exist in the updates (same values and temporal ranges)
- **Value Comparison**: Records are compared using SHA256 hashes of value columns
- **Processing Rules**:
  - **Unchanged Records**: If an update has identical values and temporal range as current state, neither expire nor insert
  - **Changed Records**: If values differ, expire old record and insert new record
  - **New Records**: Insert records that don't exist in current state
  - **Deleted Records**: Records not present in updates are expired AND get tombstone records created
- **Tombstone Records**: When records are deleted (exist in current state but not in updates):
  - The original record is expired with its original `as_of_from`
  - A tombstone record is created with the same ID and values but `effective_to = system_date`
  - This maintains complete audit trail showing exactly when data became inactive

#### Full State Mode Example

**Current State:**
```
ID=1, mv=100, price=250, effective=[2020-01-01, INFINITY]
ID=2, mv=300, price=400, effective=[2020-01-01, INFINITY]  
ID=3, mv=500, price=600, effective=[2020-01-01, INFINITY] 
```

**Updates (Full State):**
```
ID=1, mv=150, price=250, effective=[2020-01-01, 2020-02-01]  # Values changed
ID=2, mv=300, price=400, effective=[2020-01-01, INFINITY]   # Values identical  
# Note: ID=3 is not in updates = deleted
```

**Result:**
- **Expire**: ID=1 (values changed), ID=3 (deleted)
- **Insert**: 
  - ID=1 (new values with updated effective_to)
  - ID=3 (tombstone: same values but effective_to=system_date)
- **No Action**: ID=2 (identical values, no change needed)

**Tombstone Example:**
```
Original:  ID=3, mv=500, price=600, effective=[2020-01-01, INFINITY]
Tombstone: ID=3, mv=500, price=600, effective=[2020-01-01, 2025-08-30]  # Truncated to system_date
```

### Timezone Handling

The library seamlessly handles timezone-aware timestamps from databases like PostgreSQL:

```python
# PostgreSQL timestamptz columns (timezone-aware)
current_state = pd.DataFrame({
    'id': [1],
    'value': ['test'],
    'effective_from': pd.Timestamp("2024-01-01", tz='UTC'),  # timezone-aware
    'effective_to': pd.Timestamp("2099-12-31", tz='UTC'),
    # ... other columns
})

# Client updates (often timezone-naive)  
updates = pd.DataFrame({
    'id': [1],
    'value': ['updated'],
    'effective_from': pd.Timestamp("2024-01-02"),  # timezone-naive
    'effective_to': pd.Timestamp("2099-12-31"),
    # ... other columns
})

# Automatically handles timezone normalization
rows_to_expire, rows_to_insert = processor.compute_changes(current_state, updates)
```

**Key Features:**
- **Automatic Schema Normalization**: Mixed timezone-aware and timezone-naive timestamps are automatically reconciled
- **PostgreSQL Compatible**: Seamless integration with `timestamptz` columns
- **Timezone Preservation**: Original timezone information is preserved throughout the processing pipeline
- **Arrow Schema Compatibility**: Ensures consistent `timestamp[us, tz=UTC]` schemas for Rust processing

### Parallelization Strategy

The algorithm uses adaptive parallelization:
- **Serial Processing**: Small datasets (<50 ID groups AND <10k records) 
- **Parallel Processing**: Large datasets using Rayon for CPU-bound operations
- **ID Group Independence**: Each ID group processes independently, enabling perfect parallelization

## Performance

### Rust Core Performance
Benchmarked on modern hardware:

- **500k records**: ~885ms processing time
- **Adaptive Parallelization**: Automatically uses multiple threads for large datasets  
- **Parallel Thresholds**: >50 ID groups OR >10k total records triggers parallel processing
- **Conflation Efficiency**: Significant row reduction for datasets with temporal continuity

### Python Wrapper Performance (Optimized)
Advanced batch consolidation eliminates conversion bottlenecks:

- **Batch Consolidation**: Individual record batches consolidated into 10k-row batches
- **Conversion Overhead**: <0.1 seconds for large datasets (was 30+ seconds)
- **Overhead Ratio**: Python processing is only 20% of Rust processing time
- **Zero-Copy Efficiency**: Near-optimal Arrow/pandas conversion performance

**Example Performance (50k records):**
```
Rust processing:     0.5s
Python conversion:   0.05s  
Total time:          0.55s
Overhead ratio:      0.1x (10% overhead)
```

**Before Optimization:**
- 90,213 single-row batches → 45+ seconds conversion time
- Massive per-batch overhead in arro3 → pandas conversion

**After Optimization:**  
- 10 consolidated 10k-row batches → 0.05 seconds conversion time
- Efficient bulk conversion with minimal overhead

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

### Requirements
- Rust (latest stable)
- Python 3.8+ with development headers
- uv for Python dependency management

### Python Version Compatibility
This project uses PyO3 v0.21.2 which officially supports Python up to 3.12. If you're using Python 3.13+:

**For Rust tests:**
```bash
export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1
cargo test
```

**For Python bindings (recommended):**
```bash
uv run maturin develop  # Uses venv Python 3.12 automatically
```

### Setup
```bash
# Install Rust dependencies and run tests
export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1  # If using Python 3.13+
cargo test

# Build Python extension (development mode)
uv run maturin develop

# Run Python tests  
uv run python -m pytest tests/test_bitemporal_manual.py -v
```

### Permanent Fix for Python 3.13+
Add to your shell profile (~/.bashrc or ~/.zshrc):
```bash
export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1
```

### Project Structure

**Modular Architecture** (274 lines total in main file, down from 1,085):

- `src/lib.rs` - Main processing function and Python bindings (280 lines)
- `src/types.rs` - Core data structures and constants (90 lines)
- `src/overlap.rs` - Overlap detection and record categorization (70 lines) 
- `src/timeline.rs` - Timeline event processing algorithm (250 lines)
- `src/conflation.rs` - Record conflation and batch consolidation (280 lines)
- `src/batch_utils.rs` - Arrow RecordBatch utilities with hash functions (490 lines)
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
4. **`conflation.rs`** - Post-processes results to merge adjacent segments and consolidate batches for optimal performance
5. **`batch_utils.rs`** - Arrow utilities for RecordBatch creation, timestamp handling, and SHA256 hash computation

## Dependencies

- **arrow** (53.4) - Columnar data processing
- **pyo3** (0.21) - Python bindings  
- **chrono** (0.4) - Date/time handling
- **sha2** (0.10) - SHA256 hashing for client-compatible hex digests
- **rayon** (1.8) - Parallel processing
- **criterion** (0.5) - Benchmarking framework

## Architecture

### Rust Core
- Zero-copy Arrow array processing
- Parallel execution with Rayon
- Hash-based change detection with SHA256 (client-compatible hex digests)
- Post-processing conflation for optimal storage
- Batch consolidation for efficient Python conversion
- Modular design with clear separation of concerns

### Python Interface
- High-level DataFrame API (`BitemporalTimeseriesProcessor`)
- Low-level Arrow API (`compute_changes`)
- Optimized batch conversion with minimal overhead
- PyO3 bindings for seamless integration
- Compatible with pandas DataFrames via efficient conversion

## Performance Monitoring

This project includes comprehensive performance monitoring with flamegraph analysis:

### 📊 Release Performance Reports

View performance metrics and flamegraphs for each release at:
**[Release Benchmarks](https://your-username.github.io/pytemporal/)**

Each version tag automatically generates comprehensive performance documentation with flamegraphs, creating a historical record of performance evolution across releases.

### 🔥 Generating Flamegraphs Locally

```bash
# Generate flamegraphs for key benchmarks
cargo bench --bench bitemporal_benchmarks medium_dataset -- --profile-time 5
cargo bench --bench bitemporal_benchmarks conflation_effectiveness -- --profile-time 5 
cargo bench --bench bitemporal_benchmarks "scaling_by_dataset_size/records/500000" -- --profile-time 5

# Add flamegraph links to HTML reports  
python3 scripts/add_flamegraphs_to_html.py

# View reports locally
python3 -m http.server 8000 --directory target/criterion
# Then visit: http://localhost:8000/report/
```

### 📈 Performance Expectations

| Dataset Size | Processing Time | Flamegraph Available |
|--------------|----------------|---------------------|
| Small (5 records) | ~30-35 µs | ❌ |
| Medium (100 records) | ~165-170 µs | ✅ |
| Large (500k records) | ~900-950 ms | ✅ |
| Conflation test | ~28 µs | ✅ |

### 🎯 Key Optimization Areas (from Flamegraph Analysis)

- **`process_id_timeline`**: Core algorithm logic
- **Rayon parallelization**: Thread management overhead
- **Arrow operations**: Columnar data processing
- **SHA256 hashing**: Value fingerprinting for conflation

See `docs/benchmark-publishing.md` for complete setup details.

## Contributing

1. Check `CLAUDE.md` for project context and conventions
2. Run tests before submitting changes
3. Follow existing code style and patterns
4. Update benchmarks for performance-related changes
5. Use flamegraphs to validate performance improvements
6. Maintain modular architecture when adding features

## License

MIT License - see LICENSE file for details.

## Built With

- [Apache Arrow](https://arrow.apache.org/) - Columnar data format
- [PyO3](https://pyo3.rs/) - Rust-Python bindings  
- [Rayon](https://github.com/rayon-rs/rayon) - Data parallelism
- [Criterion](https://github.com/bheisler/criterion.rs) - Benchmarking
- [SHA256](https://en.wikipedia.org/wiki/SHA-2) - Cryptographic hashing algorithm (client-compatible)