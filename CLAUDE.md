# Bitemporal Timeseries Project Context

## Project Overview
This is a high-performance Rust implementation of a bitemporal timeseries algorithm with Python bindings. The system processes financial/trading data with two time dimensions: effective time (when events occurred in the real world) and as-of time (when information was recorded in the system).

## Key Concepts
- **Bitemporal Data**: Records have both `effective_from/to` and `as_of_from/to` dates
- **Conflation**: Post-processing step that merges adjacent segments with identical value hashes to reduce database rows
- **Value Hash**: xxHash-based fingerprint of value columns to detect identical records for conflation
- **ID Groups**: Records are grouped by ID columns, algorithm processes each group independently enabling parallelization

## Project Structure
- `src/lib.rs` - Core bitemporal algorithm with parallel processing (870 lines)
- `tests/integration_tests.rs` - Rust integration tests (5 comprehensive test scenarios)
- `tests/test_bitemporal_manual.py` - Python test suite (legacy, comprehensive scenarios)
- `benches/bitemporal_benchmarks.rs` - Criterion benchmarks (5 benchmark suites)
- `Cargo.toml` - Dependencies and build configuration

## Key Commands
- **Test Rust**: `cargo test`
- **Test Python**: `uv run python -m pytest tests/test_bitemporal_manual.py -v`
- **Benchmark**: `cargo bench`
- **Build Release**: `cargo build --release`
- **Build Python Wheel**: `uv run maturin develop` or `uv run maturin build --release`
- **Python Environment**: Use `uv` for all Python commands to ensure proper virtual environment usage

## CRITICAL Development Workflow
**⚠️  IMPORTANT**: After making changes to Rust code (src/lib.rs), you MUST rebuild the Python bindings with `uv run maturin develop` before running Python tests. Changes to Rust code are NOT automatically reflected in Python tests until you rebuild the bindings. This has caused confusion in the past where fixes appeared not to work when they actually did.

## Performance Characteristics
- **Baseline**: ~1.5s for 500k records (serial processing)
- **Optimized**: ~885ms for 500k records (40% improvement with adaptive parallelization)
- **Parallelization**: Uses Rayon, adaptive thresholds (>50 ID groups OR >10k total records)
- **Conflation**: Reduces output rows by merging adjacent segments with same value hash

## Algorithm Details
- **Input**: Current state RecordBatch + Updates RecordBatch  
- **Output**: ChangeSet with `to_expire` and `to_insert` batches
- **Processing**: Groups by ID columns, processes each group independently
- **Conflation**: Post-processes results to merge adjacent same-value segments
- **Update Modes**: 
  - **Delta (default)**: Updates modify existing effective periods using timeline-based processing
  - **Full State**: Updates represent complete desired state; only expire/insert when values actually change (SHA256 hash comparison)
- **Temporal Precision**: Effective dates use Date32, as_of timestamps use TimestampMicrosecond for second-level precision

## Dependencies & Purpose
- `arrow` (53.4) - Columnar data processing, RecordBatch format
- `pyo3` (0.21) - Python bindings with extension-module feature
- `pyo3-arrow` (0.3) - Arrow integration for Python
- `chrono` (0.4) - Date/time handling
- `sha2` (0.10) - SHA256 hashing for value fingerprints (client-compatible hex digests)
- `rayon` (1.8) - Data parallelism
- `ordered-float` (4.2) - Hash-able floating point values
- `criterion` (0.5) - Professional benchmarking framework

## Test Coverage
- **Rust Tests**: 5 scenarios covering head slice, tail slice, unsorted data, overwrite, no-changes
- **Python Tests**: 7 scenarios including complex multi-update and multiple current state scenarios
- **Benchmarks**: Small/medium datasets, conflation effectiveness, scaling tests, parallel effectiveness

## Design Decisions
- **Adaptive Parallelization**: Serial for small datasets to avoid overhead, parallel for large ones
- **Post-processing Conflation**: Simpler than inline conflation, maintains algorithm correctness
- **Arrow Format**: Efficient columnar processing, zero-copy operations where possible
- **Separate Test Files**: Tests moved out of lib.rs for better organization

## Previous Challenges Solved
- **Test Compilation**: Fixed by making core functions public and updating crate-type to include "rlib"
- **StringArray Builder**: Updated to use StringBuilder::new() instead of deprecated constructor
- **Performance Regression**: Fixed by implementing adaptive parallelization thresholds
- **Project Organization**: Separated tests, benchmarks, and core algorithm into appropriate files

## Gitea CI/CD
- **Workflow**: `.gitea/workflows/build-wheels.yml` - Complete Linux wheel building and publishing
- **Architectures**: x86_64 and aarch64 cross-compilation support
- **Publishing**: Automatic publishing to Gitea package registry on version tags
- **Setup Guide**: `docs/gitea-publishing.md` - Complete configuration instructions
- **Triggers**: Version tags (`v1.0.0`) and manual workflow dispatch

## Notes
- Algorithm is deterministic and thread-safe when processing different ID groups
- Conflation maintains temporal correctness while optimizing storage
- Python integration allows seamless DataFrame → RecordBatch conversion with timestamp precision preservation
- Benchmarks show excellent scaling characteristics with parallelization
- **Timestamp Precision**: as_of columns preserve microsecond precision through the entire pipeline (Python → Arrow → Rust → Arrow → Python)
- **Python Conversion**: Automatic handling of pandas timestamp[ns] → Arrow timestamp[us] conversion for Rust compatibility
- **Infinity Handling**: Uses `2260-12-31 23:59:59` as infinity representation (avoids NaT, maintains datetime type, overflow-safe)

## Recent Updates
- **2025-07-27**: Implemented microsecond timestamp precision for as_of_from/as_of_to columns
- **2025-07-27**: Fixed Python wrapper to preserve exact timestamps from input through processing  
- **2025-07-27**: Updated all tests and benchmarks to use proper timestamp schemas
- **2025-07-27**: Fixed infinity handling to use `2260-12-31 23:59:59` instead of NaT for clear debugging
- **2025-07-27**: Added complete Gitea Actions workflow for Linux wheel building and publishing
- **2025-08-04**: Fixed non-overlapping update issue where current state records were incorrectly re-emitted when updates had same ID but no temporal overlap. Enhanced `process_id_timeline` to separate overlapping vs non-overlapping updates and process them appropriately.
- **2025-08-04**: Fixed `as_of_from` timestamp inheritance issue where re-emitted current state segments retained old timestamps instead of inheriting the update's `as_of_from` timestamp. Modified `emit_segment` to pass and use update timestamps for current state records affected by overlapping updates.
- **2025-08-29**: Changed hash implementation from Blake3 numeric values to SHA256 hex digest strings for client compatibility. Updated all schemas, tests, and benchmarks to use string-based hashes. Hash values now match Python's `hashlib.sha256().hexdigest()` format.
- **2025-08-29**: Fixed full_state mode logic that was incorrectly expiring ALL current records regardless of value changes. Updated implementation to only expire/insert records when values actually change, using SHA256 hash comparison for efficiency. This prevents unnecessary database operations for unchanged records in full_state mode.
- **2025-08-30**: Implemented tombstone record creation for full_state mode. When records exist in current state but not in updates (deletion scenario), the system now creates proper tombstone records with effective_to=system_date to maintain complete bitemporal audit trail. All 23 Rust tests and 24 Python tests pass.
- **2025-08-30**: Major performance optimization through batch consolidation. Added consolidate_final_batches() function that combines many small single-row batches into fewer large 10k-row batches, reducing Python conversion overhead from 30+ seconds to <0.1 seconds for large datasets. Achieved 300x+ performance improvement in Python wrapper while preserving all functionality.
- **2025-09-02**: Implemented chunked processing to solve extreme memory usage issues. Added `compute_changes_chunked()` function that processes large datasets in configurable chunks (default 50k rows) to prevent 32GB+ memory usage. Successfully tested with 350k rows × 15 columns using only ~1GB memory vs 32GB+ without chunking. Function available as `pytemporal.compute_changes_chunked()` with optional chunk_size parameter.
- **2025-09-06**: Implemented configurable hash algorithms with XxHash as default and SHA256 for legacy compatibility. Added Arrow-direct hash computation that bypasses expensive Arrow→Rust serialization, achieving 57% performance improvement (717K→1.13M rows/sec). XxHash provides 22% better performance than SHA256 while using 44x less memory during computation.
- **2025-09-06**: Achieved ultra-high performance milestone: 800k rows × 80 columns processed in 10.0 seconds (88K rows/sec throughput) with optimized chunked processing. Memory usage reduced to 506MB vs 8.8GB without chunking (17x improvement). All compiler warnings eliminated and codebase cleaned up with Arrow-direct hashing as the standard implementation.

## RESOLVED: Full State Mode Tombstone Records ✅

### ✅ **ISSUE RESOLVED (2025-08-30)**
Full state mode now correctly handles deletion scenarios by creating tombstone records.

### **Implementation Completed:**
1. **Enhanced full_state logic** to detect deleted records (exist in current, not in updates)
2. **Tombstone record creation** for deleted records with:
   - Same ID values and value columns as expired record
   - `effective_to = system_date` (truncate the effective period)
   - `as_of_from = system_date` (new knowledge timestamp)
   - `as_of_to = INFINITY`
3. **Integrated tombstone records** into insert batch alongside regular updates

### **Testing Completed:**
- ✅ Fixed test `full_state_delete` - now passes
- ✅ All existing functionality preserved - 23/23 Rust tests pass, 24/24 Python tests pass
- ✅ Tombstone behavior working correctly in both Rust and Python APIs

## MAJOR PERFORMANCE OPTIMIZATION: Batch Consolidation ✅

### ✅ **ISSUE RESOLVED (2025-08-30)**
Eliminated massive Python conversion overhead through intelligent batch consolidation.

### **Problem Solved:**
- **Before**: 90,213+ single-row batches causing 30+ seconds conversion overhead
- **After**: 10-11 large batches (10k rows each) with <0.1 seconds conversion overhead
- **Performance Improvement**: 300x+ reduction in conversion time

### **Implementation Details:**
1. **Root Cause**: Timeline processing created individual single-row batches for each segment
2. **Solution**: Added `consolidate_final_batches()` function that:
   - Combines small batches from different ID groups into large consolidated batches
   - Targets 10k rows per batch for optimal Arrow/pandas conversion
   - Maintains schema compatibility and handles all data types
   - Only consolidates when beneficial (skips already-large batches)

### **Performance Results:**
```
Dataset Size: 50k records
Before: 90,213 batches → 45+ seconds conversion
After:  10 batches → 0.05 seconds conversion  
Improvement: 900x faster Python wrapper
```

### **Architecture Benefits:**
- **Clean Separation**: Timeline algorithm unchanged, consolidation is pure optimization
- **Maintainable**: Easy to disable consolidation for debugging
- **Future-Proof**: Adapts to different ID group counts and batch sizes
- **Zero-Copy Performance**: Achieves intended near zero-copy Arrow performance

## MAJOR MEMORY OPTIMIZATION: Chunked Processing ✅

### ✅ **ISSUE RESOLVED (2025-09-02)**
Implemented chunked processing to solve extreme memory usage problems with large datasets.

### **Problem Solved:**
- **Before**: 350k rows × 15 columns consumed 32GB+ memory and often crashed
- **After**: Same dataset processes with only ~1GB memory usage using chunked approach
- **Root Cause**: Massive intermediate `BitemporalRecord` creation and single-row batch proliferation

### **Implementation Details:**
1. **New Function**: `compute_changes_chunked()` with configurable chunk size (default 50k rows)
2. **Chunked Processing**: Splits both current_state and updates into manageable chunks
3. **Memory-Safe**: Each chunk processed independently with bounded memory usage
4. **Result Consolidation**: Automatically merges and deduplicates results from all chunks
5. **Backward Compatible**: Falls back to regular processing for small datasets

### **Performance Results:**
```
Test Scenario: 350k current + 35k updates, 15 columns
Regular (50k subset): 133MB memory increase, 0.73s
Chunked (full 350k):   1024MB memory increase, 8.4s  
Improvement: 30x+ memory reduction, handles full dataset
```

### **Usage:**
```python
import pytemporal

# Use chunked processing for large datasets
expire_indices, insert_batches, expired_batches = pytemporal.compute_changes_chunked(
    current_batch, updates_batch, id_columns, value_columns,
    system_date='2023-02-01', update_mode='delta', chunk_size=50000
)
```

### **Optimal Chunk Sizes:**
- **25k**: Slower but lowest memory usage
- **50k**: Balanced performance and memory (recommended default)  
- **75k-100k**: Fastest but higher memory usage
- **Auto-selection**: Use regular processing for datasets < chunk_size

## Development Best Practices
- Ensure to keep README.md up to date with changes to the code base or approach
- **For Large Datasets**: Always use `compute_changes_chunked()` for datasets > 100k rows
- **Memory Monitoring**: Test with realistic data sizes during development
- ✅ **RESOLVED**: All critical issues resolved, memory optimized, and production-ready

This file should be updated whenever a new piece of context or information is added / discovered in this project