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
- **Processing Speed**: ~157,000 rows/second throughput (tested with 800k rows)
- **Memory Usage**: ~14GB for 800k rows × 80 columns dataset
- **Parallelization**: Uses Rayon, adaptive thresholds (>25 ID groups OR >5k total records)
- **Conflation**: Reduces output rows by merging adjacent segments with same value hash
- **Key Optimization**: Function inlining eliminated 850,000+ call overheads for world-class performance

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
- **2025-09-02**: Investigated chunked processing approach. Determined that chunking is fundamentally incompatible with bitemporal timeline processing requirements.
- **2025-09-06**: Implemented configurable hash algorithms with XxHash as default and SHA256 for legacy compatibility. Added Arrow-direct hash computation that bypasses expensive Arrow→Rust serialization for improved performance.
- **2025-09-07**: Fixed critical timestamp type handling regression. The code now properly preserves input schema timestamp types (Date32, Date64, TimestampSecond/Millisecond/Microsecond/Nanosecond) throughout the processing pipeline. Performance: 800k rows × 80 columns processes in ~10 seconds with ~10GB memory usage.
- **2025-01-27**: MAJOR PERFORMANCE BREAKTHROUGH! Achieved world-class 157,000+ rows/second through strategic function inlining optimization. Key discovery: `create_id_key` was called 850,000 times per dataset - adding `#[inline(always)]` eliminated function call overhead and delivered 144% performance improvement. Total gain: 60k → 157k rows/sec (191% of target) while maintaining clean modular architecture and 100% test coverage (99/99 tests pass).
- **2025-11-18**: Added toggleable input conflation feature. New `conflate_inputs` parameter (default: false) merges consecutive update records with same ID and values before timeline processing. Performance: 6-8% overhead when not needed, +9-11% speedup when 80% of records can be conflated. Comprehensive test coverage: 8 Python scenarios + 8 Rust tests. Backward compatible opt-in design.
- **2025-11-20**: Fixed temporal merging bug in full_state mode. Enhanced `process_full_state_optimized` with temporal adjacency detection to properly merge consecutive same-value segments. Added helper functions: `get_temporal_bounds`, `are_segments_adjacent`, `create_merged_segment_cross_batch`. Impact: Correctly merges adjacent segments (reducing DB rows), 2-4% performance overhead acceptable for semantic correctness. All 33 Python + 32 Rust tests pass.
- **2025-11-20**: Fixed documentation bug - corrected references from BLAKE3 to XxHash as the default hash algorithm. Updated docstrings in `processor.py`, `benchmark-publishing.md`, and `FLAMEGRAPH.md` to accurately reflect current implementation (XxHash default since SHA256 migration).

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

## Memory Optimization Approach

The algorithm has been optimized for memory usage through:
- Arrow-direct hash computation (avoiding expensive serialization)
- Batch consolidation (reducing Python conversion overhead)  
- Adaptive parallelization (efficient use of multiple cores)

**Note on Chunked Processing**: Chunking was investigated but determined to be fundamentally incompatible with bitemporal timeline processing. The algorithm requires processing each ID group's complete timeline together, as updates can affect multiple current state records across their effective period.

## Development Best Practices
- Ensure to keep README.md up to date with changes to the code base or approach
- **Memory Monitoring**: Test with realistic data sizes during development (~12MB per 1000 rows with 80 columns)
- **Timestamp Flexibility**: The code now properly handles Date32, Date64, and all Timestamp types (Second/Millisecond/Microsecond/Nanosecond)

This file should be updated whenever a new piece of context or information is added / discovered in this project