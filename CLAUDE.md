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
- **Update Modes**: Delta (default) - updates modify existing effective periods
- **Temporal Precision**: Effective dates use Date32, as_of timestamps use TimestampMicrosecond for second-level precision

## Dependencies & Purpose
- `arrow` (53.4) - Columnar data processing, RecordBatch format
- `pyo3` (0.21) - Python bindings with extension-module feature
- `pyo3-arrow` (0.3) - Arrow integration for Python
- `chrono` (0.4) - Date/time handling
- `xxhash-rust` (0.8) - Fast hashing for value fingerprints
- `rayon` (1.8) - Data parallelism
- `indexmap` (2.1) - Ordered hash maps for deterministic processing
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

This file should be updated whenever a new piece of context or information is added / discovered in this project