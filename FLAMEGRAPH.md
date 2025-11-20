# Flamegraph Performance Analysis Guide

This document explains how to generate flamegraphs for analyzing performance hotspots in the bitemporal timeseries processing algorithm.

## Prerequisites

The project is now configured with `pprof` and `criterion` for performance profiling:

- `pprof = { version = "0.13", features = ["flamegraph", "criterion"] }` - Added to dev-dependencies
- Benchmark configuration updated to use `PProfProfiler`

## Current Setup

The `benches/bitemporal_benchmarks.rs` file includes pprof profiling configuration:

```rust
use pprof::criterion::{Output, PProfProfiler};

fn main() {
    let mut criterion = Criterion::default()
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    
    // Run benchmarks with profiling enabled
    bench_parallel_effectiveness(&mut criterion);
    criterion.final_summary();
}
```

## Running Profiled Benchmarks

To generate flamegraphs, run benchmarks with the `--profile-time` flag:

```bash
# Generate flamegraph for a specific benchmark (5 second profiling)
cargo bench --bench bitemporal_benchmarks medium_dataset -- --profile-time 5

# Generate flamegraph for the large dataset benchmark
cargo bench --bench bitemporal_benchmarks "scaling_by_dataset_size/records/500000" -- --profile-time 5

# Generate flamegraph for conflation effectiveness
cargo bench --bench bitemporal_benchmarks conflation_effectiveness -- --profile-time 5
```

Flamegraphs will be saved to: `target/criterion/<benchmark_name>/profile/flamegraph.svg`

**Example generated files**:
- `target/criterion/medium_dataset/profile/flamegraph.svg`
- `target/criterion/conflation_effectiveness/profile/flamegraph.svg`  
- `target/criterion/scaling_by_dataset_size/records/500000/profile/flamegraph.svg`

## Alternative Profiling Methods

### Method 1: Using perf and flamegraph tools

1. **Install perf and flamegraph tools**:
   ```bash
   # Install perf (Linux)
   sudo apt-get install linux-perf

   # Install flamegraph
   cargo install flamegraph
   ```

2. **Profile with flamegraph**:
   ```bash
   # Profile the 500k records benchmark specifically
   cargo flamegraph --bench bitemporal_benchmarks -- --bench --filter "500000"
   
   # Or profile all benchmarks
   cargo flamegraph --bench bitemporal_benchmarks
   ```

3. **View generated flamegraph**:
   ```bash
   # Opens flamegraph.svg in your browser
   firefox flamegraph.svg
   ```

### Method 2: Using criterion with manual pprof conversion

1. **Generate profile data**:
   ```bash
   # Run benchmarks to generate profiling data
   cargo bench --bench bitemporal_benchmarks
   ```

2. **Look for profile files**:
   ```bash
   find ./target -name "*.pb" -o -name "*profile*" | head -10
   ```

3. **Convert to flamegraph** (if .pb files are found):
   ```bash
   # Install pprof tool
   go install github.com/google/pprof@latest
   
   # Convert protobuf to flamegraph
   pprof -http=localhost:8080 profile.pb
   ```

### Method 3: Simple perf profiling

1. **Run benchmark under perf**:
   ```bash
   perf record --call-graph dwarf cargo bench --bench bitemporal_benchmarks
   ```

2. **Generate report**:
   ```bash
   perf report
   ```

## Expected Performance Hotspots

Based on the algorithm structure, look for these areas in flamegraphs:

### Primary Hotspots
- **`process_id_timeline`** - Core timeline processing logic
- **Rayon parallel processing** - Parallelization overhead vs benefit
- **Arrow operations** - Columnar data manipulation
- **Hash computation** - Value hashing for change detection (`xxhash` by default, `sha256` for legacy)

### Secondary Areas
- **`has_temporal_intersection`** - Overlap detection logic
- **`simple_conflate_batches`** - Post-processing conflation
- **Memory allocation** - RecordBatch creation/destruction
- **Data type conversions** - Arrow type conversions

### Key Metrics to Analyze

1. **Serial vs Parallel Performance**:
   - Compare time spent in serial vs parallel sections
   - Look for Rayon overhead in small datasets

2. **Memory Access Patterns**:
   - Arrow array access patterns
   - Data locality issues

3. **Algorithmic Complexity**:
   - Time spent in nested loops
   - Hash computation frequency

## Interpreting Results

- **Wide bars**: Functions taking significant time (hotspots)
- **Tall stacks**: Deep call chains (potential optimization points)
- **Color coding**: Different colors represent different modules/functions

## Performance Expectations

From benchmarking, the current performance characteristics are:

- **Small datasets (10-100 records)**: ~15-170 µs
- **Medium datasets (500 records)**: ~800 µs
- **Large datasets (500k records)**: ~900-950 ms
- **Parallel effectiveness**: 5-10ms depending on workload distribution

Use flamegraphs to identify why certain scenarios perform differently and where optimization efforts should focus.

## Troubleshooting

If flamegraphs are not generated:

1. **Check pprof version compatibility**:
   ```bash
   cargo tree | grep pprof
   ```

2. **Verify criterion integration**:
   ```bash
   cargo check --bench bitemporal_benchmarks
   ```

3. **Use alternative tools**:
   - `cargo flamegraph` (most reliable)
   - `perf` + manual analysis
   - `valgrind` for memory profiling

## Next Steps

Once flamegraphs are generated:

1. Identify the top 5 functions consuming CPU time
2. Analyze parallel vs serial execution patterns
3. Look for unexpected memory allocations
4. Focus optimization efforts on the widest flame sections
5. Re-run benchmarks to measure improvement

This performance analysis will help guide targeted optimizations for the bitemporal processing algorithm.