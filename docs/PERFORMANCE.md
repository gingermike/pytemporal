# Performance Characteristics

PyTemporal delivers world-class performance for bitemporal data processing.

## Current Performance Metrics

**Large Scale Processing (800k rows × 80 columns):**
- **Throughput**: 157,000+ rows/second
- **Processing Time**: ~5.4 seconds
- **Memory Usage**: ~14GB
- **Data Complexity**: 64 million cell evaluations with temporal logic

**Hash Computation Performance:**
- **XxHash**: 1.13M rows/second (16-character hashes) 
- **SHA256**: 925K rows/second (64-character hashes)
- **Arrow-Direct**: Zero deserialization overhead

**Python Wrapper Performance:**
- **Batch Consolidation**: <0.1 seconds conversion overhead
- **Memory Efficiency**: Optimized for large datasets

## Optimization History

### Function Inlining Breakthrough (2025-01-27)
The major performance breakthrough came from discovering that `create_id_key` was called 850,000+ times per dataset. Strategic function inlining with `#[inline(always)]` eliminated call overhead:

- **Before**: 60,000 rows/second
- **After**: 157,000+ rows/second  
- **Improvement**: +161% performance gain

### Key Optimizations
1. **Function Inlining**: Eliminated 850k+ function call overheads
2. **Array Caching**: Pre-extracted Arrow arrays to avoid repeated lookups
3. **String Buffer Reuse**: Reduced allocations in hot paths
4. **Aggressive Parallelization**: Tuned thresholds for modern systems
5. **Incremental Consolidation**: Memory optimization during processing

## Benchmarking

Run performance benchmarks:

```bash
cargo bench
```

Profile with flamegraphs:
```bash
cargo build --release --features profiling
uv run python validate_refactoring.py --profile
```

## Hardware Requirements

**Recommended for optimal performance:**
- Multi-core CPU (8+ cores)
- 16GB+ RAM for large datasets
- SSD storage for data loading

**Minimum requirements:**
- 4-core CPU
- 8GB RAM
- Works on any platform supported by Rust/Python