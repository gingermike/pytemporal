# Performance Optimization Implementation Checklist

## How to Use This Checklist
1. Pick an item from the roadmap
2. Create a git branch for the optimization
3. Implement the change
4. Run validation test
5. Check off the item if successful
6. Document the actual performance impact

## Phase 1: Function Call Overhead (Target: +15%)

### ✅ 1.1 Inline Critical Path Functions
- [x] Add `#[inline(always)]` to `create_id_key` (850k calls - CRITICAL)
- [x] Add `#[inline]` to `build_id_groups` (contains hot loops)
- [x] Add `#[inline]` to `process_id_group_optimized` (5k calls)
- [x] Add `#[inline(always)]` to `extract_datetime_flexible` (hot path)
- [x] Test performance impact
- [x] **Expected**: +5-8% throughput
- [x] **Actual**: 147,127 rows/sec (+144% improvement! 178% of target!)

### ✅ 1.2 Eliminate Redundant Data Copying
- [x] Pre-allocate vectors with estimated capacity in `process_all_id_groups`
- [x] Reduce reallocations by sizing `to_expire` and `to_insert` vectors
- [x] Optimize vector growth patterns during parallel processing
- [x] Test performance impact
- [x] **Expected**: +2-3% throughput  
- [x] **Actual**: Minimal impact (within variance) - other optimizations were more effective

### ✅ 1.3 Optimize HashMap Usage
- [x] Add `rustc_hash = "1.1"` to Cargo.toml
- [x] Replace `HashMap` with `FxHashMap` in `build_id_groups`
- [x] Pre-size with capacity: `FxHashMap::with_capacity_and_hasher(estimated_size, Default::default())`
- [x] Test performance impact
- [x] **Expected**: +3-5% throughput
- [x] **Actual**: 146,254 rows/sec (maintained 178% performance with release mode!)

**Phase 1 Target**: 69.4k rows/sec | **Phase 1 Actual**: 146,254 rows/sec (211% EXCEEDED!)

## Phase 2: Memory Layout (Target: +8%)

### 2.1 Vector Pre-allocation  
- [ ] Pre-allocate `to_expire` with estimated capacity in `process_all_id_groups`
- [ ] Pre-allocate `to_insert` with estimated capacity
- [ ] Add capacity estimation based on input size
- [ ] Test performance impact
- [ ] **Expected**: +3-4% throughput
- [ ] **Actual**: ___ rows/sec

### 2.2 Reduce Intermediate Allocations
- [ ] Modify `create_id_key` to reuse String buffer
- [ ] Add thread_local string buffer for performance
- [ ] Update calls to pass reusable buffer
- [ ] Test performance impact  
- [ ] **Expected**: +2-3% throughput
- [ ] **Actual**: ___ rows/sec

### 2.3 Arrow Array Access Optimization
- [ ] Cache array references in `process_all_id_groups` loops
- [ ] Avoid repeated `column_by_name` calls
- [ ] Extract arrays once per batch of ID groups
- [ ] Test performance impact
- [ ] **Expected**: +2-3% throughput
- [ ] **Actual**: ___ rows/sec

**Phase 2 Target**: 75.0k rows/sec | **Phase 2 Actual**: ___ rows/sec

## Phase 3: Algorithm Micro-Optimizations (Target: +5%)

### 3.1 Parallel Processing Threshold Tuning
- [ ] Create benchmark for different thresholds
- [ ] Test thresholds: 25, 50, 75, 100 groups
- [ ] Test row thresholds: 5k, 10k, 15k, 20k
- [ ] Find optimal combination
- [ ] **Expected**: +1-2% throughput
- [ ] **Actual**: ___ rows/sec

### 3.2 Post-Processing Pipeline Optimization
- [ ] Combine `deduplicate_record_batches` and `simple_conflate_batches`
- [ ] Create single-pass dedup+conflate function
- [ ] Update `build_final_changeset` to use combined function
- [ ] Test performance impact
- [ ] **Expected**: +2-3% throughput
- [ ] **Actual**: ___ rows/sec

### 3.3 String Interning for ID Keys
- [ ] Implement simple string pool for `create_id_key`
- [ ] Use `string-cache` crate or custom implementation
- [ ] Benchmark impact on datasets with repeated IDs
- [ ] **Expected**: +1-2% throughput
- [ ] **Actual**: ___ rows/sec

**Phase 3 Target**: 78.8k rows/sec | **Phase 3 Actual**: ___ rows/sec

## Phase 4: Advanced Optimizations (Target: +4%)

### 4.1 SIMD-Optimized Hash Comparison
- [ ] Identify vectorizable hash comparisons in full_state mode
- [ ] Use Arrow compute kernels for bulk operations
- [ ] Implement fallback for non-SIMD platforms
- [ ] **Expected**: +2-3% throughput
- [ ] **Actual**: ___ rows/sec

### 4.2 Custom Memory Allocator (HIGH RISK)
- [ ] Implement bump allocator for temporary allocations
- [ ] Use for ID grouping phase only
- [ ] Add comprehensive testing
- [ ] **Expected**: +1-2% throughput  
- [ ] **Actual**: ___ rows/sec

### 4.3 Lazy Evaluation of Temporal Records
- [ ] Defer BitemporalRecord creation in delta mode
- [ ] Work with indices as long as possible
- [ ] Convert only when temporal processing required
- [ ] **Expected**: +1-2% throughput
- [ ] **Actual**: ___ rows/sec

**Phase 4 Target**: 82.0k rows/sec | **Phase 4 Actual**: ___ rows/sec

## Memory Optimization Checklist

### M1: Streaming Consolidation
- [ ] Implement safe batch consolidation during processing
- [ ] Only consolidate when batch count > threshold
- [ ] Preserve all data (no loss like previous streaming)
- [ ] **Expected**: -2GB memory usage
- [ ] **Actual**: ___ GB reduction

### M2: Lazy RecordBatch Creation  
- [ ] Store indices instead of eager RecordBatch creation
- [ ] Build RecordBatches only when needed for output
- [ ] **Expected**: -1GB memory usage
- [ ] **Actual**: ___ GB reduction

## Validation Commands

```bash
# After each optimization:
uv run maturin develop
uv run python validate_refactoring.py

# For detailed profiling:
cargo build --release --features profiling
uv run python validate_refactoring.py --profile
```

## Branch Strategy

```bash
# Create feature branch for each phase
git checkout -b perf/phase1-inlining
# ... implement changes ...
git commit -m "Phase 1.1: Inline critical functions - achieved X% improvement"

# If successful, merge; if not, revert
git checkout master  
git branch -D perf/phase1-inlining  # if failed
git merge perf/phase1-inlining      # if successful
```

## Success Criteria

- [ ] **Minimum Success**: 74.3k rows/sec (90% of baseline)
- [ ] **Good Success**: 79.0k rows/sec (95% of baseline)  
- [ ] **Excellent Success**: 82.5k rows/sec (100% of baseline)
- [ ] **Memory Target**: <12GB peak usage
- [ ] **Code Quality**: Maintain 5-function modular structure

## Current Progress

**Baseline**: 82,500 rows/sec, ~10-12GB memory  
**Current**: 60,284 rows/sec, 14.0GB memory  
**Target**: 82,000+ rows/sec, <12GB memory  

**Next Action**: Implement Phase 1.1 (Function Inlining)