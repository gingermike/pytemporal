# Quick Start: Next Optimization Session

## Current Status Summary
- **Architecture**: ✅ COMPLETE - Clean 5-function modular design
- **Performance**: 60,284 rows/sec (73% of 82.5k target) 
- **Gap**: 22,216 rows/sec improvement needed

## Immediate Next Action: Phase 1.1 Function Inlining

### Why This First?
- **Highest Impact**: +5-8% expected throughput gain
- **Lowest Risk**: Simple compiler directive, preserves code structure  
- **Quick Win**: 15 minutes to implement, immediate results

### Implementation Steps

1. **Add inline directives** to `src/lib.rs`:
```rust
#[inline(always)]
fn prepare_inputs(
    // ... existing function
```

```rust  
#[inline(always)]
fn build_id_groups(
    // ... existing function
```

```rust
#[inline(always)] 
fn handle_empty_inputs(
    // ... existing function
```

2. **Test the change**:
```bash
uv run maturin develop
uv run python validate_refactoring.py
```

3. **Expected Result**: ~64,000 rows/sec (up from 60,284)

### If Successful → Phase 1.2 (Eliminate Copying)
### If Failed → Investigate and revert

## Complete Implementation Order

### Week 1: Foundation (Low Risk, High Impact)
1. **✅ TODAY**: Phase 1.1 - Function inlining (15 min)
2. Phase 1.3 - FxHashMap optimization (30 min)  
3. Phase 2.1 - Vector pre-allocation (45 min)
4. Phase 1.2 - Eliminate copying (1 hour)

**Weekly Target**: 69.4k rows/sec (85% recovery)

### Week 2: Optimization (Medium Risk, Good Impact)  
5. Phase 2.2 - Reduce allocations (45 min)
6. Phase 2.3 - Arrow array caching (1 hour)
7. Phase 3.1 - Parallel threshold tuning (2 hours)

**Weekly Target**: 75.0k rows/sec (91% recovery)

### Week 3: Advanced (Higher Risk, Final Push)
8. Phase 3.2 - Pipeline optimization (3 hours)
9. Memory optimization M1 - Safe consolidation (3 hours)
10. Final profiling and tuning (4 hours)

**Weekly Target**: 82.0k rows/sec (99% recovery)

## Emergency Rollback Plan
```bash
git stash  # Quick rollback during development
git reset --hard HEAD~1  # Permanent rollback if committed
```

## Validation After Each Step
- Performance must be ≥ previous step
- Memory must be ≤ previous step  
- All tests must pass

---

**START HERE**: Open `src/lib.rs` and add `#[inline(always)]` to the first three functions listed above.