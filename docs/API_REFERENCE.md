# API Reference

Complete API documentation for PyTemporal library.

## High-Level DataFrame API (Recommended)

### BitemporalTimeseriesProcessor

```python
from pytemporal import BitemporalTimeseriesProcessor

processor = BitemporalTimeseriesProcessor(
    id_columns=['id', 'field'],
    value_columns=['mv', 'price']
)

result = processor.process_updates(
    current_state=current_df,
    updates=updates_df,
    system_date='2025-01-27'
)
```

**Constructor Parameters:**
- `id_columns` (List[str]): Column names that identify unique entities
- `value_columns` (List[str]): Column names containing business values
- `conflate_inputs` (bool, optional): Merge consecutive updates with same values (default: False)

**Method Parameters:**
- `system_date` (str/datetime): System date for temporal processing
- `update_mode` (str, optional): 'delta' (default) or 'full_state'
- `hash_algorithm` (str, optional): 'xxhash' (default) or 'sha256'
- `conflate_inputs` (bool, optional): Override class-level conflation setting

**Returns:**
- `ProcessingResult` object with `to_expire`, `to_insert`, and `expired_records`

### ProcessingResult

```python
result = processor.process_updates(...)

# Access results
expired_rows = result.to_expire  # List of row indices
new_records = result.to_insert   # DataFrame with new/modified records
audit_records = result.expired_records  # DataFrame with expired records

# Convert to DataFrame
changes_df = result.to_dataframe()
```

## Low-Level Arrow API

For advanced users requiring direct Arrow operations:

```python
from pytemporal import process_updates
import pyarrow as pa

# Convert pandas to Arrow
current_batch = pa.RecordBatch.from_pandas(current_df)
updates_batch = pa.RecordBatch.from_pandas(updates_df)

# Process with Arrow API
changeset = process_updates(
    current_state=current_batch,
    updates=updates_batch, 
    id_columns=['id', 'field'],
    value_columns=['mv', 'price'],
    system_date=date(2025, 1, 27),
    update_mode='delta'
)
```

## Data Schema Requirements

### Required Columns

All DataFrames must include:
- **ID columns**: Identify unique entities (configurable names)
- **Value columns**: Business data to track changes (configurable names)  
- `effective_from`: When record becomes effective
- `effective_to`: When record stops being effective
- `as_of_from`: When information was known
- `as_of_to`: When information was superseded

### Supported Data Types

**Temporal Columns:**
- `effective_from/to`: Date32, Date64, or any Timestamp type
- `as_of_from/to`: TimestampSecond/Millisecond/Microsecond/Nanosecond

**ID/Value Columns:**
- String/Utf8
- Integer types (Int8, Int16, Int32, Int64)
- Float types (Float32, Float64)
- Boolean
- Date types
- Null values supported

### Example Schema

```python
import pandas as pd

df = pd.DataFrame({
    'id': [1234, 1235],                    # ID column
    'field': ['test', 'price'],            # ID column  
    'mv': [300.0, 400.0],                  # Value column
    'price': [150.0, 200.0],               # Value column
    'effective_from': ['2020-01-01', '2020-01-01'],
    'effective_to': ['2021-01-01', '2021-01-01'],
    'as_of_from': ['2025-01-27', '2025-01-27'],
    'as_of_to': ['2262-04-11', '2262-04-11']  # Infinity representation
})
```

## Update Modes

### Delta Mode (Default)
Updates modify existing effective periods using timeline-based processing:
```python
processor = BitemporalTimeseriesProcessor(
    id_columns=['id'],
    value_columns=['value'],
    update_mode='delta'  # Default
)
```

### Full State Mode
Updates represent complete desired state. Only changes when values differ:
```python
processor = BitemporalTimeseriesProcessor(
    id_columns=['id'],
    value_columns=['value'],
    update_mode='full_state'
)
```

## Hash Algorithms

### XxHash (Default)
Fast, non-cryptographic hash for performance:
```python
processor = BitemporalTimeseriesProcessor(
    id_columns=['id'],
    value_columns=['value'],
    hash_algorithm='xxhash'  # Default
)
```

### SHA256
Cryptographic hash for legacy compatibility:
```python
processor = BitemporalTimeseriesProcessor(
    id_columns=['id'],
    value_columns=['value'],
    hash_algorithm='sha256'
)
```

## Input Conflation

Input conflation merges consecutive update records with the same ID and values **before** timeline processing. This is useful when receiving non-conflated data from external sources.

### When to Use

**✅ Enable conflation when:**
- External data feeds provide redundant consecutive segments
- Source systems don't perform conflation
- You want to optimize processing and reduce output rows

**⚠️ Skip conflation when:**
- Data is already conflated
- Processing small datasets (<1k rows)
- You need maximum performance and know data has no redundancy

### Performance Impact

- **Overhead**: ~6-8% when no conflation opportunities exist
- **Benefit**: +9-11% speedup when 80% of records can be conflated
- **Row Reduction**: Can reduce output by 50-75% with consecutive same-value segments

### Usage Examples

**Class-Level Configuration (applies to all calls):**
```python
processor = BitemporalTimeseriesProcessor(
    id_columns=['id', 'field'],
    value_columns=['mv', 'price'],
    conflate_inputs=True  # Enable for all process_updates calls
)

expire, insert = processor.compute_changes(current_state, updates)
```

**Per-Call Override:**
```python
processor = BitemporalTimeseriesProcessor(
    id_columns=['id', 'field'],
    value_columns=['mv', 'price']
    # conflate_inputs defaults to False
)

# Enable for specific call
expire, insert = processor.compute_changes(
    current_state,
    updates,
    conflate_inputs=True  # Override class default
)
```

**Example: Conflation in Action**
```python
import pandas as pd
from pytemporal import BitemporalTimeseriesProcessor

# Input: 4 records with consecutive dates and same values
updates = pd.DataFrame([
    [1234, "test", 100, 200, "2020-01-01", "2020-06-01", ...],
    [1234, "test", 100, 200, "2020-06-01", "2020-12-01", ...],  # Same values
    [5678, "demo", 50, 75, "2020-01-01", "2020-06-01", ...],
    [5678, "demo", 50, 75, "2020-06-01", "2020-12-01", ...],    # Same values
], columns=['id', 'field', 'mv', 'price', 'effective_from', 'effective_to', ...])

processor = BitemporalTimeseriesProcessor(
    id_columns=['id', 'field'],
    value_columns=['mv', 'price'],
    conflate_inputs=True
)

_, insert = processor.compute_changes(
    current_state=pd.DataFrame(columns=updates.columns),
    updates=updates,
    update_mode='full_state'
)

# Output: 2 records (conflated)
# [1234, "test", 100, 200, "2020-01-01", "2020-12-01", ...]
# [5678, "demo", 50, 75, "2020-01-01", "2020-12-01", ...]
assert len(insert) == 2
```

### How It Works

Conflation happens during input preparation:

1. **Grouping**: Records grouped by ID columns
2. **Sorting**: Within each group, sorted by `effective_from`
3. **Scanning**: Consecutive records checked for:
   - Same `value_hash` (identical business values)
   - Adjacent dates (`effective_to[i] == effective_from[i+1]`)
4. **Merging**: Qualifying records merged by extending `effective_to` of first record

This reduces the number of records flowing through timeline processing, batch consolidation, and Python conversion.

## Error Handling

```python
try:
    result = processor.process_updates(current_state, updates, system_date)
except Exception as e:
    print(f"Processing error: {e}")
    # Handle error appropriately
```

Common errors:
- Missing required columns
- Invalid date formats  
- Schema mismatches between current state and updates
- Memory errors on very large datasets