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

**Parameters:**
- `id_columns` (List[str]): Column names that identify unique entities
- `value_columns` (List[str]): Column names containing business values
- `system_date` (str/datetime): System date for temporal processing
- `update_mode` (str, optional): 'delta' (default) or 'full_state'
- `hash_algorithm` (str, optional): 'xxhash' (default) or 'sha256'

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