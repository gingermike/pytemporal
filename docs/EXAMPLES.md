# Usage Examples

Practical examples for common bitemporal data processing scenarios.

## Basic Usage

### Simple Price Updates

```python
import pandas as pd
from pytemporal import BitemporalTimeseriesProcessor

# Initialize processor
processor = BitemporalTimeseriesProcessor(
    id_columns=['symbol'],
    value_columns=['price']
)

# Current state: AAPL at $150
current_state = pd.DataFrame({
    'symbol': ['AAPL'],
    'price': [150.0],
    'effective_from': pd.to_datetime(['2025-01-01']),
    'effective_to': pd.to_datetime(['2025-12-31']),
    'as_of_from': pd.to_datetime(['2025-01-01']),
    'as_of_to': pd.to_datetime(['2262-04-11'])
})

# Update: AAPL price changes to $160 on Jan 15
updates = pd.DataFrame({
    'symbol': ['AAPL'],
    'price': [160.0], 
    'effective_from': pd.to_datetime(['2025-01-15']),
    'effective_to': pd.to_datetime(['2025-12-31']),
    'as_of_from': pd.to_datetime(['2025-01-27']),
    'as_of_to': pd.to_datetime(['2262-04-11'])
})

# Process the update
result = processor.process_updates(
    current_state=current_state,
    updates=updates,
    system_date='2025-01-27'
)

print(f"Rows to expire: {len(result.to_expire)}")
print(f"New records: {len(result.to_insert)}")
```

## Complex Multi-Field Updates

```python
# Multi-field portfolio data
processor = BitemporalTimeseriesProcessor(
    id_columns=['portfolio_id', 'symbol'],
    value_columns=['quantity', 'market_value', 'weight']
)

current_state = pd.DataFrame({
    'portfolio_id': ['PORT001', 'PORT001', 'PORT002'],
    'symbol': ['AAPL', 'GOOGL', 'AAPL'],
    'quantity': [100, 50, 200],
    'market_value': [15000, 17500, 30000],
    'weight': [0.24, 0.28, 0.48],
    'effective_from': pd.to_datetime(['2025-01-01'] * 3),
    'effective_to': pd.to_datetime(['2025-12-31'] * 3),
    'as_of_from': pd.to_datetime(['2025-01-01'] * 3),
    'as_of_to': pd.to_datetime(['2262-04-11'] * 3)
})

# Update portfolio weights after rebalancing
updates = pd.DataFrame({
    'portfolio_id': ['PORT001', 'PORT001'],
    'symbol': ['AAPL', 'GOOGL'],
    'quantity': [100, 50],  # Same quantities
    'market_value': [16000, 17500],  # AAPL price increased
    'weight': [0.32, 0.28],  # Rebalanced weights
    'effective_from': pd.to_datetime(['2025-01-15', '2025-01-15']),
    'effective_to': pd.to_datetime(['2025-12-31', '2025-12-31']),
    'as_of_from': pd.to_datetime(['2025-01-27', '2025-01-27']),
    'as_of_to': pd.to_datetime(['2262-04-11', '2262-04-11'])
})

result = processor.process_updates(current_state, updates, '2025-01-27')
```

## Full State Mode

Use when updates represent complete desired state:

```python
processor = BitemporalTimeseriesProcessor(
    id_columns=['account_id'],
    value_columns=['balance', 'status'],
    update_mode='full_state'  # Complete state replacement
)

# Current state: 3 accounts
current_state = pd.DataFrame({
    'account_id': ['ACC001', 'ACC002', 'ACC003'],
    'balance': [1000.0, 2000.0, 1500.0],
    'status': ['active', 'active', 'active'],
    'effective_from': pd.to_datetime(['2025-01-01'] * 3),
    'effective_to': pd.to_datetime(['2025-12-31'] * 3),
    'as_of_from': pd.to_datetime(['2025-01-01'] * 3),
    'as_of_to': pd.to_datetime(['2262-04-11'] * 3)
})

# Update: Only 2 accounts in new state (ACC003 deleted)
updates = pd.DataFrame({
    'account_id': ['ACC001', 'ACC002'],
    'balance': [1100.0, 2000.0],  # ACC001 balance changed
    'status': ['active', 'active'],
    'effective_from': pd.to_datetime(['2025-01-27'] * 2),
    'effective_to': pd.to_datetime(['2025-12-31'] * 2),
    'as_of_from': pd.to_datetime(['2025-01-27'] * 2),
    'as_of_to': pd.to_datetime(['2262-04-11'] * 2)
})

result = processor.process_updates(current_state, updates, '2025-01-27')
# Automatically creates tombstone record for deleted ACC003
```

## Input Conflation

Input conflation merges consecutive records with identical values to optimize processing and reduce output size.

### Basic Conflation Example

```python
import pandas as pd
from pytemporal import BitemporalTimeseriesProcessor

# External feed provides non-conflated data with consecutive segments
updates = pd.DataFrame({
    'id': [1234, 1234, 5678, 5678],
    'field': ['price', 'price', 'volume', 'volume'],
    'value': [100.0, 100.0, 5000, 5000],  # Same values!
    'effective_from': pd.to_datetime([
        '2025-01-01', '2025-06-01',  # Consecutive for id=1234
        '2025-01-01', '2025-06-01'   # Consecutive for id=5678
    ]),
    'effective_to': pd.to_datetime([
        '2025-06-01', '2025-12-31',
        '2025-06-01', '2025-12-31'
    ]),
    'as_of_from': pd.to_datetime(['2025-01-27'] * 4),
    'as_of_to': pd.to_datetime(['2262-04-11'] * 4)
})

# Enable conflation to merge consecutive same-value segments
processor = BitemporalTimeseriesProcessor(
    id_columns=['id', 'field'],
    value_columns=['value'],
    conflate_inputs=True  # Enable conflation
)

_, insert = processor.compute_changes(
    current_state=pd.DataFrame(columns=updates.columns),
    updates=updates,
    update_mode='full_state'
)

# Result: 2 records instead of 4 (50% reduction)
# [1234, 'price', 100.0, '2025-01-01', '2025-12-31', ...]
# [5678, 'volume', 5000, '2025-01-01', '2025-12-31', ...]
print(f"Input: {len(updates)} rows → Output: {len(insert)} rows")
# Output: Input: 4 rows → Output: 2 rows
```

### Selective Conflation

```python
# Create processor with conflation disabled by default
processor = BitemporalTimeseriesProcessor(
    id_columns=['id'],
    value_columns=['value']
    # conflate_inputs defaults to False
)

# Enable for specific external feeds that need it
external_feed_result = processor.compute_changes(
    current_state,
    external_updates,
    conflate_inputs=True  # Enable for this call
)

# Disable for clean internal data
internal_result = processor.compute_changes(
    current_state,
    internal_updates,
    conflate_inputs=False  # Or omit - uses class default
)
```

### Conflation with Value Changes

```python
# Only consecutive records with SAME values are merged
updates = pd.DataFrame({
    'id': [1234, 1234, 1234, 1234],
    'field': ['price', 'price', 'price', 'price'],
    'value': [100.0, 100.0, 150.0, 150.0],  # Value changes
    'effective_from': pd.to_datetime([
        '2025-01-01', '2025-03-01',  # Same value (100)
        '2025-06-01', '2025-09-01'   # Same value (150)
    ]),
    'effective_to': pd.to_datetime([
        '2025-03-01', '2025-06-01',
        '2025-09-01', '2025-12-31'
    ]),
    'as_of_from': pd.to_datetime(['2025-01-27'] * 4),
    'as_of_to': pd.to_datetime(['2262-04-11'] * 4)
})

processor = BitemporalTimeseriesProcessor(
    id_columns=['id', 'field'],
    value_columns=['value'],
    conflate_inputs=True
)

_, insert = processor.compute_changes(
    pd.DataFrame(columns=updates.columns),
    updates,
    update_mode='full_state'
)

# Result: 2 records (one for value=100, one for value=150)
# [1234, 'price', 100.0, '2025-01-01', '2025-06-01', ...]
# [1234, 'price', 150.0, '2025-06-01', '2025-12-31', ...]
assert len(insert) == 2
```

### Performance Considerations

```python
# Good use case: External feed with many redundant segments
large_external_feed = load_external_data()  # 50,000 rows

processor = BitemporalTimeseriesProcessor(
    id_columns=['id', 'field'],
    value_columns=['price', 'volume'],
    conflate_inputs=True  # Reduces 50k → 25k rows (50% reduction)
)

result = processor.compute_changes(current_state, large_external_feed)
# Benefits: Faster processing, fewer output rows, reduced storage

# Bad use case: Small, already-clean data
small_clean_data = pd.DataFrame(...)  # 100 rows, already conflated

processor = BitemporalTimeseriesProcessor(
    id_columns=['id'],
    value_columns=['value'],
    conflate_inputs=False  # Skip overhead for clean data
)

result = processor.compute_changes(current_state, small_clean_data)
# Benefits: Avoid 6-8% overhead when not needed
```

## Working with Different Date Types

```python
# Mix of date/timestamp formats
current_state = pd.DataFrame({
    'id': [1, 2],
    'value': ['A', 'B'],
    # Date32 format
    'effective_from': pd.to_datetime(['2025-01-01', '2025-01-01']).date,
    'effective_to': pd.to_datetime(['2025-12-31', '2025-12-31']).date,
    # Timestamp with timezone
    'as_of_from': pd.to_datetime(['2025-01-01 10:30:00+00:00', '2025-01-01 10:30:00+00:00']),
    'as_of_to': pd.to_datetime(['2262-04-11 23:59:59+00:00', '2262-04-11 23:59:59+00:00'])
})

# PyTemporal handles the type conversions automatically
```

## Large Dataset Processing

```python
# For large datasets, consider memory management
import gc

processor = BitemporalTimeseriesProcessor(
    id_columns=['entity_id'],
    value_columns=['metric1', 'metric2', 'metric3']
)

# Process in chunks for very large datasets
chunk_size = 10000
results = []

for chunk in pd.read_csv('large_updates.csv', chunksize=chunk_size):
    chunk_result = processor.process_updates(current_state, chunk, system_date)
    results.append(chunk_result.to_dataframe())
    gc.collect()  # Force garbage collection

# Combine results
final_result = pd.concat(results, ignore_index=True)
```

## Error Handling and Validation

```python
def safe_process_updates(processor, current_state, updates, system_date):
    try:
        # Validate input data
        required_cols = ['effective_from', 'effective_to', 'as_of_from', 'as_of_to']
        for col in required_cols:
            if col not in current_state.columns:
                raise ValueError(f"Missing required column: {col}")
            if col not in updates.columns:
                raise ValueError(f"Missing required column in updates: {col}")
        
        # Process updates
        result = processor.process_updates(current_state, updates, system_date)
        
        # Validate results
        if result.to_insert.empty and result.to_expire:
            print("Warning: Records expired but no new records inserted")
        
        return result
        
    except Exception as e:
        print(f"Processing failed: {e}")
        return None

# Usage
result = safe_process_updates(processor, current_state, updates, '2025-01-27')
if result:
    print(f"Successfully processed {len(result.to_insert)} new records")
```

## Performance Tips

```python
# 1. Use appropriate hash algorithm
processor = BitemporalTimeseriesProcessor(
    id_columns=['id'],
    value_columns=['value'],
    hash_algorithm='xxhash'  # Faster for most use cases
)

# 2. Pre-sort data by ID columns for better cache performance
current_state = current_state.sort_values(by=['id', 'effective_from'])
updates = updates.sort_values(by=['id', 'effective_from'])

# 3. Use appropriate data types
# Convert string dates to datetime objects once
current_state['effective_from'] = pd.to_datetime(current_state['effective_from'])

# 4. For repeated processing, reuse the processor instance
processor = BitemporalTimeseriesProcessor(id_columns=['id'], value_columns=['value'])
for batch in data_batches:
    result = processor.process_updates(current_state, batch, system_date)
    current_state = update_current_state(current_state, result)
```