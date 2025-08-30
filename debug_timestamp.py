#!/usr/bin/env python3

import pandas as pd
from tests.scenarios.basic import full_state_delete
from tests.scenarios.defaults import default_id_columns, default_value_columns, default_columns
from pytemporal import BitemporalTimeseriesProcessor

# Get the test scenario data
current_state, updates, expected = full_state_delete.data()
update_mode = full_state_delete.update_mode

# Create processor
processor = BitemporalTimeseriesProcessor(
    id_columns=default_id_columns,
    value_columns=default_value_columns
)

current_state_df = pd.DataFrame(current_state, columns=default_columns)
updates_df = pd.DataFrame(updates, columns=default_columns)

print("=== INPUT DATA ===")
print("Current State:")
print(current_state_df[['id', 'field', 'effective_from', 'effective_to', 'as_of_from']])
print()
print("Updates:")
print(updates_df[['id', 'field', 'effective_from', 'effective_to', 'as_of_from']])
print()

# Process
expire, insert = processor.compute_changes(
    current_state_df, 
    updates_df,
    update_mode=update_mode
)

print("=== ACTUAL RESULTS ===")
print("EXPIRE:")
for idx, row in expire.iterrows():
    print(f"ID={row['id']}, field={row['field']}, effective_from={row['effective_from']}, effective_to={row['effective_to']}, as_of_from={row['as_of_from']}")
print()

print("INSERT:")
for idx, row in insert.iterrows():
    print(f"ID={row['id']}, field={row['field']}, effective_from={row['effective_from']}, effective_to={row['effective_to']}, as_of_from={row['as_of_from']}")
print()

# Expected results
expected_expire, expected_insert = expected
expected_expire_df = pd.DataFrame(expected_expire, columns=default_columns)
expected_insert_df = pd.DataFrame(expected_insert, columns=default_columns)

print("=== EXPECTED RESULTS ===")
print("EXPECTED EXPIRE:")
for idx, row in expected_expire_df.iterrows():
    print(f"ID={row['id']}, field={row['field']}, effective_from={row['effective_from']}, effective_to={row['effective_to']}, as_of_from={row['as_of_from']}")
print()

print("EXPECTED INSERT:")
for idx, row in expected_insert_df.iterrows():
    print(f"ID={row['id']}, field={row['field']}, effective_from={row['effective_from']}, effective_to={row['effective_to']}, as_of_from={row['as_of_from']}")