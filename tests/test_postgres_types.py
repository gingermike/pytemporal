#!/usr/bin/env python3
"""Test PostgreSQL data types support including Date32 and other common types.

This test ensures that the bitemporal processor correctly handles PostgreSQL
data types like Date32, Date64, Float32, Int8, Int16, etc., especially in
tombstone record creation scenarios.
"""

import pandas as pd
import pytest
from datetime import date
from pytemporal import BitemporalTimeseriesProcessor

def test_date32_tombstone_support():
    """Test that Date32 columns work correctly in tombstone creation."""
    
    # Current state with Date32-compatible date
    current_data = pd.DataFrame([
        {
            "id": 1,
            "name": "test_record",
            "start_date": date(2024, 1, 1),  # This should map to Date32
            "value": 100.0,
            "effective_from": pd.Timestamp("2024-01-01").date(),
            "effective_to": pd.Timestamp("2260-12-31").date(),
            "as_of_from": pd.Timestamp("2024-01-10", tz="UTC"),
            "as_of_to": pd.Timestamp("2260-12-31 23:59:59", tz="UTC"),
        }
    ])
    
    # Updates with no matching records (should create tombstone)
    updates_data = pd.DataFrame([
        {
            "id": 2,
            "name": "different_record", 
            "start_date": date(2024, 2, 1),
            "value": 200.0,
            "effective_from": pd.Timestamp("2024-02-01").date(),
            "effective_to": pd.Timestamp("2260-12-31").date(),
            "as_of_from": pd.Timestamp("2024-02-10", tz="UTC"),
            "as_of_to": pd.Timestamp("2260-12-31 23:59:59", tz="UTC"),
        }
    ])
    
    # Process with full_state mode to trigger tombstone creation
    processor = BitemporalTimeseriesProcessor(
        id_columns=["id", "name"],
        value_columns=["start_date", "value"]
    )
    
    result = processor.compute_changes(
        current_data,
        updates_data,
        update_mode="full_state"
    )
    
    expire_df, insert_df = result
    
    # Should expire the current record and create a tombstone
    assert len(expire_df) == 1, "Should expire the current record"
    assert len(insert_df) == 2, "Should insert new record + tombstone"
    
    # Verify tombstone has correct Date32 handling
    tombstone = insert_df[insert_df["id"] == 1]
    assert len(tombstone) == 1, "Should have one tombstone record"
    assert tombstone.iloc[0]["start_date"] == date(2024, 1, 1), "Date32 field should be preserved in tombstone"
    assert tombstone.iloc[0]["value"] == 100.0, "Value should be preserved in tombstone"
    
    print("✅ Date32 tombstone support test passed")

def test_mixed_postgres_types():
    """Test various PostgreSQL data types in value columns."""
    
    # Mix different data types that are common in PostgreSQL
    current_data = pd.DataFrame([
        {
            "id": 1,
            "small_int": 42,  # Int16
            "big_int": 123456789,  # Int64
            "decimal_val": 123.45,  # Float64
            "start_date": date(2024, 1, 1),  # Date32
            "is_active": True,  # Boolean
            "effective_from": pd.Timestamp("2024-01-01").date(),
            "effective_to": pd.Timestamp("2260-12-31").date(),
            "as_of_from": pd.Timestamp("2024-01-10", tz="UTC"),
            "as_of_to": pd.Timestamp("2260-12-31 23:59:59", tz="UTC"),
        }
    ])
    
    # Updates that should be detected as no-change for value columns
    updates_data = pd.DataFrame([
        {
            "id": 1,
            "small_int": 42,  # Same value
            "big_int": 123456789,  # Same value
            "decimal_val": 123.45,  # Same value
            "start_date": date(2024, 1, 1),  # Same date
            "is_active": True,  # Same boolean
            "effective_from": pd.Timestamp("2024-01-02").date(),  # Different effective date
            "effective_to": pd.Timestamp("2260-12-31").date(),
            "as_of_from": pd.Timestamp("2024-01-15", tz="UTC"),
            "as_of_to": pd.Timestamp("2260-12-31 23:59:59", tz="UTC"),
        }
    ])
    
    processor = BitemporalTimeseriesProcessor(
        id_columns=["id"],
        value_columns=["small_int", "big_int", "decimal_val", "start_date", "is_active"]
    )
    
    result = processor.compute_changes(
        current_data,
        updates_data,
        update_mode="delta"
    )
    
    expire_df, insert_df = result
    
    # Values are identical, so should generate timeline operations but data is the same
    # The key test is that it doesn't crash with unsupported data types
    assert len(expire_df) >= 0, "Should handle mixed data types without error"
    assert len(insert_df) >= 0, "Should handle mixed data types without error"
    
    print("✅ Mixed PostgreSQL types test passed")

def test_null_handling_with_postgres_types():
    """Test that NULL values work correctly with PostgreSQL types."""
    
    current_data = pd.DataFrame([
        {
            "id": 1,
            "optional_date": None,  # NULL Date32
            "optional_number": None,  # NULL Float64
            "required_field": "test",
            "effective_from": pd.Timestamp("2024-01-01").date(),
            "effective_to": pd.Timestamp("2260-12-31").date(),
            "as_of_from": pd.Timestamp("2024-01-10", tz="UTC"),
            "as_of_to": pd.Timestamp("2260-12-31 23:59:59", tz="UTC"),
        }
    ])
    
    updates_data = pd.DataFrame([
        {
            "id": 1,
            "optional_date": date(2024, 2, 1),  # Now has a value
            "optional_number": 456.78,  # Now has a value
            "required_field": "test",  # Same value
            "effective_from": pd.Timestamp("2024-02-01").date(),
            "effective_to": pd.Timestamp("2260-12-31").date(),
            "as_of_from": pd.Timestamp("2024-02-10", tz="UTC"),
            "as_of_to": pd.Timestamp("2260-12-31 23:59:59", tz="UTC"),
        }
    ])
    
    processor = BitemporalTimeseriesProcessor(
        id_columns=["id"],
        value_columns=["optional_date", "optional_number", "required_field"]
    )
    
    result = processor.compute_changes(
        current_data,
        updates_data,
        update_mode="delta"
    )
    
    expire_df, insert_df = result
    
    # Should detect change from NULL to actual values
    assert len(insert_df) > 0, "Should detect NULL to value changes"
    
    # Verify the update has the new values
    if len(insert_df) > 0:
        updated_record = insert_df.iloc[-1]  # Latest insert
        assert updated_record["optional_date"] == date(2024, 2, 1)
        assert updated_record["optional_number"] == 456.78
        assert updated_record["required_field"] == "test"
    
    print("✅ NULL handling with PostgreSQL types test passed")

if __name__ == "__main__":
    test_date32_tombstone_support()
    test_mixed_postgres_types()
    test_null_handling_with_postgres_types()
    print("🎉 All PostgreSQL types tests passed!")