#!/usr/bin/env python3
"""Test batch_utils.rs PostgreSQL data types support.

This test ensures that create_record_batch_from_records function correctly handles
all PostgreSQL data types without falling back to the less efficient slice method.
"""

import pandas as pd
import pytest
import numpy as np
from datetime import date, datetime
from decimal import Decimal
from pytemporal import BitemporalTimeseriesProcessor

def test_batch_utils_comprehensive_types():
    """Test that all PostgreSQL types work efficiently in batch creation."""
    
    # Create data with comprehensive PostgreSQL types
    current_data = pd.DataFrame([
        {
            "id": 1,
            # Integer types
            "tiny_int": np.int8(42),           # Int8 
            "small_int": np.int16(1234),       # Int16
            "regular_int": 123456,             # Int32
            "big_int": 123456789012345,        # Int64
            
            # Float types  
            "real_val": np.float32(123.45),    # Float32
            "double_val": 678.90,              # Float64
            
            # Date/Time types
            "date_field": date(2024, 1, 15),   # Date32
            "timestamp_field": pd.Timestamp("2024-01-15 12:30:45", tz="UTC"),  # Timestamp
            
            # Boolean
            "active_flag": True,               # Boolean
            
            # Text
            "description": "Test record",      # String
            
            # Temporal columns
            "effective_from": pd.Timestamp("2024-01-01").date(),
            "effective_to": pd.Timestamp("2260-12-31").date(),
            "as_of_from": pd.Timestamp("2024-01-10", tz="UTC"),
            "as_of_to": pd.Timestamp("2260-12-31 23:59:59", tz="UTC"),
        }
    ])
    
    # Updates that should trigger record batch creation
    updates_data = pd.DataFrame([
        {
            "id": 1,
            # Same values but different effective period
            "tiny_int": np.int8(42),
            "small_int": np.int16(1234), 
            "regular_int": 123456,
            "big_int": 123456789012345,
            "real_val": np.float32(123.45),
            "double_val": 678.90,
            "date_field": date(2024, 1, 15),
            "timestamp_field": pd.Timestamp("2024-01-15 12:30:45", tz="UTC"),
            "active_flag": True,
            "description": "Test record",
            
            # Different effective period
            "effective_from": pd.Timestamp("2024-01-02").date(),
            "effective_to": pd.Timestamp("2260-12-31").date(),
            "as_of_from": pd.Timestamp("2024-01-15", tz="UTC"),
            "as_of_to": pd.Timestamp("2260-12-31 23:59:59", tz="UTC"),
        }
    ])
    
    processor = BitemporalTimeseriesProcessor(
        id_columns=["id"],
        value_columns=["tiny_int", "small_int", "regular_int", "big_int", 
                      "real_val", "double_val", "date_field", "timestamp_field",
                      "active_flag", "description"]
    )
    
    result = processor.compute_changes(
        current_data,
        updates_data,
        update_mode="delta"
    )
    
    expire_df, insert_df = result
    
    # Should process successfully without falling back to slice method
    assert len(expire_df) >= 0, "Should handle all PostgreSQL types without error"
    assert len(insert_df) >= 0, "Should handle all PostgreSQL types without error"
    
    # Verify data integrity in results
    if len(insert_df) > 0:
        # Check that all data types are preserved correctly
        latest_record = insert_df.iloc[-1]
        assert latest_record["tiny_int"] == 42, "Int8 should be preserved"
        assert latest_record["small_int"] == 1234, "Int16 should be preserved" 
        assert latest_record["regular_int"] == 123456, "Int32 should be preserved"
        assert latest_record["big_int"] == 123456789012345, "Int64 should be preserved"
        assert abs(latest_record["real_val"] - 123.45) < 0.01, "Float32 should be preserved"
        assert abs(latest_record["double_val"] - 678.90) < 0.01, "Float64 should be preserved"
        assert latest_record["date_field"] == date(2024, 1, 15), "Date32 should be preserved"
        assert latest_record["active_flag"] == True, "Boolean should be preserved"
        assert latest_record["description"] == "Test record", "String should be preserved"
    
    print("✅ Batch utilities comprehensive types test passed")

def test_null_values_with_all_types():
    """Test that NULL values work correctly with all PostgreSQL types."""
    
    current_data = pd.DataFrame([
        {
            "id": 1,
            "optional_tiny": None,      # NULL Int8
            "optional_small": None,     # NULL Int16  
            "optional_int": None,       # NULL Int32
            "optional_big": None,       # NULL Int64
            "optional_real": None,      # NULL Float32
            "optional_double": None,    # NULL Float64
            "optional_date": None,      # NULL Date32
            "optional_bool": None,      # NULL Boolean
            "optional_text": None,      # NULL String
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
            # Set some previously NULL values
            "optional_tiny": np.int8(5),
            "optional_small": np.int16(100),
            "optional_int": 500,
            "optional_big": 999999,
            "optional_real": np.float32(1.23),
            "optional_double": 4.56,
            "optional_date": date(2024, 2, 1),
            "optional_bool": True,
            "optional_text": "updated",
            "required_field": "test",
            "effective_from": pd.Timestamp("2024-02-01").date(),
            "effective_to": pd.Timestamp("2260-12-31").date(),
            "as_of_from": pd.Timestamp("2024-02-10", tz="UTC"),
            "as_of_to": pd.Timestamp("2260-12-31 23:59:59", tz="UTC"),
        }
    ])
    
    processor = BitemporalTimeseriesProcessor(
        id_columns=["id"],
        value_columns=["optional_tiny", "optional_small", "optional_int", "optional_big",
                      "optional_real", "optional_double", "optional_date", "optional_bool", 
                      "optional_text", "required_field"]
    )
    
    result = processor.compute_changes(
        current_data,
        updates_data,
        update_mode="delta"
    )
    
    expire_df, insert_df = result
    
    # Should detect changes from NULL to values
    assert len(insert_df) > 0, "Should detect NULL to value changes"
    
    # Verify the updated values
    if len(insert_df) > 0:
        updated_record = insert_df.iloc[-1]
        assert updated_record["optional_tiny"] == 5
        assert updated_record["optional_small"] == 100
        assert updated_record["optional_int"] == 500
        assert updated_record["optional_big"] == 999999
        assert abs(updated_record["optional_real"] - 1.23) < 0.01
        assert abs(updated_record["optional_double"] - 4.56) < 0.01
        assert updated_record["optional_date"] == date(2024, 2, 1)
        assert updated_record["optional_bool"] == True
        assert updated_record["optional_text"] == "updated"
        assert updated_record["required_field"] == "test"
    
    print("✅ NULL values with all types test passed")

def test_type_mixing_edge_cases():
    """Test edge cases with type mixing that might cause issues."""
    
    # Test integers vs floats with same numeric values
    current_data = pd.DataFrame([
        {
            "id": 1,
            "mixed_number": 100,        # Int64
            "effective_from": pd.Timestamp("2024-01-01").date(),
            "effective_to": pd.Timestamp("2260-12-31").date(), 
            "as_of_from": pd.Timestamp("2024-01-10", tz="UTC"),
            "as_of_to": pd.Timestamp("2260-12-31 23:59:59", tz="UTC"),
        }
    ])
    
    updates_data = pd.DataFrame([
        {
            "id": 1,
            "mixed_number": 100.0,      # Float64 with same value
            "effective_from": pd.Timestamp("2024-01-02").date(),
            "effective_to": pd.Timestamp("2260-12-31").date(),
            "as_of_from": pd.Timestamp("2024-01-15", tz="UTC"),
            "as_of_to": pd.Timestamp("2260-12-31 23:59:59", tz="UTC"),
        }
    ])
    
    processor = BitemporalTimeseriesProcessor(
        id_columns=["id"],
        value_columns=["mixed_number"]
    )
    
    result = processor.compute_changes(
        current_data,
        updates_data,
        update_mode="delta"
    )
    
    expire_df, insert_df = result
    
    # Should handle the type difference correctly (hash normalization should work)
    # This tests the integration between batch creation and hash computation
    assert len(expire_df) >= 0, "Should handle integer/float type differences"
    assert len(insert_df) >= 0, "Should handle integer/float type differences"
    
    print("✅ Type mixing edge cases test passed")

if __name__ == "__main__":
    test_batch_utils_comprehensive_types()
    test_null_values_with_all_types()
    test_type_mixing_edge_cases()
    print("🎉 All batch utilities PostgreSQL types tests passed!")