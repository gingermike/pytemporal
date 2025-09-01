#!/usr/bin/env python3
"""Test hash normalization bug fix for mixed type scenarios.

This test ensures that the hash normalization fix correctly handles scenarios where
identical business values have different data types (e.g., Int64 vs Float64) between
current state and updates batches. This prevents incorrect "extra row" generation
in delta mode processing.
"""

import pandas as pd
import pytest
from pytemporal import BitemporalTimeseriesProcessor

def test_hash_normalization_mixed_types():
    """Test that Int64 vs Float64 with same numeric values are correctly detected as no-change."""
    
    # Current state - will have Int64 quantity due to no nulls
    current_data = pd.DataFrame([
        {
            "portfolio_id": 1,
            "security_id": "AAPL",
            "quantity": 100,  # Will be Int64
            "price": 150.25,
            "is_active": True,
            "trade_time": pd.Timestamp("2024-01-01 09:30:00"),
            "settlement_time": pd.Timestamp("2024-01-03 09:30:00", tz="UTC"),
            "effective_from": pd.Timestamp("2024-01-01").date(),
            "effective_to": pd.Timestamp("2260-12-31").date(),
            "as_of_from": pd.Timestamp("2024-01-10", tz="UTC"),
            "as_of_to": pd.Timestamp("2260-12-31 23:59:59", tz="UTC"),
        },
        {
            "portfolio_id": 2,
            "security_id": "GOOGL",
            "quantity": 200,  # Will be Int64
            "price": 2800.50,
            "is_active": False,
            "trade_time": pd.Timestamp("2024-01-01 10:00:00"),
            "settlement_time": pd.Timestamp("2024-01-03 10:00:00", tz="UTC"),
            "effective_from": pd.Timestamp("2024-01-01").date(),
            "effective_to": pd.Timestamp("2260-12-31").date(),
            "as_of_from": pd.Timestamp("2024-01-10", tz="UTC"),
            "as_of_to": pd.Timestamp("2260-12-31 23:59:59", tz="UTC"),
        },
    ])
    
    # Updates - will have Float64 quantity due to MSFT having None
    updates_data = pd.DataFrame([
        {
            "portfolio_id": 1,
            "security_id": "AAPL",
            "quantity": 100.0,  # Same value as current, but will be Float64 due to MSFT null
            "price": 150.25,    # Same value
            "is_active": True,   # Same value
            "trade_time": pd.Timestamp("2024-01-01 09:30:00"),  # Same value
            "settlement_time": pd.Timestamp("2024-01-03 09:30:00", tz="UTC"),  # Same value
            "effective_from": pd.Timestamp("2024-01-02").date(),  # Different effective date
            "effective_to": pd.Timestamp("2260-12-31").date(),
            "as_of_from": pd.Timestamp("2024-01-15", tz="UTC"),
            "as_of_to": pd.Timestamp("2260-12-31 23:59:59", tz="UTC"),
        },
        {
            "portfolio_id": 3,
            "security_id": "MSFT",
            "quantity": None,    # This null causes pandas to infer Float64 for the column
            "price": 350.75,
            "is_active": None,
            "trade_time": pd.Timestamp("2024-01-01 11:00:00"),
            "settlement_time": pd.Timestamp("2024-01-03 11:00:00", tz="UTC"),
            "effective_from": pd.Timestamp("2024-01-02").date(),
            "effective_to": pd.Timestamp("2260-12-31").date(),
            "as_of_from": pd.Timestamp("2024-01-15", tz="UTC"),
            "as_of_to": pd.Timestamp("2260-12-31 23:59:59", tz="UTC"),
        },
    ])
    
    # Verify the type difference that triggers the bug
    assert str(current_data['quantity'].dtype) == 'int64', "Current data should have Int64 quantity"
    assert str(updates_data['quantity'].dtype) == 'float64', "Updates data should have Float64 quantity due to nulls"
    
    # Process with delta mode
    processor = BitemporalTimeseriesProcessor(
        id_columns=["portfolio_id", "security_id"],
        value_columns=["quantity", "price", "is_active", "trade_time", "settlement_time"]
    )
    
    result = processor.compute_changes(
        current_data,
        updates_data,
        update_mode="delta"
    )
    
    expire_df, insert_df = result
    
    # AAPL should NOT appear in expires or inserts due to hash normalization
    aapl_expires = expire_df[expire_df["security_id"] == "AAPL"] if len(expire_df) > 0 else pd.DataFrame()
    aapl_inserts = insert_df[insert_df["security_id"] == "AAPL"] if len(insert_df) > 0 else pd.DataFrame()
    
    assert len(aapl_expires) == 0, "AAPL should not be expired when values are numerically identical"
    assert len(aapl_inserts) == 0, "AAPL should not be inserted when values are numerically identical"
    
    # MSFT should be inserted as it's a new record
    msft_inserts = insert_df[insert_df["security_id"] == "MSFT"] if len(insert_df) > 0 else pd.DataFrame()
    assert len(msft_inserts) == 1, "MSFT should be inserted as it's a new record"
    
    print("✅ Hash normalization test passed: No extra rows generated for AAPL")

def test_hash_normalization_edge_cases():
    """Test edge cases for hash normalization (whole numbers, zero, negative)."""
    
    current_data = pd.DataFrame([
        {
            "id": 1, "name": "test1", "value": 0,     # Int64 zero
            "effective_from": pd.Timestamp("2024-01-01").date(),
            "effective_to": pd.Timestamp("2260-12-31").date(),
            "as_of_from": pd.Timestamp("2024-01-10", tz="UTC"),
            "as_of_to": pd.Timestamp("2260-12-31 23:59:59", tz="UTC"),
        },
        {
            "id": 2, "name": "test2", "value": -100,  # Int64 negative
            "effective_from": pd.Timestamp("2024-01-01").date(),
            "effective_to": pd.Timestamp("2260-12-31").date(),
            "as_of_from": pd.Timestamp("2024-01-10", tz="UTC"),
            "as_of_to": pd.Timestamp("2260-12-31 23:59:59", tz="UTC"),
        },
    ])
    
    updates_data = pd.DataFrame([
        {
            "id": 1, "name": "test1", "value": 0.0,    # Float64 zero (same as Int64 zero)
            "effective_from": pd.Timestamp("2024-01-02").date(),
            "effective_to": pd.Timestamp("2260-12-31").date(),
            "as_of_from": pd.Timestamp("2024-01-15", tz="UTC"),
            "as_of_to": pd.Timestamp("2260-12-31 23:59:59", tz="UTC"),
        },
        {
            "id": 2, "name": "test2", "value": -100.0,  # Float64 negative (same as Int64 -100)
            "effective_from": pd.Timestamp("2024-01-02").date(),
            "effective_to": pd.Timestamp("2260-12-31").date(),
            "as_of_from": pd.Timestamp("2024-01-15", tz="UTC"),
            "as_of_to": pd.Timestamp("2260-12-31 23:59:59", tz="UTC"),
        },
        {
            "id": 3, "name": "test3", "value": None,    # Causes Float64 inference
            "effective_from": pd.Timestamp("2024-01-02").date(),
            "effective_to": pd.Timestamp("2260-12-31").date(),
            "as_of_from": pd.Timestamp("2024-01-15", tz="UTC"),
            "as_of_to": pd.Timestamp("2260-12-31 23:59:59", tz="UTC"),
        },
    ])
    
    processor = BitemporalTimeseriesProcessor(
        id_columns=["id", "name"],
        value_columns=["value"]
    )
    
    result = processor.compute_changes(
        current_data,
        updates_data,
        update_mode="delta"
    )
    
    expire_df, insert_df = result
    
    # test1 and test2 should not be processed (values unchanged)
    test1_ops = len(expire_df[expire_df["name"] == "test1"]) + len(insert_df[insert_df["name"] == "test1"])
    test2_ops = len(expire_df[expire_df["name"] == "test2"]) + len(insert_df[insert_df["name"] == "test2"])
    
    assert test1_ops == 0, "test1 (0 vs 0.0) should not generate operations"
    assert test2_ops == 0, "test2 (-100 vs -100.0) should not generate operations"
    
    # test3 should be inserted as new record
    test3_inserts = len(insert_df[insert_df["name"] == "test3"])
    assert test3_inserts == 1, "test3 should be inserted as new record"
    
    print("✅ Edge cases test passed: Zero and negative values handled correctly")

if __name__ == "__main__":
    test_hash_normalization_mixed_types()
    test_hash_normalization_edge_cases()
    print("🎉 All hash normalization tests passed!")