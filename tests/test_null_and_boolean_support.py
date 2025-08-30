"""
Tests for NULL value handling and Boolean data type support.

These tests ensure that the bitemporal processor can handle:
1. NULL values in value columns without panicking
2. Boolean data types in both delta and full_state modes
3. Mixed data types including NULL and Boolean values
4. Tombstone record creation with Boolean columns
"""

import pandas as pd
import pytest
from pytemporal import BitemporalTimeseriesProcessor


class TestNullValueHandling:
    """Test NULL value support in various scenarios."""
    
    def test_null_values_in_current_state(self):
        """Test handling NULL values in current state records."""
        processor = BitemporalTimeseriesProcessor(
            id_columns=['portfolio_id', 'security_id'],
            value_columns=['quantity', 'price', 'is_active']
        )
        
        # Current state with NULL values
        current_state = pd.DataFrame([
            {
                "portfolio_id": 1,
                "security_id": "AAPL",
                "quantity": None,  # NULL in current state
                "price": 150.25,
                "is_active": None,  # NULL in current state
                "effective_from": pd.Timestamp("2024-01-01"),
                "effective_to": pd.Timestamp("2099-12-31"),
                "as_of_from": pd.Timestamp("2024-01-01 09:30:00"),
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59"),
            }
        ])
        
        # Updates with different values
        updates = pd.DataFrame([
            {
                "portfolio_id": 1,
                "security_id": "AAPL",
                "quantity": 100,  # Different from NULL
                "price": 150.25,
                "is_active": True,  # Different from NULL
                "effective_from": pd.Timestamp("2024-01-01"),
                "effective_to": pd.Timestamp("2099-12-31"),
                "as_of_from": pd.Timestamp("2024-01-02 09:30:00"),
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59"),
            }
        ])
        
        # Should not panic and should process correctly
        rows_to_expire, rows_to_insert = processor.compute_changes(
            current_state, updates, update_mode='delta'
        )
        
        assert len(rows_to_expire) == 1
        assert len(rows_to_insert) == 1
        
    def test_null_values_in_updates(self):
        """Test handling NULL values in update records."""
        processor = BitemporalTimeseriesProcessor(
            id_columns=['portfolio_id', 'security_id'],
            value_columns=['quantity', 'price', 'is_active']
        )
        
        # Clean current state
        current_state = pd.DataFrame([
            {
                "portfolio_id": 1,
                "security_id": "AAPL",
                "quantity": 100,
                "price": 150.25,
                "is_active": True,
                "effective_from": pd.Timestamp("2024-01-01"),
                "effective_to": pd.Timestamp("2099-12-31"),
                "as_of_from": pd.Timestamp("2024-01-01 09:30:00"),
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59"),
            }
        ])
        
        # Updates with NULL values
        updates = pd.DataFrame([
            {
                "portfolio_id": 3,  # New record
                "security_id": "MSFT",
                "quantity": None,  # NULL in updates
                "price": 350.75,
                "is_active": None,  # NULL in updates
                "effective_from": pd.Timestamp("2024-01-02"),
                "effective_to": pd.Timestamp("2099-12-31"),
                "as_of_from": pd.Timestamp("2024-01-02 11:00:00"),
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59"),
            }
        ])
        
        # Should not panic and should insert new record with NULLs
        rows_to_expire, rows_to_insert = processor.compute_changes(
            current_state, updates, update_mode='delta'
        )
        
        assert len(rows_to_expire) == 0
        assert len(rows_to_insert) == 1
        assert pd.isna(rows_to_insert.iloc[0]['quantity'])
        assert pd.isna(rows_to_insert.iloc[0]['is_active'])
        
    def test_all_null_column(self):
        """Test handling columns that are entirely NULL."""
        processor = BitemporalTimeseriesProcessor(
            id_columns=['portfolio_id'],
            value_columns=['nullable_field']
        )
        
        # Current state with all NULL values in a column
        current_state = pd.DataFrame([
            {
                "portfolio_id": 1,
                "nullable_field": None,
                "effective_from": pd.Timestamp("2024-01-01"),
                "effective_to": pd.Timestamp("2099-12-31"),
                "as_of_from": pd.Timestamp("2024-01-01 09:30:00"),
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59"),
            }
        ])
        
        # Updates also with NULL
        updates = pd.DataFrame([
            {
                "portfolio_id": 2,
                "nullable_field": None,
                "effective_from": pd.Timestamp("2024-01-02"),
                "effective_to": pd.Timestamp("2099-12-31"),
                "as_of_from": pd.Timestamp("2024-01-02 10:00:00"),
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59"),
            }
        ])
        
        # Should handle entirely NULL columns without panic
        rows_to_expire, rows_to_insert = processor.compute_changes(
            current_state, updates, update_mode='delta'
        )
        
        assert len(rows_to_expire) == 0
        assert len(rows_to_insert) == 1


class TestBooleanDataTypeSupport:
    """Test Boolean data type support in various scenarios."""
    
    def test_boolean_in_delta_mode(self):
        """Test Boolean columns work in delta mode."""
        processor = BitemporalTimeseriesProcessor(
            id_columns=['id'],
            value_columns=['is_active', 'is_verified']
        )
        
        current_state = pd.DataFrame([
            {
                "id": 1,
                "is_active": True,
                "is_verified": False,
                "effective_from": pd.Timestamp("2024-01-01"),
                "effective_to": pd.Timestamp("2099-12-31"),
                "as_of_from": pd.Timestamp("2024-01-01 09:30:00"),
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59"),
            }
        ])
        
        updates = pd.DataFrame([
            {
                "id": 1,
                "is_active": False,  # Boolean change
                "is_verified": True,  # Boolean change
                "effective_from": pd.Timestamp("2024-01-02"),
                "effective_to": pd.Timestamp("2099-12-31"),
                "as_of_from": pd.Timestamp("2024-01-02 09:30:00"),
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59"),
            }
        ])
        
        rows_to_expire, rows_to_insert = processor.compute_changes(
            current_state, updates, update_mode='delta'
        )
        
        assert len(rows_to_expire) == 1
        assert len(rows_to_insert) == 2  # Split timeline
        
    def test_boolean_in_full_state_mode(self):
        """Test Boolean columns work in full_state mode including tombstones."""
        processor = BitemporalTimeseriesProcessor(
            id_columns=['id'],
            value_columns=['is_active', 'enabled']
        )
        
        current_state = pd.DataFrame([
            {
                "id": 1,
                "is_active": True,
                "enabled": False,
                "effective_from": pd.Timestamp("2024-01-01"),
                "effective_to": pd.Timestamp("2099-12-31"),
                "as_of_from": pd.Timestamp("2024-01-01 09:30:00"),
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59"),
            },
            {
                "id": 2,
                "is_active": False,
                "enabled": True,
                "effective_from": pd.Timestamp("2024-01-01"),
                "effective_to": pd.Timestamp("2099-12-31"),
                "as_of_from": pd.Timestamp("2024-01-01 09:30:00"),
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59"),
            }
        ])
        
        # Updates only contain id=1, so id=2 should get tombstone
        updates = pd.DataFrame([
            {
                "id": 1,
                "is_active": True,  # Same value
                "enabled": True,    # Different value
                "effective_from": pd.Timestamp("2024-01-01"),
                "effective_to": pd.Timestamp("2099-12-31"),
                "as_of_from": pd.Timestamp("2024-01-02 09:30:00"),
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59"),
            }
        ])
        
        # This should create tombstone records with Boolean columns
        rows_to_expire, rows_to_insert = processor.compute_changes(
            current_state, updates, update_mode='full_state'
        )
        
        assert len(rows_to_expire) >= 1  # At least id=2 gets expired
        assert len(rows_to_insert) >= 1  # At least tombstone for id=2
        
        # Verify Boolean values are preserved
        for _, row in rows_to_insert.iterrows():
            assert isinstance(row['is_active'], (bool, type(None)))
            assert isinstance(row['enabled'], (bool, type(None)))
    
    def test_mixed_boolean_and_null(self):
        """Test Boolean columns with NULL values."""
        processor = BitemporalTimeseriesProcessor(
            id_columns=['id'],
            value_columns=['is_active', 'is_verified']
        )
        
        current_state = pd.DataFrame([
            {
                "id": 1,
                "is_active": True,
                "is_verified": None,  # NULL Boolean
                "effective_from": pd.Timestamp("2024-01-01"),
                "effective_to": pd.Timestamp("2099-12-31"),
                "as_of_from": pd.Timestamp("2024-01-01 09:30:00"),
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59"),
            }
        ])
        
        updates = pd.DataFrame([
            {
                "id": 2,
                "is_active": None,  # NULL Boolean
                "is_verified": False,
                "effective_from": pd.Timestamp("2024-01-02"),
                "effective_to": pd.Timestamp("2099-12-31"),
                "as_of_from": pd.Timestamp("2024-01-02 09:30:00"),
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59"),
            }
        ])
        
        # Should handle Boolean columns with NULLs in both modes
        for mode in ['delta', 'full_state']:
            rows_to_expire, rows_to_insert = processor.compute_changes(
                current_state, updates, update_mode=mode
            )
            
            # Should not panic and process correctly
            assert isinstance(rows_to_expire, pd.DataFrame)
            assert isinstance(rows_to_insert, pd.DataFrame)


class TestDeveloperScenario:
    """Test the exact scenario that originally caused the panic."""
    
    def test_original_developer_scenario(self):
        """Reproduce and verify the original developer scenario works."""
        processor = BitemporalTimeseriesProcessor(
            id_columns=['portfolio_id', 'security_id'],
            value_columns=['quantity', 'price', 'is_active']
        )
        
        # Current state (clean_data equivalent)
        clean_data = pd.DataFrame([
            {
                "portfolio_id": 1,
                "security_id": "AAPL",
                "quantity": 100,
                "price": 150.25,
                "is_active": True,
                "effective_from": pd.Timestamp("2024-01-01"),
                "effective_to": pd.Timestamp("2099-12-31"),
                "as_of_from": pd.Timestamp("2024-01-01 09:30:00"),
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59"),
            },
            {
                "portfolio_id": 2,
                "security_id": "GOOGL",
                "quantity": 200,
                "price": 2800.50,
                "is_active": False,
                "effective_from": pd.Timestamp("2024-01-01"),
                "effective_to": pd.Timestamp("2099-12-31"),
                "as_of_from": pd.Timestamp("2024-01-01 10:00:00"), 
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59"),
            },
        ])
        
        # Updates (nullable_data equivalent) with NULL values
        nullable_data = pd.DataFrame([
            {
                "portfolio_id": 1,
                "security_id": "AAPL",
                "quantity": 100,
                "price": 150.25,
                "is_active": True,
                "effective_from": pd.Timestamp("2024-01-02"),  # Different effective range  
                "effective_to": pd.Timestamp("2099-12-31"),
                "as_of_from": pd.Timestamp("2024-01-02 09:30:00"),
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59"),
            },
            {
                "portfolio_id": 3,  # Valid identifier (NOT NULL required)
                "security_id": "MSFT",
                "quantity": None,  # NULL in value field (allowed)
                "price": 350.75,
                "is_active": None,  # NULL in value field (allowed)
                "effective_from": pd.Timestamp("2024-01-02"),
                "effective_to": pd.Timestamp("2099-12-31"),
                "as_of_from": pd.Timestamp("2024-01-02 11:00:00"),
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59"),
            },
        ])
        
        # Test both modes to ensure comprehensive support
        for mode in ['delta', 'full_state']:
            rows_to_expire, rows_to_insert = processor.compute_changes(
                clean_data, nullable_data, update_mode=mode
            )
            
            # Should complete without panic
            assert isinstance(rows_to_expire, pd.DataFrame)
            assert isinstance(rows_to_insert, pd.DataFrame)
            assert len(rows_to_insert) > 0  # Should have inserts
            
            # Verify NULL values are preserved
            null_rows = rows_to_insert[rows_to_insert['security_id'] == 'MSFT']
            if len(null_rows) > 0:
                assert pd.isna(null_rows.iloc[0]['quantity'])
                assert pd.isna(null_rows.iloc[0]['is_active'])


class TestEdgeCases:
    """Test various edge cases and combinations."""
    
    def test_multiple_data_types_with_nulls(self):
        """Test multiple data types including NULLs in a single dataset."""
        processor = BitemporalTimeseriesProcessor(
            id_columns=['id'],
            value_columns=['string_field', 'int_field', 'float_field', 'bool_field']
        )
        
        current_state = pd.DataFrame([
            {
                "id": 1,
                "string_field": "test",
                "int_field": 42,
                "float_field": 3.14,
                "bool_field": True,
                "effective_from": pd.Timestamp("2024-01-01"),
                "effective_to": pd.Timestamp("2099-12-31"),
                "as_of_from": pd.Timestamp("2024-01-01 09:30:00"),
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59"),
            }
        ])
        
        updates = pd.DataFrame([
            {
                "id": 2,
                "string_field": None,    # NULL string
                "int_field": None,       # NULL int
                "float_field": None,     # NULL float
                "bool_field": None,      # NULL bool
                "effective_from": pd.Timestamp("2024-01-02"),
                "effective_to": pd.Timestamp("2099-12-31"),
                "as_of_from": pd.Timestamp("2024-01-02 09:30:00"),
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59"),
            }
        ])
        
        # Should handle mixed data types with NULLs
        rows_to_expire, rows_to_insert = processor.compute_changes(
            current_state, updates, update_mode='delta'
        )
        
        assert len(rows_to_insert) == 1
        new_row = rows_to_insert.iloc[0]
        assert pd.isna(new_row['string_field'])
        assert pd.isna(new_row['int_field'])
        assert pd.isna(new_row['float_field'])
        assert pd.isna(new_row['bool_field'])
    
    def test_tombstone_with_all_data_types(self):
        """Test tombstone creation with all supported data types."""
        processor = BitemporalTimeseriesProcessor(
            id_columns=['id'],
            value_columns=['string_val', 'int_val', 'float_val', 'bool_val']
        )
        
        current_state = pd.DataFrame([
            {
                "id": 1,
                "string_val": "delete_me",
                "int_val": 999,
                "float_val": 99.9,
                "bool_val": True,
                "effective_from": pd.Timestamp("2024-01-01"),
                "effective_to": pd.Timestamp("2099-12-31"),
                "as_of_from": pd.Timestamp("2024-01-01 09:30:00"),
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59"),
            }
        ])
        
        # Empty updates - should create tombstone for id=1
        updates = pd.DataFrame([], columns=current_state.columns)
        
        # Should create tombstone with all data types
        rows_to_expire, rows_to_insert = processor.compute_changes(
            current_state, updates, update_mode='full_state'
        )
        
        assert len(rows_to_expire) == 1
        assert len(rows_to_insert) == 1  # Tombstone
        
        tombstone = rows_to_insert.iloc[0]
        assert tombstone['string_val'] == "delete_me"
        assert tombstone['int_val'] == 999
        assert tombstone['float_val'] == 99.9
        assert tombstone['bool_val'] == True
        assert tombstone['effective_to'] < pd.Timestamp("2099-12-31")  # Should be truncated