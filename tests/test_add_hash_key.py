"""
Tests for the add_hash_key function.

This test suite establishes baseline behavior for the hash key generation
functionality, ensuring consistency, performance, and proper error handling.
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, date
import time

from pytemporal import add_hash_key, BitemporalTimeseriesProcessor, INFINITY_TIMESTAMP


class TestAddHashKeyBasicFunctionality:
    """Test basic functionality of add_hash_key."""
    
    def test_basic_hash_addition(self):
        """Test that hash column is added correctly."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'price': [100, 200, 300],
            'volume': [10, 20, 30]
        })
        
        result = add_hash_key(df, ['price', 'volume'])
        
        # Check that all original columns are preserved
        assert all(col in result.columns for col in df.columns)
        
        # Check that hash column is added
        assert 'value_hash' in result.columns
        
        # Check that data is preserved
        pd.testing.assert_frame_equal(result.drop('value_hash', axis=1), df)
        
        # Check that hash values are int64 (as used internally)
        assert result['value_hash'].dtype == np.int64
        
        # Check that hash values are non-zero and different for different input
        assert result['value_hash'].iloc[0] != 0
        assert result['value_hash'].iloc[0] != result['value_hash'].iloc[1]
    
    def test_single_value_field(self):
        """Test hash with single value field."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'price': [100, 200, 100]  # Duplicate values
        })
        
        result = add_hash_key(df, ['price'])
        
        # Same price should produce same hash
        assert result['value_hash'].iloc[0] == result['value_hash'].iloc[2]
        # Different price should produce different hash
        assert result['value_hash'].iloc[0] != result['value_hash'].iloc[1]
    
    def test_multiple_value_fields(self):
        """Test hash with multiple value fields."""
        df = pd.DataFrame({
            'id': [1, 2, 3, 4],
            'price': [100, 100, 200, 100],
            'volume': [10, 20, 10, 10],
            'quantity': [5, 5, 10, 5]
        })
        
        result = add_hash_key(df, ['price', 'volume', 'quantity'])
        
        # Rows 0 and 3 have identical values, should have same hash
        assert result['value_hash'].iloc[0] == result['value_hash'].iloc[3]
        
        # Other rows should have different hashes
        assert result['value_hash'].iloc[0] != result['value_hash'].iloc[1]
        assert result['value_hash'].iloc[0] != result['value_hash'].iloc[2]
        assert result['value_hash'].iloc[1] != result['value_hash'].iloc[2]
    
    def test_hash_deterministic(self):
        """Test that hash is deterministic across multiple calls."""
        df = pd.DataFrame({
            'price': [100, 200, 300],
            'volume': [10, 20, 30]
        })
        
        result1 = add_hash_key(df, ['price', 'volume'])
        result2 = add_hash_key(df, ['price', 'volume'])
        
        # Hashes should be identical across calls
        pd.testing.assert_series_equal(result1['value_hash'], result2['value_hash'])


class TestAddHashKeyDataTypes:
    """Test add_hash_key with different data types."""
    
    def test_string_values(self):
        """Test hash with string values."""
        df = pd.DataFrame({
            'category': ['apple', 'banana', 'apple'],
            'subcategory': ['red', 'yellow', 'green']
        })
        
        result = add_hash_key(df, ['category', 'subcategory'])
        
        # Check that string hashing works
        assert 'value_hash' in result.columns
        assert result['value_hash'].dtype == np.int64
        
        # Different strings should produce different hashes
        assert result['value_hash'].iloc[0] != result['value_hash'].iloc[1]
        # Same category, different subcategory should produce different hashes
        assert result['value_hash'].iloc[0] != result['value_hash'].iloc[2]
    
    def test_integer_values(self):
        """Test hash with integer values."""
        df = pd.DataFrame({
            'int32_col': np.array([1, 2, 3], dtype=np.int32),
            'int64_col': np.array([100, 200, 300], dtype=np.int64)
        })
        
        result = add_hash_key(df, ['int32_col', 'int64_col'])
        
        assert 'value_hash' in result.columns
        assert len(set(result['value_hash'])) == 3  # All different
    
    def test_float_values(self):
        """Test hash with float values."""
        df = pd.DataFrame({
            'price': [99.99, 199.99, 99.99],
            'rate': [0.1, 0.2, 0.1]
        })
        
        result = add_hash_key(df, ['price', 'rate'])
        
        # Identical float values should produce identical hashes
        assert result['value_hash'].iloc[0] == result['value_hash'].iloc[2]
        assert result['value_hash'].iloc[0] != result['value_hash'].iloc[1]
    
    def test_date_values(self):
        """Test hash with date/datetime values."""
        df = pd.DataFrame({
            'date_col': pd.to_datetime(['2020-01-01', '2020-01-02', '2020-01-01']),
            'value': [100, 200, 300]
        })
        
        result = add_hash_key(df, ['date_col', 'value'])
        
        # Same date, different value should produce different hashes
        assert result['value_hash'].iloc[0] != result['value_hash'].iloc[2]
        assert result['value_hash'].iloc[0] != result['value_hash'].iloc[1]
    
    def test_mixed_data_types(self):
        """Test hash with mixed data types."""
        df = pd.DataFrame({
            'str_col': ['A', 'B', 'A'],
            'int_col': [1, 2, 1],
            'float_col': [1.5, 2.5, 1.5],
            'date_col': pd.to_datetime(['2020-01-01', '2020-01-02', '2020-01-01'])
        })
        
        result = add_hash_key(df, ['str_col', 'int_col', 'float_col', 'date_col'])
        
        # Row 0 and 2 should have identical hashes (all columns match)
        assert result['value_hash'].iloc[0] == result['value_hash'].iloc[2]
        # Row 1 should be different
        assert result['value_hash'].iloc[0] != result['value_hash'].iloc[1]


class TestAddHashKeyErrorHandling:
    """Test error handling in add_hash_key."""
    
    def test_empty_dataframe(self):
        """Test that empty DataFrame raises appropriate error."""
        df = pd.DataFrame()
        
        with pytest.raises(ValueError, match="Cannot add hash key to empty DataFrame"):
            add_hash_key(df, ['price'])
    
    def test_nonexistent_column(self):
        """Test that nonexistent column raises appropriate error."""
        df = pd.DataFrame({'price': [100, 200]})
        
        with pytest.raises(ValueError, match="Value fields not found in DataFrame"):
            add_hash_key(df, ['nonexistent_column'])
    
    def test_multiple_missing_columns(self):
        """Test error message with multiple missing columns."""
        df = pd.DataFrame({'price': [100, 200]})
        
        with pytest.raises(ValueError, match="Value fields not found in DataFrame"):
            add_hash_key(df, ['missing1', 'missing2'])
    
    def test_partial_missing_columns(self):
        """Test error when some columns exist and some don't."""
        df = pd.DataFrame({'price': [100, 200], 'volume': [10, 20]})
        
        with pytest.raises(ValueError, match="Value fields not found in DataFrame"):
            add_hash_key(df, ['price', 'nonexistent'])
    
    def test_empty_value_fields(self):
        """Test behavior with empty value_fields list."""
        df = pd.DataFrame({'price': [100, 200]})
        
        # Should work but produce potentially identical hashes for all rows
        result = add_hash_key(df, [])
        assert 'value_hash' in result.columns


class TestAddHashKeyConsistency:
    """Test consistency with internal bitemporal algorithm."""
    
    def test_consistency_with_internal_processor(self):
        """Test that hashes match those generated by internal processor."""
        # Create data in bitemporal format
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'field': ['A', 'B', 'C'],
            'price': [100, 200, 300],
            'volume': [10, 20, 30],
            'effective_from': pd.to_datetime(['2020-01-01', '2020-02-01', '2020-03-01']),
            'effective_to': pd.to_datetime(['2021-01-01', '2021-02-01', '2021-03-01']),
            'as_of_from': pd.to_datetime(['2025-01-01', '2025-01-01', '2025-01-01']),
            'as_of_to': [INFINITY_TIMESTAMP, INFINITY_TIMESTAMP, INFINITY_TIMESTAMP],
            'value_hash': [0, 0, 0]  # Placeholder
        })
        
        # Get hash from our function
        hash_result = add_hash_key(df.drop('value_hash', axis=1), ['price', 'volume'])
        external_hashes = hash_result['value_hash'].tolist()
        
        # Create processor and test with no-op update to see internal hashes
        processor = BitemporalTimeseriesProcessor(['id', 'field'], ['price', 'volume'])
        
        # Update the DataFrame with computed hashes and test
        df_with_hashes = df.copy()
        df_with_hashes['value_hash'] = external_hashes
        
        # No-change update should produce no changes if hashes are correct
        empty_updates = pd.DataFrame(columns=df.columns)
        to_expire, to_insert = processor.compute_changes(df_with_hashes, empty_updates, '2025-01-02')
        
        # If our hashes are correct, there should be no changes needed
        assert len(to_expire) == 0, "Hash mismatch detected - internal processor wants to expire records"
        assert len(to_insert) == 0, "Hash mismatch detected - internal processor wants to insert records"
    
    def test_hash_order_independence(self):
        """Test that row order doesn't affect individual hash values."""
        df1 = pd.DataFrame({
            'price': [100, 200, 300],
            'volume': [10, 20, 30]
        })
        
        df2 = pd.DataFrame({
            'price': [300, 100, 200],  # Different order
            'volume': [30, 10, 20]
        })
        
        result1 = add_hash_key(df1, ['price', 'volume'])
        result2 = add_hash_key(df2, ['price', 'volume'])
        
        # Hash for [100,10] should be same in both DataFrames
        hash_100_10_df1 = result1[result1['price'] == 100]['value_hash'].iloc[0]
        hash_100_10_df2 = result2[result2['price'] == 100]['value_hash'].iloc[0]
        
        assert hash_100_10_df1 == hash_100_10_df2


class TestAddHashKeyPerformance:
    """Performance baseline tests for add_hash_key."""
    
    def test_performance_baseline_small(self):
        """Baseline performance for small dataset (1000 rows)."""
        df = pd.DataFrame({
            'id': range(1000),
            'price': np.random.randint(100, 1000, 1000),
            'volume': np.random.randint(1, 100, 1000),
            'category': [f'cat_{i%10}' for i in range(1000)]
        })
        
        start_time = time.time()
        result = add_hash_key(df, ['price', 'volume', 'category'])
        elapsed = time.time() - start_time
        
        # Should complete in reasonable time (baseline)
        assert elapsed < 1.0, f"Small dataset took {elapsed:.3f}s (expected < 1.0s)"
        assert len(result) == 1000
        assert 'value_hash' in result.columns
    
    def test_performance_baseline_medium(self):
        """Baseline performance for medium dataset (10000 rows)."""
        df = pd.DataFrame({
            'id': range(10000),
            'price': np.random.randint(100, 1000, 10000),
            'volume': np.random.randint(1, 100, 10000)
        })
        
        start_time = time.time()
        result = add_hash_key(df, ['price', 'volume'])
        elapsed = time.time() - start_time
        
        # Should complete in reasonable time (baseline)
        assert elapsed < 2.0, f"Medium dataset took {elapsed:.3f}s (expected < 2.0s)"
        assert len(result) == 10000
        
        # Performance should scale reasonably (rough check)
        print(f"Medium dataset (10k rows) performance: {elapsed:.3f}s")


class TestAddHashKeyEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_single_row(self):
        """Test with single row DataFrame."""
        df = pd.DataFrame({'price': [100], 'volume': [10]})
        
        result = add_hash_key(df, ['price', 'volume'])
        
        assert len(result) == 1
        assert 'value_hash' in result.columns
        assert result['value_hash'].iloc[0] != 0
    
    def test_duplicate_rows(self):
        """Test with completely duplicate rows."""
        df = pd.DataFrame({
            'price': [100, 100, 100],
            'volume': [10, 10, 10]
        })
        
        result = add_hash_key(df, ['price', 'volume'])
        
        # All rows should have identical hashes
        unique_hashes = result['value_hash'].nunique()
        assert unique_hashes == 1
    
    def test_large_numbers(self):
        """Test with large numeric values."""
        df = pd.DataFrame({
            'big_int': [9223372036854775807, 9223372036854775806],  # Near int64 max
            'big_float': [1e15, 1e16]
        })
        
        result = add_hash_key(df, ['big_int', 'big_float'])
        
        assert 'value_hash' in result.columns
        assert result['value_hash'].iloc[0] != result['value_hash'].iloc[1]
    
    def test_unicode_strings(self):
        """Test with unicode strings."""
        df = pd.DataFrame({
            'unicode_col': ['Hello 世界', '🚀 Rocket', 'Café'],
            'value': [1, 2, 3]
        })
        
        result = add_hash_key(df, ['unicode_col', 'value'])
        
        assert 'value_hash' in result.columns
        # All should be different
        assert result['value_hash'].nunique() == 3
    
    def test_field_order_consistency(self):
        """Test that field order in value_fields matters."""
        df = pd.DataFrame({
            'a': [1, 2],
            'b': [10, 20],
            'c': [100, 200]
        })
        
        result1 = add_hash_key(df, ['a', 'b'])
        result2 = add_hash_key(df, ['b', 'a'])  # Different order
        
        # Field order should matter for hash computation
        assert not result1['value_hash'].equals(result2['value_hash'])


if __name__ == '__main__':
    # Run the tests if executed directly
    pytest.main([__file__, '-v'])