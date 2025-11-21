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
        
        # Check that hash values are strings (SHA256 hex digests)
        assert result['value_hash'].dtype == object  # strings are stored as object type
        
        # Check that hash values are non-empty and different for different input
        assert result['value_hash'].iloc[0] != ""
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
        assert result['value_hash'].dtype == object  # strings are stored as object type
        
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
    
    def test_boolean_values(self):
        """Test hash with Boolean values."""
        df = pd.DataFrame({
            'active': [True, False, True, False],
            'verified': [False, True, True, False],
            'enabled': [True, True, False, False]
        })
        
        result = add_hash_key(df, ['active', 'verified'])
        
        # Check that Boolean hashing works
        assert 'value_hash' in result.columns
        assert result['value_hash'].dtype == object  # strings are stored as object type
        
        # Different Boolean combinations should produce different hashes
        hash_values = result['value_hash'].tolist()
        unique_hashes = len(set(hash_values))
        
        # We have [True,False], [False,True], [True,True], [False,False] - all different
        assert unique_hashes == 4
        
        # Test specific combinations
        true_false_mask = (result['active'] == True) & (result['verified'] == False)
        false_true_mask = (result['active'] == False) & (result['verified'] == True)
        
        assert result[true_false_mask]['value_hash'].iloc[0] != result[false_true_mask]['value_hash'].iloc[0]
        
    def test_boolean_consistency(self):
        """Test that same Boolean values produce same hashes."""
        df1 = pd.DataFrame({
            'flag1': [True, False],
            'flag2': [False, True]
        })
        
        df2 = pd.DataFrame({
            'flag1': [False, True, True],
            'flag2': [True, False, False]
        })
        
        result1 = add_hash_key(df1, ['flag1', 'flag2'])
        result2 = add_hash_key(df2, ['flag1', 'flag2'])
        
        # [True, False] should produce same hash in both DataFrames
        true_false_hash_1 = result1[(result1['flag1'] == True) & (result1['flag2'] == False)]['value_hash'].iloc[0]
        true_false_hash_2 = result2[(result2['flag1'] == True) & (result2['flag2'] == False)]['value_hash'].iloc[0]
        
        assert true_false_hash_1 == true_false_hash_2
        
    def test_boolean_with_other_types(self):
        """Test Boolean values mixed with other data types."""
        df = pd.DataFrame({
            'active': [True, False, True, False],
            'count': [1, 2, 1, 2],
            'name': ['A', 'B', 'C', 'D']
        })
        
        # Test Boolean + Integer
        result1 = add_hash_key(df, ['active', 'count'])
        
        # Rows with same [active, count] should have same hash
        mask_true_1 = (result1['active'] == True) & (result1['count'] == 1)
        true_1_rows = result1[mask_true_1]
        if len(true_1_rows) > 1:
            assert all(true_1_rows['value_hash'] == true_1_rows['value_hash'].iloc[0])
        
        # Test Boolean + String
        result2 = add_hash_key(df, ['active', 'name'])
        assert 'value_hash' in result2.columns
        
        # All should be different (unique combinations)
        assert result2['value_hash'].nunique() == 4
        
    def test_mixed_data_types(self):
        """Test hash with mixed data types including Boolean."""
        df = pd.DataFrame({
            'str_col': ['A', 'B', 'A'],
            'int_col': [1, 2, 1],
            'float_col': [1.5, 2.5, 1.5],
            'bool_col': [True, False, True],
            'date_col': pd.to_datetime(['2020-01-01', '2020-01-02', '2020-01-01'])
        })
        
        result = add_hash_key(df, ['str_col', 'int_col', 'float_col', 'bool_col', 'date_col'])
        
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
        assert result['value_hash'].iloc[0] != ""
    
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
    
    def test_boolean_edge_cases(self):
        """Test edge cases with Boolean values."""
        # All True
        df1 = pd.DataFrame({'flag': [True, True, True]})
        result1 = add_hash_key(df1, ['flag'])
        assert result1['value_hash'].nunique() == 1  # All should be same
        
        # All False  
        df2 = pd.DataFrame({'flag': [False, False, False]})
        result2 = add_hash_key(df2, ['flag'])
        assert result2['value_hash'].nunique() == 1  # All should be same
        
        # Mixed
        df3 = pd.DataFrame({'flag': [True, False, True]})
        result3 = add_hash_key(df3, ['flag'])
        assert result3['value_hash'].nunique() == 2  # Two unique values
        
        # Single Boolean column
        df4 = pd.DataFrame({'active': [True]})
        result4 = add_hash_key(df4, ['active'])
        assert len(result4) == 1
        assert result4['value_hash'].iloc[0] != ""
    
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


class TestHashAlgorithmParameter:
    """Test the hash_algorithm parameter functionality."""

    def test_default_algorithm_is_xxhash(self):
        """Test that default algorithm is xxhash."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'price': [100.0, 200.0, 100.0],
            'volume': [10, 20, 10]
        })

        result_default = add_hash_key(df, ['price', 'volume'])
        result_explicit_xxhash = add_hash_key(df, ['price', 'volume'], hash_algorithm='xxhash')

        # Default should match explicit xxhash
        pd.testing.assert_series_equal(
            result_default['value_hash'],
            result_explicit_xxhash['value_hash'],
            check_names=False
        )

    def test_xxhash_algorithm_explicit(self):
        """Test explicit xxhash algorithm parameter."""
        df = pd.DataFrame({
            'id': [1, 2],
            'value': [100, 200]
        })

        result = add_hash_key(df, ['value'], hash_algorithm='xxhash')

        # Check hash column exists
        assert 'value_hash' in result.columns
        # Check hash values are non-empty strings
        assert all(isinstance(h, str) and len(h) > 0 for h in result['value_hash'])
        # XxHash produces 16 character hex strings
        assert all(len(h) == 16 for h in result['value_hash'])

    def test_sha256_algorithm(self):
        """Test SHA256 algorithm parameter."""
        df = pd.DataFrame({
            'id': [1, 2],
            'value': [100, 200]
        })

        result = add_hash_key(df, ['value'], hash_algorithm='sha256')

        # Check hash column exists
        assert 'value_hash' in result.columns
        # Check hash values are non-empty strings
        assert all(isinstance(h, str) and len(h) > 0 for h in result['value_hash'])
        # SHA256 produces 64 character hex strings
        assert all(len(h) == 64 for h in result['value_hash'])

    def test_xxhash_vs_sha256_different(self):
        """Test that xxhash and sha256 produce different hash values."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'price': [100.0, 200.0, 100.0],
            'volume': [10, 20, 10]
        })

        result_xxhash = add_hash_key(df, ['price', 'volume'], hash_algorithm='xxhash')
        result_sha256 = add_hash_key(df, ['price', 'volume'], hash_algorithm='sha256')

        # Different algorithms should produce different hashes
        assert not result_xxhash['value_hash'].equals(result_sha256['value_hash'])

        # But both should still have same values produce same hashes within algorithm
        assert result_xxhash.loc[0, 'value_hash'] == result_xxhash.loc[2, 'value_hash']
        assert result_sha256.loc[0, 'value_hash'] == result_sha256.loc[2, 'value_hash']

    def test_invalid_algorithm_raises_error(self):
        """Test that invalid algorithm parameter raises appropriate error."""
        df = pd.DataFrame({
            'id': [1, 2],
            'value': [100, 200]
        })

        with pytest.raises((ValueError, RuntimeError)) as exc_info:
            add_hash_key(df, ['value'], hash_algorithm='invalid_algorithm')

        # Error message should mention the unknown algorithm
        assert 'algorithm' in str(exc_info.value).lower() or 'invalid' in str(exc_info.value).lower()

    def test_algorithm_case_insensitive(self):
        """Test that algorithm parameter is case-insensitive."""
        df = pd.DataFrame({
            'id': [1, 2],
            'value': [100, 200]
        })

        result_lower = add_hash_key(df, ['value'], hash_algorithm='xxhash')
        result_upper = add_hash_key(df, ['value'], hash_algorithm='XXHASH')
        result_mixed = add_hash_key(df, ['value'], hash_algorithm='XxHash')

        # All case variations should produce identical results
        pd.testing.assert_series_equal(result_lower['value_hash'], result_upper['value_hash'], check_names=False)
        pd.testing.assert_series_equal(result_lower['value_hash'], result_mixed['value_hash'], check_names=False)

    def test_algorithm_aliases(self):
        """Test that algorithm aliases work correctly."""
        df = pd.DataFrame({
            'id': [1, 2],
            'value': [100, 200]
        })

        # Test xxhash aliases
        result_xxhash = add_hash_key(df, ['value'], hash_algorithm='xxhash')
        result_xx = add_hash_key(df, ['value'], hash_algorithm='xx')
        pd.testing.assert_series_equal(result_xxhash['value_hash'], result_xx['value_hash'], check_names=False)

        # Test sha256 aliases
        result_sha256 = add_hash_key(df, ['value'], hash_algorithm='sha256')
        result_sha = add_hash_key(df, ['value'], hash_algorithm='sha')
        pd.testing.assert_series_equal(result_sha256['value_hash'], result_sha['value_hash'], check_names=False)

    def test_hash_consistency_within_algorithm(self):
        """Test that same input produces same hash within an algorithm."""
        df = pd.DataFrame({
            'id': [1, 2, 3, 4],
            'price': [100.0, 200.0, 100.0, 300.0],
            'volume': [10, 20, 10, 30]
        })

        # Test xxhash consistency
        result1_xxhash = add_hash_key(df, ['price', 'volume'], hash_algorithm='xxhash')
        result2_xxhash = add_hash_key(df, ['price', 'volume'], hash_algorithm='xxhash')
        pd.testing.assert_series_equal(result1_xxhash['value_hash'], result2_xxhash['value_hash'], check_names=False)

        # Test sha256 consistency
        result1_sha256 = add_hash_key(df, ['price', 'volume'], hash_algorithm='sha256')
        result2_sha256 = add_hash_key(df, ['price', 'volume'], hash_algorithm='sha256')
        pd.testing.assert_series_equal(result1_sha256['value_hash'], result2_sha256['value_hash'], check_names=False)

    def test_algorithm_with_complex_data_types(self):
        """Test algorithm parameter works with various data types."""
        df = pd.DataFrame({
            'string_col': ['abc', 'def'],
            'int_col': [1, 2],
            'float_col': [1.5, 2.5],
            'bool_col': [True, False],
            'date_col': [date(2024, 1, 1), date(2024, 1, 2)],
            'datetime_col': [datetime(2024, 1, 1, 12, 0), datetime(2024, 1, 2, 13, 0)]
        })

        value_fields = ['string_col', 'int_col', 'float_col', 'bool_col', 'date_col', 'datetime_col']

        # Both algorithms should handle all data types
        result_xxhash = add_hash_key(df, value_fields, hash_algorithm='xxhash')
        result_sha256 = add_hash_key(df, value_fields, hash_algorithm='sha256')

        # Verify hashes were computed
        assert all(len(h) == 16 for h in result_xxhash['value_hash'])
        assert all(len(h) == 64 for h in result_sha256['value_hash'])

        # Different rows should have different hashes
        assert result_xxhash['value_hash'].iloc[0] != result_xxhash['value_hash'].iloc[1]
        assert result_sha256['value_hash'].iloc[0] != result_sha256['value_hash'].iloc[1]

    def test_algorithm_backward_compatibility(self):
        """Test that omitting algorithm parameter still works (backward compatibility)."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'value': [100, 200, 300]
        })

        # Should work without algorithm parameter (uses default xxhash)
        result = add_hash_key(df, ['value'])

        assert 'value_hash' in result.columns
        assert all(len(h) == 16 for h in result['value_hash'])  # XxHash length


if __name__ == '__main__':
    # Run the tests if executed directly
    pytest.main([__file__, '-v'])