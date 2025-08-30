"""
Tests for timezone handling in bitemporal processor.

These tests ensure that timezone-aware timestamps from PostgreSQL timestamptz 
columns are handled correctly without causing schema mismatches.
"""

import pandas as pd
import pytest
from pytemporal import BitemporalTimeseriesProcessor
import pytz
from datetime import datetime


class TestTimezoneHandling:
    """Test timezone-aware timestamp handling."""
    
    def test_timezone_aware_current_state(self):
        """Test handling of timezone-aware timestamps from PostgreSQL."""
        processor = BitemporalTimeseriesProcessor(
            id_columns=['id'],
            value_columns=['value']
        )
        
        # Simulate current state from PostgreSQL with timestamptz (timezone-aware)
        tz_utc = pytz.UTC
        tz_est = pytz.timezone('US/Eastern')
        
        current_state = pd.DataFrame([
            {
                "id": 1,
                "value": "test",
                "effective_from": pd.Timestamp("2024-01-01", tz=tz_utc),
                "effective_to": pd.Timestamp("2099-12-31", tz=tz_utc),
                "as_of_from": pd.Timestamp("2024-01-01 09:30:00", tz=tz_utc),
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59", tz=tz_utc),
            }
        ])
        
        # Updates with timezone-naive timestamps (common from client code)
        updates = pd.DataFrame([
            {
                "id": 2,
                "value": "new",
                "effective_from": pd.Timestamp("2024-01-02"),
                "effective_to": pd.Timestamp("2099-12-31"),
                "as_of_from": pd.Timestamp("2024-01-02 10:00:00"),
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59"),
            }
        ])
        
        # Should handle mixed timezone-aware and timezone-naive without errors
        rows_to_expire, rows_to_insert = processor.compute_changes(
            current_state, updates, update_mode='delta'
        )
        
        assert isinstance(rows_to_expire, pd.DataFrame)
        assert isinstance(rows_to_insert, pd.DataFrame)
        assert len(rows_to_insert) == 1
        
    def test_mixed_timezones(self):
        """Test handling of different timezone scenarios across separate DataFrames."""
        processor = BitemporalTimeseriesProcessor(
            id_columns=['id'],
            value_columns=['value']
        )
        
        # Current state with UTC timezone
        current_state = pd.DataFrame([
            {
                "id": 1,
                "value": "utc",
                "effective_from": pd.Timestamp("2024-01-01", tz=pytz.UTC),
                "effective_to": pd.Timestamp("2099-12-31", tz=pytz.UTC),
                "as_of_from": pd.Timestamp("2024-01-01 09:30:00", tz=pytz.UTC),
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59", tz=pytz.UTC),
            }
        ])
        
        # Updates with different timezone (EST)
        est_tz = pytz.timezone('US/Eastern')
        updates = pd.DataFrame([
            {
                "id": 2,
                "value": "est",
                "effective_from": pd.Timestamp("2024-01-03", tz=est_tz),
                "effective_to": pd.Timestamp("2099-12-31", tz=est_tz),
                "as_of_from": pd.Timestamp("2024-01-03 10:00:00", tz=est_tz),
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59", tz=est_tz),
            }
        ])
        
        # Should process different timezones across DataFrames
        rows_to_expire, rows_to_insert = processor.compute_changes(
            current_state, updates, update_mode='delta'
        )
        
        assert len(rows_to_insert) == 1
        
    def test_preserve_timezone_info(self):
        """Test that timezone information is preserved through processing."""
        processor = BitemporalTimeseriesProcessor(
            id_columns=['id'],
            value_columns=['value']
        )
        
        # Create timezone-aware current state
        tz_utc = pytz.UTC
        original_timestamp = pd.Timestamp("2024-01-01 09:30:00", tz=tz_utc)
        
        current_state = pd.DataFrame([
            {
                "id": 1,
                "value": "test",
                "effective_from": pd.Timestamp("2024-01-01", tz=tz_utc),
                "effective_to": pd.Timestamp("2099-12-31", tz=tz_utc),
                "as_of_from": original_timestamp,
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59", tz=tz_utc),
            }
        ])
        
        # Verify timezone is preserved after _prepare_dataframe
        prepared_df = processor._prepare_dataframe(current_state)
        
        # Check that timezone information is still there
        assert prepared_df['as_of_from'].dt.tz is not None
        assert prepared_df['effective_from'].dt.tz is not None
        
        # The actual timestamp values should be preserved
        assert prepared_df['as_of_from'].iloc[0].tz == tz_utc
        
    def test_already_datetime_not_converted(self):
        """Test that existing datetime columns are not unnecessarily converted."""
        processor = BitemporalTimeseriesProcessor(
            id_columns=['id'],
            value_columns=['value']
        )
        
        # Create DataFrame with existing datetime columns
        original_timestamp = pd.Timestamp("2024-01-01 09:30:00", tz=pytz.UTC)
        
        df = pd.DataFrame([
            {
                "id": 1,
                "value": "test", 
                "effective_from": pd.Timestamp("2024-01-01"),  # Already datetime
                "effective_to": pd.Timestamp("2099-12-31"),    # Already datetime
                "as_of_from": original_timestamp,              # Already datetime with tz
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59", tz=pytz.UTC),
            }
        ])
        
        # Store original dtypes
        original_dtypes = {
            'effective_from': df['effective_from'].dtype,
            'effective_to': df['effective_to'].dtype,
            'as_of_from': df['as_of_from'].dtype,
            'as_of_to': df['as_of_to'].dtype,
        }
        
        # Process through _prepare_dataframe
        result_df = processor._prepare_dataframe(df)
        
        # Verify dtypes are preserved (no unnecessary conversion)
        for col in ['effective_from', 'effective_to', 'as_of_from', 'as_of_to']:
            # Should maintain datetime type
            assert pd.api.types.is_datetime64_any_dtype(result_df[col])
            # Timezone info should be preserved where it existed
            if hasattr(original_dtypes[col], 'tz') and original_dtypes[col].tz is not None:
                assert result_df[col].dt.tz is not None
            elif result_df[col].dt.tz is not None:  # Column gained timezone info
                assert result_df[col].dt.tz is not None
                
    def test_string_dates_still_converted(self):
        """Test that string dates are still properly converted."""
        processor = BitemporalTimeseriesProcessor(
            id_columns=['id'],
            value_columns=['value']
        )
        
        # Create DataFrame with string date columns (needs conversion)
        df = pd.DataFrame([
            {
                "id": 1,
                "value": "test",
                "effective_from": "2024-01-01",          # String, needs conversion
                "effective_to": "2099-12-31",            # String, needs conversion  
                "as_of_from": "2024-01-01 09:30:00",     # String, needs conversion
                "as_of_to": "2099-12-31 23:59:59",       # String, needs conversion
            }
        ])
        
        # Process through _prepare_dataframe
        result_df = processor._prepare_dataframe(df)
        
        # Verify all columns are converted to datetime
        for col in ['effective_from', 'effective_to', 'as_of_from', 'as_of_to']:
            assert pd.api.types.is_datetime64_any_dtype(result_df[col])
            
        # Verify actual conversion worked
        assert result_df['effective_from'].iloc[0] == pd.Timestamp("2024-01-01")
        assert result_df['as_of_from'].iloc[0] == pd.Timestamp("2024-01-01 09:30:00")

    def test_postgresql_timestamptz_scenario(self):
        """Test the specific PostgreSQL timestamptz scenario mentioned by the user."""
        processor = BitemporalTimeseriesProcessor(
            id_columns=['portfolio_id', 'security_id'],
            value_columns=['quantity', 'price']
        )
        
        # Simulate current state from PostgreSQL with timestamptz columns
        # This is what would come from a PostgreSQL database query
        pg_utc = pytz.UTC
        current_state = pd.DataFrame([
            {
                "portfolio_id": 1,
                "security_id": "AAPL",
                "quantity": 100,
                "price": 150.25,
                "effective_from": pd.Timestamp("2024-01-01", tz=pg_utc),
                "effective_to": pd.Timestamp("2099-12-31", tz=pg_utc), 
                "as_of_from": pd.Timestamp("2024-01-01 09:30:00", tz=pg_utc),
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59", tz=pg_utc),
            }
        ])
        
        # Simulate updates from client application (likely timezone-naive)
        updates = pd.DataFrame([
            {
                "portfolio_id": 1,
                "security_id": "AAPL", 
                "quantity": 200,
                "price": 155.50,
                "effective_from": pd.Timestamp("2024-01-02"),
                "effective_to": pd.Timestamp("2099-12-31"),
                "as_of_from": pd.Timestamp("2024-01-02 10:00:00"),
                "as_of_to": pd.Timestamp("2099-12-31 23:59:59"),
            }
        ])
        
        # This scenario should work without schema mismatch errors
        try:
            rows_to_expire, rows_to_insert = processor.compute_changes(
                current_state, updates, update_mode='delta'
            )
            
            # Verify processing succeeded
            assert isinstance(rows_to_expire, pd.DataFrame)
            assert isinstance(rows_to_insert, pd.DataFrame)
            assert len(rows_to_expire) == 1  # Should expire current record
            assert len(rows_to_insert) == 2  # Should insert split timeline
            
        except Exception as e:
            pytest.fail(f"PostgreSQL timestamptz scenario failed: {e}")