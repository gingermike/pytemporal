"""Tests for schema validation and alignment functionality."""
import pandas as pd
import pytest
from datetime import datetime

from pytemporal import BitemporalTimeseriesProcessor, INFINITY_TIMESTAMP


class TestSchemaAlignment:
    """Tests for _align_schemas method."""

    def setup_method(self):
        """Set up test fixtures."""
        self.processor = BitemporalTimeseriesProcessor(
            id_columns=['id'],
            value_columns=['value']
        )
        self.system_date = '2024-01-15'

    def _make_df(self, columns, data=None):
        """Helper to create DataFrames with temporal columns."""
        if data is None:
            data = []
        df = pd.DataFrame(data, columns=columns)
        # Convert temporal columns to datetime
        for col in ['effective_from', 'effective_to', 'as_of_from', 'as_of_to']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        return df

    def test_identical_schemas_pass(self):
        """Test that identical schemas pass validation."""
        columns = ['id', 'value', 'effective_from', 'effective_to', 'as_of_from', 'as_of_to']

        current_state = self._make_df(columns, [
            [1, 100, '2024-01-01', '2024-12-31', '2024-01-01', INFINITY_TIMESTAMP]
        ])
        updates = self._make_df(columns, [
            [1, 200, '2024-06-01', '2024-12-31', '2024-01-15', INFINITY_TIMESTAMP]
        ])

        # Should not raise
        expire, insert = self.processor.compute_changes(
            current_state, updates, system_date=self.system_date
        )
        assert not insert.empty

    def test_column_reordering(self):
        """Test that columns in different order are automatically aligned."""
        # Current state has standard order
        current_columns = ['id', 'value', 'effective_from', 'effective_to', 'as_of_from', 'as_of_to']
        current_state = self._make_df(current_columns, [
            [1, 100, '2024-01-01', '2024-12-31', '2024-01-01', INFINITY_TIMESTAMP]
        ])

        # Updates have different column order
        updates_columns = ['effective_from', 'effective_to', 'id', 'as_of_from', 'as_of_to', 'value']
        updates = self._make_df(updates_columns, [
            ['2024-06-01', '2024-12-31', 1, '2024-01-15', INFINITY_TIMESTAMP, 200]
        ])

        # Should succeed with automatic reordering
        expire, insert = self.processor.compute_changes(
            current_state, updates, system_date=self.system_date
        )
        assert not insert.empty
        # Verify insert has correct column order (matching current_state)
        assert list(insert.columns)[:6] == current_columns

    def test_extra_column_in_updates_raises_error(self):
        """Test that extra columns in updates raise a clear error."""
        current_columns = ['id', 'value', 'effective_from', 'effective_to', 'as_of_from', 'as_of_to']
        current_state = self._make_df(current_columns, [
            [1, 100, '2024-01-01', '2024-12-31', '2024-01-01', INFINITY_TIMESTAMP]
        ])

        # Updates have an extra 'status' column
        updates_columns = ['id', 'value', 'status', 'effective_from', 'effective_to', 'as_of_from', 'as_of_to']
        updates = self._make_df(updates_columns, [
            [1, 200, 'active', '2024-06-01', '2024-12-31', '2024-01-15', INFINITY_TIMESTAMP]
        ])

        with pytest.raises(ValueError) as exc_info:
            self.processor.compute_changes(current_state, updates, system_date=self.system_date)

        assert "Schema mismatch" in str(exc_info.value)
        assert "status" in str(exc_info.value)
        assert "not in current_state" in str(exc_info.value)

    def test_missing_column_in_updates_raises_error(self):
        """Test that missing columns in updates raise a clear error."""
        current_columns = ['id', 'value', 'extra_field', 'effective_from', 'effective_to', 'as_of_from', 'as_of_to']
        current_state = self._make_df(current_columns, [
            [1, 100, 'data', '2024-01-01', '2024-12-31', '2024-01-01', INFINITY_TIMESTAMP]
        ])

        # Updates missing 'extra_field' column
        updates_columns = ['id', 'value', 'effective_from', 'effective_to', 'as_of_from', 'as_of_to']
        updates = self._make_df(updates_columns, [
            [1, 200, '2024-06-01', '2024-12-31', '2024-01-15', INFINITY_TIMESTAMP]
        ])

        # Need to update processor with correct value_columns
        processor = BitemporalTimeseriesProcessor(
            id_columns=['id'],
            value_columns=['value', 'extra_field']
        )

        with pytest.raises(ValueError) as exc_info:
            processor.compute_changes(current_state, updates, system_date=self.system_date)

        assert "Schema mismatch" in str(exc_info.value)
        assert "extra_field" in str(exc_info.value)
        assert "missing" in str(exc_info.value)

    def test_empty_current_state_accepts_updates_schema(self):
        """Test that empty current_state adopts updates schema."""
        # Empty current state
        current_columns = ['id', 'value', 'effective_from', 'effective_to', 'as_of_from', 'as_of_to']
        current_state = self._make_df(current_columns, [])

        # Updates with same columns but different order
        updates_columns = ['value', 'id', 'effective_from', 'effective_to', 'as_of_from', 'as_of_to']
        updates = self._make_df(updates_columns, [
            [200, 1, '2024-06-01', '2024-12-31', '2024-01-15', INFINITY_TIMESTAMP]
        ])

        # Should succeed - empty current_state is flexible
        expire, insert = self.processor.compute_changes(
            current_state, updates, system_date=self.system_date
        )
        assert not insert.empty

    def test_multiple_extra_columns_error_message(self):
        """Test error message lists all extra columns."""
        current_columns = ['id', 'value', 'effective_from', 'effective_to', 'as_of_from', 'as_of_to']
        current_state = self._make_df(current_columns, [
            [1, 100, '2024-01-01', '2024-12-31', '2024-01-01', INFINITY_TIMESTAMP]
        ])

        # Updates have multiple extra columns
        updates_columns = ['id', 'value', 'status', 'category', 'effective_from', 'effective_to', 'as_of_from', 'as_of_to']
        updates = self._make_df(updates_columns, [
            [1, 200, 'active', 'A', '2024-06-01', '2024-12-31', '2024-01-15', INFINITY_TIMESTAMP]
        ])

        with pytest.raises(ValueError) as exc_info:
            self.processor.compute_changes(current_state, updates, system_date=self.system_date)

        error_msg = str(exc_info.value)
        assert "category" in error_msg
        assert "status" in error_msg

    def test_value_hash_column_ignored_in_validation(self):
        """Test that value_hash column differences are ignored (it's internal)."""
        # Current state has value_hash
        current_columns = ['id', 'value', 'effective_from', 'effective_to', 'as_of_from', 'as_of_to', 'value_hash']
        current_state = self._make_df(current_columns, [
            [1, 100, '2024-01-01', '2024-12-31', '2024-01-01', INFINITY_TIMESTAMP, 'abc123']
        ])

        # Updates don't have value_hash (it will be computed)
        updates_columns = ['id', 'value', 'effective_from', 'effective_to', 'as_of_from', 'as_of_to']
        updates = self._make_df(updates_columns, [
            [1, 200, '2024-06-01', '2024-12-31', '2024-01-15', INFINITY_TIMESTAMP]
        ])

        # Should succeed - value_hash is internal
        expire, insert = self.processor.compute_changes(
            current_state, updates, system_date=self.system_date
        )
        assert not insert.empty


class TestSchemaAlignmentIntegration:
    """Integration tests for schema alignment with real processing."""

    def test_reordered_columns_produce_correct_results(self):
        """Test that reordered columns produce identical results to correctly ordered columns."""
        processor = BitemporalTimeseriesProcessor(
            id_columns=['id'],
            value_columns=['price', 'quantity']
        )
        system_date = '2024-01-15'

        # Standard column order
        standard_columns = ['id', 'price', 'quantity', 'effective_from', 'effective_to', 'as_of_from', 'as_of_to']
        current_state = pd.DataFrame([
            [1, 100.0, 10, pd.Timestamp('2024-01-01'), pd.Timestamp('2024-12-31'),
             pd.Timestamp('2024-01-01'), INFINITY_TIMESTAMP]
        ], columns=standard_columns)

        # Updates with reordered columns
        reordered_columns = ['quantity', 'id', 'price', 'as_of_from', 'as_of_to', 'effective_from', 'effective_to']
        updates_reordered = pd.DataFrame([
            [20, 1, 150.0, pd.Timestamp('2024-01-15'), INFINITY_TIMESTAMP,
             pd.Timestamp('2024-06-01'), pd.Timestamp('2024-12-31')]
        ], columns=reordered_columns)

        # Updates with standard order (for comparison)
        updates_standard = pd.DataFrame([
            [1, 150.0, 20, pd.Timestamp('2024-06-01'), pd.Timestamp('2024-12-31'),
             pd.Timestamp('2024-01-15'), INFINITY_TIMESTAMP]
        ], columns=standard_columns)

        # Process both
        expire1, insert1 = processor.compute_changes(current_state.copy(), updates_reordered, system_date=system_date)
        expire2, insert2 = processor.compute_changes(current_state.copy(), updates_standard, system_date=system_date)

        # Results should be identical
        insert1_sorted = insert1.sort_values(by=['id', 'effective_from']).reset_index(drop=True)
        insert2_sorted = insert2.sort_values(by=['id', 'effective_from']).reset_index(drop=True)

        # Compare key columns (excluding value_hash which may differ due to processing order)
        compare_cols = ['id', 'price', 'quantity', 'effective_from', 'effective_to']
        pd.testing.assert_frame_equal(
            insert1_sorted[compare_cols],
            insert2_sorted[compare_cols]
        )
