"""Tests for schema alignment (column reordering) functionality."""
import pandas as pd
import pytest

from pytemporal import BitemporalTimeseriesProcessor, INFINITY_TIMESTAMP


class TestSchemaAlignment:
    """Tests for _align_schemas column reordering."""

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
        for col in ['effective_from', 'effective_to', 'as_of_from', 'as_of_to']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        return df

    def test_identical_schemas_pass(self):
        """Test that identical schemas work correctly."""
        columns = ['id', 'value', 'effective_from', 'effective_to', 'as_of_from', 'as_of_to']

        current_state = self._make_df(columns, [
            [1, 100, '2024-01-01', '2024-12-31', '2024-01-01', INFINITY_TIMESTAMP]
        ])
        updates = self._make_df(columns, [
            [1, 200, '2024-06-01', '2024-12-31', '2024-01-15', INFINITY_TIMESTAMP]
        ])

        expire, insert = self.processor.compute_changes(
            current_state, updates, system_date=self.system_date
        )
        assert not insert.empty

    def test_column_reordering(self):
        """Test that columns in different order are automatically aligned."""
        current_columns = ['id', 'value', 'effective_from', 'effective_to', 'as_of_from', 'as_of_to']
        current_state = self._make_df(current_columns, [
            [1, 100, '2024-01-01', '2024-12-31', '2024-01-01', INFINITY_TIMESTAMP]
        ])

        # Updates have different column order
        updates_columns = ['effective_from', 'effective_to', 'id', 'as_of_from', 'as_of_to', 'value']
        updates = self._make_df(updates_columns, [
            ['2024-06-01', '2024-12-31', 1, '2024-01-15', INFINITY_TIMESTAMP, 200]
        ])

        expire, insert = self.processor.compute_changes(
            current_state, updates, system_date=self.system_date
        )
        assert not insert.empty

    def test_extra_columns_in_current_state_filtered(self):
        """Test that extra DB columns in current_state are filtered out."""
        # current_state has extra DB columns (id, effective_range, etc.)
        current_columns = ['db_id', 'id', 'value', 'effective_range', 'effective_from', 'effective_to', 'as_of_from', 'as_of_to']
        current_state = self._make_df(current_columns, [
            [999, 1, 100, '[2024-01-01,2024-12-31)', '2024-01-01', '2024-12-31', '2024-01-01', INFINITY_TIMESTAMP]
        ])

        # Updates don't have DB-specific columns
        updates_columns = ['id', 'value', 'effective_from', 'effective_to', 'as_of_from', 'as_of_to']
        updates = self._make_df(updates_columns, [
            [1, 200, '2024-06-01', '2024-12-31', '2024-01-15', INFINITY_TIMESTAMP]
        ])

        expire, insert = self.processor.compute_changes(
            current_state, updates, system_date=self.system_date
        )
        assert not insert.empty

    def test_empty_current_state(self):
        """Test that empty current_state works correctly."""
        current_columns = ['id', 'value', 'effective_from', 'effective_to', 'as_of_from', 'as_of_to']
        current_state = self._make_df(current_columns, [])

        updates_columns = ['value', 'id', 'effective_from', 'effective_to', 'as_of_from', 'as_of_to']
        updates = self._make_df(updates_columns, [
            [200, 1, '2024-06-01', '2024-12-31', '2024-01-15', INFINITY_TIMESTAMP]
        ])

        expire, insert = self.processor.compute_changes(
            current_state, updates, system_date=self.system_date
        )
        assert not insert.empty

    def test_reordered_columns_produce_correct_results(self):
        """Test that reordered columns produce identical results."""
        processor = BitemporalTimeseriesProcessor(
            id_columns=['id'],
            value_columns=['price', 'quantity']
        )
        system_date = '2024-01-15'

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

        # Updates with standard order
        updates_standard = pd.DataFrame([
            [1, 150.0, 20, pd.Timestamp('2024-06-01'), pd.Timestamp('2024-12-31'),
             pd.Timestamp('2024-01-15'), INFINITY_TIMESTAMP]
        ], columns=standard_columns)

        expire1, insert1 = processor.compute_changes(current_state.copy(), updates_reordered, system_date=system_date)
        expire2, insert2 = processor.compute_changes(current_state.copy(), updates_standard, system_date=system_date)

        # Results should be identical
        compare_cols = ['id', 'price', 'quantity', 'effective_from', 'effective_to']
        pd.testing.assert_frame_equal(
            insert1[compare_cols].sort_values(by=['id', 'effective_from']).reset_index(drop=True),
            insert2[compare_cols].sort_values(by=['id', 'effective_from']).reset_index(drop=True)
        )
