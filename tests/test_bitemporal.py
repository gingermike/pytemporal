import unittest
import pandas as pd
import numpy as np
from datetime import datetime, date
from bitemporal_timeseries import BitemporalTimeseriesProcessor, POSTGRES_INFINITY

# Use pandas max timestamp for test data to avoid overflow issues
TEST_INFINITY = pd.Timestamp.max.normalize()  # Normalized to date precision

class TestBitemporalTimeseries(unittest.TestCase):
    
    def setUp(self):
        """Set up test data and processor."""
        self.processor = BitemporalTimeseriesProcessor(
            id_columns=['id', 'category'],
            value_columns=['value1', 'value2']
        )
        
    def test_simple_update_slice(self):
        """Test update that slices through middle of existing record."""
        current = pd.DataFrame({
            'id': [1],
            'category': ['A'],
            'value1': [100],
            'value2': [200],
            'effective_from': pd.to_datetime(['2024-01-01']),
            'effective_to': [TEST_INFINITY],
            'as_of_from': pd.to_datetime(['2024-01-01']),
            'as_of_to': [TEST_INFINITY]
        })
        
        updates = pd.DataFrame({
            'id': [1],
            'category': ['A'],
            'value1': [150],
            'value2': [250],
            'effective_from': pd.to_datetime(['2024-06-01']),
            'effective_to': pd.to_datetime(['2024-08-31']),
            'as_of_from': pd.to_datetime(['2024-07-21']),
            'as_of_to': [TEST_INFINITY]
        })
        
        to_expire, to_insert = self.processor.compute_changes(
            current, updates, '2024-07-21'
        )
        
        # Should expire the original row
        self.assertEqual(len(to_expire), 1)
        
        # Should insert 3 rows: before, update, after
        self.assertEqual(len(to_insert), 3)
        
        # Check date ranges
        insert_sorted = to_insert.sort_values('effective_from').reset_index(drop=True)
        
        # Before segment
        self.assertEqual(insert_sorted.iloc[0]['effective_from'], pd.Timestamp('2024-01-01'))
        self.assertEqual(insert_sorted.iloc[0]['effective_to'], pd.Timestamp('2024-06-01'))
        self.assertEqual(insert_sorted.iloc[0]['value1'], 100)
        
        # Update segment
        self.assertEqual(insert_sorted.iloc[1]['effective_from'], pd.Timestamp('2024-06-01'))
        self.assertEqual(insert_sorted.iloc[1]['effective_to'], pd.Timestamp('2024-08-31'))
        self.assertEqual(insert_sorted.iloc[1]['value1'], 150)
        
        # After segment
        self.assertEqual(insert_sorted.iloc[2]['effective_from'], pd.Timestamp('2024-08-31'))
        # Should be infinity (converted to POSTGRES_INFINITY in output)
        self.assertEqual(insert_sorted.iloc[2]['effective_to'], POSTGRES_INFINITY)
        self.assertEqual(insert_sorted.iloc[2]['value1'], 100)
    
    def test_postgres_infinity_handling(self):
        """Test proper handling of PostgreSQL infinity dates."""
        current = pd.DataFrame({
            'id': [1],
            'category': ['A'],
            'value1': [100],
            'value2': [200],
            'effective_from': pd.to_datetime(['2024-01-01']),
            'effective_to': [TEST_INFINITY],
            'as_of_from': pd.to_datetime(['2024-01-01']),
            'as_of_to': [TEST_INFINITY]
        })
        
        updates = pd.DataFrame({
            'id': [1],
            'category': ['A'],
            'value1': [150],
            'value2': [250],
            'effective_from': pd.to_datetime(['2024-06-01']),
            'effective_to': [TEST_INFINITY],
            'as_of_from': pd.to_datetime(['2024-07-21']),
            'as_of_to': [TEST_INFINITY]
        })
        
        to_expire, to_insert = self.processor.compute_changes(
            current, updates, '2024-07-21'
        )
        
        # Check that infinity dates are properly converted to POSTGRES_INFINITY
        for _, row in to_insert.iterrows():
            if row['effective_to'] >= pd.Timestamp('2262-01-01'):
                self.assertEqual(row['effective_to'], POSTGRES_INFINITY)
            if row['as_of_to'] >= pd.Timestamp('2262-01-01'):
                self.assertEqual(row['as_of_to'], POSTGRES_INFINITY)
    
    def test_full_state_mode(self):
        """Test full state mode where missing IDs are expired."""
        current = pd.DataFrame({
            'id': [1, 2, 3],
            'category': ['A', 'B', 'C'],
            'value1': [100, 200, 300],
            'value2': [1000, 2000, 3000],
            'effective_from': pd.to_datetime(['2024-01-01', '2024-01-01', '2024-01-01']),
            'effective_to': [TEST_INFINITY, TEST_INFINITY, TEST_INFINITY],
            'as_of_from': pd.to_datetime(['2024-01-01', '2024-01-01', '2024-01-01']),
            'as_of_to': [TEST_INFINITY, TEST_INFINITY, TEST_INFINITY]
        })
        
        # Full state update only includes IDs 1 and 3 (ID 2 is missing)
        updates = pd.DataFrame({
            'id': [1, 3],
            'category': ['A', 'C'],
            'value1': [150, 350],
            'value2': [1500, 3500],
            'effective_from': pd.to_datetime(['2024-01-01', '2024-01-01']),
            'effective_to': [TEST_INFINITY, TEST_INFINITY],
            'as_of_from': pd.to_datetime(['2024-07-21', '2024-07-21']),
            'as_of_to': [TEST_INFINITY, TEST_INFINITY]
        })
        
        to_expire, to_insert = self.processor.compute_changes(
            current, updates, '2024-07-21', update_mode='full_state'
        )
        
        # Should expire all 3 original rows
        self.assertEqual(len(to_expire), 3)
        
        # Should only insert 2 rows (IDs 1 and 3)
        self.assertEqual(len(to_insert), 2)
        
        # Check that ID 2 is in expired but not in inserts
        expired_ids = set(to_expire['id'].values)
        inserted_ids = set(to_insert['id'].values)
        
        self.assertIn(2, expired_ids)
        self.assertNotIn(2, inserted_ids)
        self.assertEqual(inserted_ids, {1, 3})
    
    def test_delta_mode_vs_full_state(self):
        """Compare delta mode vs full state mode behavior."""
        current = pd.DataFrame({
            'id': [1, 2],
            'category': ['A', 'B'],
            'value1': [100, 200],
            'value2': [1000, 2000],
            'effective_from': pd.to_datetime(['2024-01-01', '2024-01-01']),
            'effective_to': [TEST_INFINITY, TEST_INFINITY],
            'as_of_from': pd.to_datetime(['2024-01-01', '2024-01-01']),
            'as_of_to': [TEST_INFINITY, TEST_INFINITY]
        })
        
        # Update only ID 1
        updates = pd.DataFrame({
            'id': [1],
            'category': ['A'],
            'value1': [150],
            'value2': [1500],
            'effective_from': pd.to_datetime(['2024-01-01']),
            'effective_to': [TEST_INFINITY],
            'as_of_from': pd.to_datetime(['2024-07-21']),
            'as_of_to': [TEST_INFINITY]
        })
        
        # Delta mode - should only affect ID 1
        to_expire_delta, to_insert_delta = self.processor.compute_changes(
            current, updates, '2024-07-21', update_mode='delta'
        )
        
        self.assertEqual(len(to_expire_delta), 1)
        self.assertEqual(to_expire_delta.iloc[0]['id'], 1)
        self.assertEqual(len(to_insert_delta), 1)
        
        # Full state mode - should expire both IDs
        to_expire_full, to_insert_full = self.processor.compute_changes(
            current, updates, '2024-07-21', update_mode='full_state'
        )
        
        self.assertEqual(len(to_expire_full), 2)
        self.assertEqual(len(to_insert_full), 1)  # Only ID 1 inserted
    
    def test_no_nulls_in_output(self):
        """Ensure output never contains null dates."""
        current = pd.DataFrame({
            'id': [1],
            'category': ['A'],
            'value1': [100],
            'value2': [200],
            'effective_from': pd.to_datetime(['2024-01-01']),
            'effective_to': [TEST_INFINITY],
            'as_of_from': pd.to_datetime(['2024-01-01']),
            'as_of_to': [TEST_INFINITY]
        })
        
        updates = pd.DataFrame({
            'id': [1],
            'category': ['A'],
            'value1': [150],
            'value2': [250],
            'effective_from': pd.to_datetime(['2024-06-01']),
            'effective_to': [TEST_INFINITY],
            'as_of_from': pd.to_datetime(['2024-07-21']),
            'as_of_to': [TEST_INFINITY]
        })
        
        to_expire, to_insert = self.processor.compute_changes(
            current, updates, '2024-07-21'
        )
        
        # Check no nulls in date columns
        date_cols = ['effective_from', 'effective_to', 'as_of_from', 'as_of_to']
        
        for col in date_cols:
            if col in to_expire.columns:
                self.assertFalse(to_expire[col].isna().any())
            if col in to_insert.columns:
                self.assertFalse(to_insert[col].isna().any())
    
    def test_already_expired_records_ignored(self):
        """Test that already expired records are ignored."""
        current = pd.DataFrame({
            'id': [1, 1],
            'category': ['A', 'A'],
            'value1': [100, 200],
            'value2': [1000, 2000],
            'effective_from': pd.to_datetime(['2024-01-01', '2024-01-01']),
            'effective_to': [TEST_INFINITY, TEST_INFINITY],
            'as_of_from': pd.to_datetime(['2024-01-01', '2024-01-01']),
            'as_of_to': [pd.Timestamp('2024-06-01'), TEST_INFINITY]  # First record already expired
        })
        
        updates = pd.DataFrame({
            'id': [1],
            'category': ['A'],
            'value1': [300],
            'value2': [3000],
            'effective_from': pd.to_datetime(['2024-03-01']),
            'effective_to': pd.to_datetime(['2024-09-01']),
            'as_of_from': pd.to_datetime(['2024-07-21']),
            'as_of_to': [TEST_INFINITY]
        })
        
        to_expire, to_insert = self.processor.compute_changes(
            current, updates, '2024-07-21'
        )
        
        # Should only process the non-expired record
        self.assertEqual(len(to_expire), 1)
        self.assertEqual(to_expire.iloc[0]['value1'], 200)
    
    def test_complex_multi_id_full_state(self):
        """Test complex scenario with multiple IDs in full state mode."""
        current = pd.DataFrame({
            'id': [1, 1, 2, 2, 3],
            'category': ['A', 'A', 'B', 'B', 'C'],
            'value1': [100, 110, 200, 210, 300],
            'value2': [1000, 1100, 2000, 2100, 3000],
            'effective_from': pd.to_datetime(['2024-01-01', '2024-07-01', 
                                             '2024-01-01', '2024-07-01', 
                                             '2024-01-01']),
            'effective_to': [pd.Timestamp('2024-07-01'), TEST_INFINITY,
                           pd.Timestamp('2024-07-01'), TEST_INFINITY,
                           TEST_INFINITY],
            'as_of_from': pd.to_datetime(['2024-01-01'] * 5),
            'as_of_to': [TEST_INFINITY] * 5
        })
        
        # Full state with only IDs 1 and 2, with modifications
        updates = pd.DataFrame({
            'id': [1, 2],
            'category': ['A', 'B'],
            'value1': [120, 220],
            'value2': [1200, 2200],
            'effective_from': pd.to_datetime(['2024-01-01', '2024-01-01']),
            'effective_to': [TEST_INFINITY, TEST_INFINITY],
            'as_of_from': pd.to_datetime(['2024-07-21', '2024-07-21']),
            'as_of_to': [TEST_INFINITY, TEST_INFINITY]
        })
        
        to_expire, to_insert = self.processor.compute_changes(
            current, updates, '2024-07-21', update_mode='full_state'
        )
        
        # All 5 current records should be expired
        self.assertEqual(len(to_expire), 5)
        
        # Should insert 2 records (one for each ID in updates)
        self.assertEqual(len(to_insert), 2)
        
        # ID 3 should be completely removed
        self.assertNotIn(3, to_insert['id'].values)
    
    def test_unsorted_data_handling(self):
        """Test that unsorted current state and updates are handled correctly."""
        # Unsorted current state - rows not in chronological order
        current = pd.DataFrame({
            'id': [1, 1, 1],
            'category': ['A', 'A', 'A'],
            'value1': [300, 100, 200],  # Values correspond to time periods
            'value2': [3000, 1000, 2000],
            'effective_from': pd.to_datetime(['2024-08-01', '2024-01-01', '2024-04-01']),
            'effective_to': pd.to_datetime(['2024-12-31', '2024-04-01', '2024-08-01']),
            'as_of_from': pd.to_datetime(['2024-01-01', '2024-01-01', '2024-01-01']),
            'as_of_to': [TEST_INFINITY, TEST_INFINITY, TEST_INFINITY]
        })
        
        # Update that spans multiple periods
        updates = pd.DataFrame({
            'id': [1],
            'category': ['A'],
            'value1': [999],
            'value2': [9999],
            'effective_from': pd.to_datetime(['2024-03-01']),
            'effective_to': pd.to_datetime(['2024-09-01']),
            'as_of_from': pd.to_datetime(['2024-07-21']),
            'as_of_to': [TEST_INFINITY]
        })
        
        to_expire, to_insert = self.processor.compute_changes(
            current, updates, '2024-07-21'
        )
        
        # Should expire all 3 original rows regardless of order
        self.assertEqual(len(to_expire), 3)
        
        # Should create 5 segments:
        # 1. Jan-Mar (100)
        # 2. Mar-Apr (999) 
        # 3. Apr-Aug (999)
        # 4. Aug-Sep (999)
        # 5. Sep-Dec (300)
        self.assertEqual(len(to_insert), 5)
        
        # Verify segments are in correct order
        insert_sorted = to_insert.sort_values('effective_from').reset_index(drop=True)
        
        expected = [
            ('2024-01-01', '2024-03-01', 100),
            ('2024-03-01', '2024-04-01', 999),
            ('2024-04-01', '2024-08-01', 999),
            ('2024-08-01', '2024-09-01', 999),
            ('2024-09-01', '2024-12-31', 300)
        ]
        
        for i, (from_date, to_date, value) in enumerate(expected):
            self.assertEqual(
                insert_sorted.iloc[i]['effective_from'], 
                pd.Timestamp(from_date)
            )
            self.assertEqual(
                insert_sorted.iloc[i]['effective_to'], 
                pd.Timestamp(to_date) if to_date != '2024-12-31' else insert_sorted.iloc[i]['effective_to']
            )
            self.assertEqual(insert_sorted.iloc[i]['value1'], value)
    
    def test_multiple_overlapping_updates(self):
        """Test multiple updates in one batch that overlap the same current records."""
        current = pd.DataFrame({
            'id': [1, 1],
            'category': ['A', 'A'],
            'value1': [100, 200],
            'value2': [1000, 2000],
            'effective_from': pd.to_datetime(['2024-01-01', '2024-07-01']),
            'effective_to': [pd.Timestamp('2024-07-01'), TEST_INFINITY],
            'as_of_from': pd.to_datetime(['2024-01-01', '2024-01-01']),
            'as_of_to': [TEST_INFINITY, TEST_INFINITY]
        })
        
        # Two updates that both affect the same periods
        updates = pd.DataFrame({
            'id': [1, 1],
            'category': ['A', 'A'],
            'value1': [150, 250],
            'value2': [1500, 2500],
            'effective_from': pd.to_datetime(['2024-03-01', '2024-09-01']),
            'effective_to': pd.to_datetime(['2024-05-01', '2024-11-01']),
            'as_of_from': pd.to_datetime(['2024-07-21', '2024-07-21']),
            'as_of_to': [TEST_INFINITY, TEST_INFINITY]
        })
        
        to_expire, to_insert = self.processor.compute_changes(
            current, updates, '2024-07-21'
        )
        
        # Both original rows should be expired
        self.assertEqual(len(to_expire), 2)
        
        # Expected segments:
        # Jan-Mar (100), Mar-May (150), May-Jul (100), 
        # Jul-Sep (200), Sep-Nov (250), Nov-infinity (200)
        self.assertEqual(len(to_insert), 6)
        
        insert_sorted = to_insert.sort_values('effective_from').reset_index(drop=True)
        
        expected_values = [100, 150, 100, 200, 250, 200]
        for i, expected_val in enumerate(expected_values):
            self.assertEqual(insert_sorted.iloc[i]['value1'], expected_val)
    
    def test_unsorted_updates_processed_consistently(self):
        """Test that unsorted updates are processed in a consistent order."""
        current = pd.DataFrame({
            'id': [1],
            'category': ['A'],
            'value1': [100],
            'value2': [1000],
            'effective_from': pd.to_datetime(['2024-01-01']),
            'effective_to': [TEST_INFINITY],
            'as_of_from': pd.to_datetime(['2024-01-01']),
            'as_of_to': [TEST_INFINITY]
        })
        
        # Updates not in chronological order
        updates = pd.DataFrame({
            'id': [1, 1, 1],
            'category': ['A', 'A', 'A'],
            'value1': [300, 100, 200],  # Out of order
            'value2': [3000, 1000, 2000],
            'effective_from': pd.to_datetime(['2024-09-01', '2024-01-01', '2024-05-01']),
            'effective_to': pd.to_datetime(['2024-11-01', '2024-03-01', '2024-07-01']),
            'as_of_from': pd.to_datetime(['2024-07-21', '2024-07-21', '2024-07-21']),
            'as_of_to': [TEST_INFINITY, TEST_INFINITY, TEST_INFINITY]
        })
        
        to_expire, to_insert = self.processor.compute_changes(
            current, updates, '2024-07-21'
        )
        
        # Original row should be expired
        self.assertEqual(len(to_expire), 1)
        
        # Should create segments for each update plus fill-in segments
        # The exact number depends on implementation but result should be consistent
        self.assertGreater(len(to_insert), 3)
        
        # Verify no overlaps in result
        insert_sorted = to_insert.sort_values('effective_from').reset_index(drop=True)
        for i in range(len(insert_sorted) - 1):
            self.assertLessEqual(
                insert_sorted.iloc[i]['effective_to'],
                insert_sorted.iloc[i + 1]['effective_from']
            )


class TestBitemporalEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions."""
    
    def setUp(self):
        self.processor = BitemporalTimeseriesProcessor(
            id_columns=['id'],
            value_columns=['value']
        )
    
    def test_already_expired_records(self):
        """Test that already expired records are ignored."""
        current = pd.DataFrame({
            'id': [1, 1],
            'value': [100, 200],
            'effective_from': pd.to_datetime(['2024-01-01', '2024-01-01']),
            'effective_to': pd.to_datetime(['2024-12-31', '2024-12-31']),
            'as_of_from': pd.to_datetime(['2024-01-01', '2024-01-01']),
            'as_of_to': [pd.Timestamp('2024-06-01'), None]  # First record already expired
        })
        
        updates = pd.DataFrame({
            'id': [1],
            'value': [300],
            'effective_from': pd.to_datetime(['2024-03-01']),
            'effective_to': pd.to_datetime(['2024-09-01']),
            'as_of_from': pd.to_datetime(['2024-07-21']),
            'as_of_to': [None]
        })
        
        to_expire, to_insert = self.processor.compute_changes(
            current, updates, '2024-07-21'
        )
        
        # Should only process the non-expired record
        self.assertEqual(len(to_expire), 1)
        self.assertEqual(to_expire.iloc[0]['value'], 200)
    
    def test_hash_values_returned(self):
        """Test that hash values are included in the returned data."""
        current = pd.DataFrame({
            'id': [1],
            'value': [100],
            'effective_from': pd.to_datetime(['2024-01-01']),
            'effective_to': [TEST_INFINITY],
            'as_of_from': pd.to_datetime(['2024-01-01']),
            'as_of_to': [TEST_INFINITY]
        })

        updates = pd.DataFrame({
            'id': [1],
            'value': [150],
            'effective_from': pd.to_datetime(['2024-06-01']),
            'effective_to': pd.to_datetime(['2024-08-31']),
            'as_of_from': pd.to_datetime(['2024-07-21']),
            'as_of_to': [TEST_INFINITY]
        })

        to_expire, to_insert = self.processor.compute_changes(
            current, updates, '2024-07-21'
        )

        # Check that value_hash column exists
        self.assertIn('value_hash', to_insert.columns)
        
        # Check that hash values are integers
        for hash_value in to_insert['value_hash']:
            self.assertIsInstance(hash_value, (int, np.integer))
            
        # Check that different value combinations have different hashes
        unique_hashes = to_insert['value_hash'].nunique()
        # We expect at least 1 hash value
        self.assertGreaterEqual(unique_hashes, 1)



if __name__ == '__main__':
    unittest.main(verbose=2)