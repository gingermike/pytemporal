import unittest
import pandas as pd
import numpy as np
from datetime import datetime, date
from bitemporal_processor import BitemporalTimeseriesProcessor, POSTGRES_INFINITY

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
            'effective_to': [POSTGRES_INFINITY],
            'as_of_from': pd.to_datetime(['2024-01-01']),
            'as_of_to': [POSTGRES_INFINITY]
        })
        
        updates = pd.DataFrame({
            'id': [1],
            'category': ['A'],
            'value1': [150],
            'value2': [250],
            'effective_from': pd.to_datetime(['2024-06-01']),
            'effective_to': pd.to_datetime(['2024-08-31']),
            'as_of_from': pd.to_datetime(['2024-07-21']),
            'as_of_to': [POSTGRES_INFINITY]
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
        # Should be infinity
        self.assertTrue(insert_sorted.iloc[2]['effective_to'] >= pd.Timestamp('9999-01-01'))
        self.assertEqual(insert_sorted.iloc[2]['value1'], 100)
    
    def test_postgres_infinity_handling(self):
        """Test proper handling of PostgreSQL infinity dates."""
        current = pd.DataFrame({
            'id': [1],
            'category': ['A'],
            'value1': [100],
            'value2': [200],
            'effective_from': pd.to_datetime(['2024-01-01']),
            'effective_to': [POSTGRES_INFINITY],
            'as_of_from': pd.to_datetime(['2024-01-01']),
            'as_of_to': [POSTGRES_INFINITY]
        })
        
        updates = pd.DataFrame({
            'id': [1],
            'category': ['A'],
            'value1': [150],
            'value2': [250],
            'effective_from': pd.to_datetime(['2024-06-01']),
            'effective_to': [POSTGRES_INFINITY],
            'as_of_from': pd.to_datetime(['2024-07-21']),
            'as_of_to': [POSTGRES_INFINITY]
        })
        
        to_expire, to_insert = self.processor.compute_changes(
            current, updates, '2024-07-21'
        )
        
        # Check that infinity dates are preserved
        for _, row in to_insert.iterrows():
            if row['effective_to'] >= pd.Timestamp('9999-01-01'):
                self.assertEqual(row['effective_to'], POSTGRES_INFINITY)
            if row['as_of_to'] >= pd.Timestamp('9999-01-01'):
                self.assertEqual(row['as_of_to'], POSTGRES_INFINITY)
    
    def test_full_state_mode(self):
        """Test full state mode where missing IDs are expired."""
        current = pd.DataFrame({
            'id': [1, 2, 3],
            'category': ['A', 'B', 'C'],
            'value1': [100, 200, 300],
            'value2': [1000, 2000, 3000],
            'effective_from': pd.to_datetime(['2024-01-01', '2024-01-01', '2024-01-01']),
            'effective_to': [POSTGRES_INFINITY, POSTGRES_INFINITY, POSTGRES_INFINITY],
            'as_of_from': pd.to_datetime(['2024-01-01', '2024-01-01', '2024-01-01']),
            'as_of_to': [POSTGRES_INFINITY, POSTGRES_INFINITY, POSTGRES_INFINITY]
        })
        
        # Full state update only includes IDs 1 and 3 (ID 2 is missing)
        updates = pd.DataFrame({
            'id': [1, 3],
            'category': ['A', 'C'],
            'value1': [150, 350],
            'value2': [1500, 3500],
            'effective_from': pd.to_datetime(['2024-01-01', '2024-01-01']),
            'effective_to': [POSTGRES_INFINITY, POSTGRES_INFINITY],
            'as_of_from': pd.to_datetime(['2024-07-21', '2024-07-21']),
            'as_of_to': [POSTGRES_INFINITY, POSTGRES_INFINITY]
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
            'effective_to': [POSTGRES_INFINITY, POSTGRES_INFINITY],
            'as_of_from': pd.to_datetime(['2024-01-01', '2024-01-01']),
            'as_of_to': [POSTGRES_INFINITY, POSTGRES_INFINITY]
        })
        
        # Update only ID 1
        updates = pd.DataFrame({
            'id': [1],
            'category': ['A'],
            'value1': [150],
            'value2': [1500],
            'effective_from': pd.to_datetime(['2024-01-01']),
            'effective_to': [POSTGRES_INFINITY],
            'as_of_from': pd.to_datetime(['2024-07-21']),
            'as_of_to': [POSTGRES_INFINITY]
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
            'effective_to': [POSTGRES_INFINITY],
            'as_of_from': pd.to_datetime(['2024-01-01']),
            'as_of_to': [POSTGRES_INFINITY]
        })
        
        updates = pd.DataFrame({
            'id': [1],
            'category': ['A'],
            'value1': [150],
            'value2': [250],
            'effective_from': pd.to_datetime(['2024-06-01']),
            'effective_to': [POSTGRES_INFINITY],
            'as_of_from': pd.to_datetime(['2024-07-21']),
            'as_of_to': [POSTGRES_INFINITY]
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
            'effective_to': [POSTGRES_INFINITY, POSTGRES_INFINITY],
            'as_of_from': pd.to_datetime(['2024-01-01', '2024-01-01']),
            'as_of_to': [pd.Timestamp('2024-06-01'), POSTGRES_INFINITY]  # First record already expired
        })
        
        updates = pd.DataFrame({
            'id': [1],
            'category': ['A'],
            'value1': [300],
            'value2': [3000],
            'effective_from': pd.to_datetime(['2024-03-01']),
            'effective_to': pd.to_datetime(['2024-09-01']),
            'as_of_from': pd.to_datetime(['2024-07-21']),
            'as_of_to': [POSTGRES_INFINITY]
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
            'effective_to': [pd.Timestamp('2024-07-01'), POSTGRES_INFINITY,
                           pd.Timestamp('2024-07-01'), POSTGRES_INFINITY,
                           POSTGRES_INFINITY],
            'as_of_from': pd.to_datetime(['2024-01-01'] * 5),
            'as_of_to': [POSTGRES_INFINITY] * 5
        })
        
        # Full state with only IDs 1 and 2, with modifications
        updates = pd.DataFrame({
            'id': [1, 2],
            'category': ['A', 'B'],
            'value1': [120, 220],
            'value2': [1200, 2200],
            'effective_from': pd.to_datetime(['2024-01-01', '2024-01-01']),
            'effective_to': [POSTGRES_INFINITY, POSTGRES_INFINITY],
            'as_of_from': pd.to_datetime(['2024-07-21', '2024-07-21']),
            'as_of_to': [POSTGRES_INFINITY, POSTGRES_INFINITY]
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


if __name__ == '__main__':
    unittest.main(verbose=2)
        
    def test_simple_update_slice(self):
        """Test update that slices through middle of existing record."""
        current = pd.DataFrame({
            'id': [1],
            'category': ['A'],
            'value1': [100],
            'value2': [200],
            'effective_from': pd.to_datetime(['2024-01-01']),
            'effective_to': pd.to_datetime(['2024-12-31']),
            'as_of_from': pd.to_datetime(['2024-01-01']),
            'as_of_to': [None]
        })
        
        updates = pd.DataFrame({
            'id': [1],
            'category': ['A'],
            'value1': [150],
            'value2': [250],
            'effective_from': pd.to_datetime(['2024-06-01']),
            'effective_to': pd.to_datetime(['2024-08-31']),
            'as_of_from': pd.to_datetime(['2024-07-21']),
            'as_of_to': [None]
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
        self.assertEqual(insert_sorted.iloc[2]['effective_to'], pd.Timestamp('2024-12-31'))
        self.assertEqual(insert_sorted.iloc[2]['value1'], 100)
    
    def test_exact_overlap(self):
        """Test update with exact same dates but different values."""
        current = pd.DataFrame({
            'id': [1],
            'category': ['A'],
            'value1': [100],
            'value2': [200],
            'effective_from': pd.to_datetime(['2024-01-01']),
            'effective_to': pd.to_datetime(['2024-12-31']),
            'as_of_from': pd.to_datetime(['2024-01-01']),
            'as_of_to': [None]
        })
        
        updates = pd.DataFrame({
            'id': [1],
            'category': ['A'],
            'value1': [150],  # Different value
            'value2': [200],
            'effective_from': pd.to_datetime(['2024-01-01']),
            'effective_to': pd.to_datetime(['2024-12-31']),
            'as_of_from': pd.to_datetime(['2024-07-21']),
            'as_of_to': [None]
        })
        
        to_expire, to_insert = self.processor.compute_changes(
            current, updates, '2024-07-21'
        )
        
        # Should expire the original
        self.assertEqual(len(to_expire), 1)
        
        # Should insert only 1 row (the update)
        self.assertEqual(len(to_insert), 1)
        self.assertEqual(to_insert.iloc[0]['value1'], 150)
    
    def test_pure_insert(self):
        """Test update for ID that doesn't exist in current state."""
        current = pd.DataFrame({
            'id': [1],
            'category': ['A'],
            'value1': [100],
            'value2': [200],
            'effective_from': pd.to_datetime(['2024-01-01']),
            'effective_to': pd.to_datetime(['2024-12-31']),
            'as_of_from': pd.to_datetime(['2024-01-01']),
            'as_of_to': [None]
        })
        
        updates = pd.DataFrame({
            'id': [2],  # New ID
            'category': ['B'],
            'value1': [300],
            'value2': [400],
            'effective_from': pd.to_datetime(['2024-01-01']),
            'effective_to': pd.to_datetime(['2024-12-31']),
            'as_of_from': pd.to_datetime(['2024-07-21']),
            'as_of_to': [None]
        })
        
        to_expire, to_insert = self.processor.compute_changes(
            current, updates, '2024-07-21'
        )
        
        # Should not expire anything
        self.assertEqual(len(to_expire), 0)
        
        # Should insert 1 new row
        self.assertEqual(len(to_insert), 1)
        self.assertEqual(to_insert.iloc[0]['id'], 2)
    
    def test_multiple_overlapping_records(self):
        """Test update that overlaps multiple existing records."""
        current = pd.DataFrame({
            'id': [1, 1, 1],
            'category': ['A', 'A', 'A'],
            'value1': [100, 200, 300],
            'value2': [1000, 2000, 3000],
            'effective_from': pd.to_datetime(['2024-01-01', '2024-04-01', '2024-08-01']),
            'effective_to': pd.to_datetime(['2024-04-01', '2024-08-01', '2024-12-31']),
            'as_of_from': pd.to_datetime(['2024-01-01', '2024-01-01', '2024-01-01']),
            'as_of_to': [None, None, None]
        })
        
        updates = pd.DataFrame({
            'id': [1],
            'category': ['A'],
            'value1': [999],
            'value2': [9999],
            'effective_from': pd.to_datetime(['2024-03-01']),
            'effective_to': pd.to_datetime(['2024-09-01']),
            'as_of_from': pd.to_datetime(['2024-07-21']),
            'as_of_to': [None]
        })
        
        to_expire, to_insert = self.processor.compute_changes(
            current, updates, '2024-07-21'
        )
        
        # Should expire all 3 original rows
        self.assertEqual(len(to_expire), 3)
        
        # Should create segments for each interaction
        # Expected: Jan-Mar(100), Mar-Apr(999), Apr-Aug(999), Aug-Sep(999), Sep-Dec(300)
        self.assertEqual(len(to_insert), 5)
    
    def test_no_changes_needed(self):
        """Test update with same values - no changes needed."""
        current = pd.DataFrame({
            'id': [1],
            'category': ['A'],
            'value1': [100],
            'value2': [200],
            'effective_from': pd.to_datetime(['2024-01-01']),
            'effective_to': pd.to_datetime(['2024-12-31']),
            'as_of_from': pd.to_datetime(['2024-01-01']),
            'as_of_to': [None]
        })
        
        updates = pd.DataFrame({
            'id': [1],
            'category': ['A'],
            'value1': [100],  # Same values
            'value2': [200],
            'effective_from': pd.to_datetime(['2024-01-01']),
            'effective_to': pd.to_datetime(['2024-12-31']),
            'as_of_from': pd.to_datetime(['2024-07-21']),
            'as_of_to': [None]
        })
        
        to_expire, to_insert = self.processor.compute_changes(
            current, updates, '2024-07-21'
        )
        
        # Should not make any changes
        self.assertEqual(len(to_expire), 0)
        self.assertEqual(len(to_insert), 0)
    
    def test_gap_handling(self):
        """Test that gaps in timeseries are preserved."""
        current = pd.DataFrame({
            'id': [1, 1],
            'category': ['A', 'A'],
            'value1': [100, 300],
            'value2': [1000, 3000],
            'effective_from': pd.to_datetime(['2024-01-01', '2024-08-01']),
            'effective_to': pd.to_datetime(['2024-03-01', '2024-12-31']),
            'as_of_from': pd.to_datetime(['2024-01-01', '2024-01-01']),
            'as_of_to': [None, None]
        })
        
        # Update in the gap
        updates = pd.DataFrame({
            'id': [1],
            'category': ['A'],
            'value1': [200],
            'value2': [2000],
            'effective_from': pd.to_datetime(['2024-04-01']),
            'effective_to': pd.to_datetime(['2024-06-01']),
            'as_of_from': pd.to_datetime(['2024-07-21']),
            'as_of_to': [None]
        })
        
        to_expire, to_insert = self.processor.compute_changes(
            current, updates, '2024-07-21'
        )
        
        # Should not expire anything (update is in gap)
        self.assertEqual(len(to_expire), 0)
        
        # Should insert just the new record
        self.assertEqual(len(to_insert), 1)
        self.assertEqual(to_insert.iloc[0]['effective_from'], pd.Timestamp('2024-04-01'))
        self.assertEqual(to_insert.iloc[0]['effective_to'], pd.Timestamp('2024-06-01'))
    
    def test_extension_beyond_range(self):
        """Test update that extends beyond current effective range."""
        current = pd.DataFrame({
            'id': [1],
            'category': ['A'],
            'value1': [100],
            'value2': [200],
            'effective_from': pd.to_datetime(['2024-01-01']),
            'effective_to': pd.to_datetime(['2024-06-01']),
            'as_of_from': pd.to_datetime(['2024-01-01']),
            'as_of_to': [None]
        })
        
        updates = pd.DataFrame({
            'id': [1],
            'category': ['A'],
            'value1': [150],
            'value2': [250],
            'effective_from': pd.to_datetime(['2024-05-01']),
            'effective_to': pd.to_datetime(['2024-12-31']),  # Extends beyond
            'as_of_from': pd.to_datetime(['2024-07-21']),
            'as_of_to': [None]
        })
        
        to_expire, to_insert = self.processor.compute_changes(
            current, updates, '2024-07-21'
        )
        
        # Should expire the original
        self.assertEqual(len(to_expire), 1)
        
        # Should insert: before segment (Jan-May) and update segment (May-Dec)
        self.assertEqual(len(to_insert), 2)
        
        insert_sorted = to_insert.sort_values('effective_from').reset_index(drop=True)
        self.assertEqual(insert_sorted.iloc[0]['effective_to'], pd.Timestamp('2024-05-01'))
        self.assertEqual(insert_sorted.iloc[1]['effective_from'], pd.Timestamp('2024-05-01'))
        self.assertEqual(insert_sorted.iloc[1]['effective_to'], pd.Timestamp('2024-12-31'))
    
    def test_performance_large_dataset(self):
        """Test performance with large dataset."""
        # Generate large dataset
        n_records = 100000
        n_updates = 10000
        
        # Create current state
        np.random.seed(42)
        ids = np.repeat(range(10000), 10)  # 10 records per ID
        categories = np.random.choice(['A', 'B', 'C', 'D'], n_records)
        
        dates = pd.date_range('2024-01-01', '2024-12-31', periods=11)
        current = pd.DataFrame({
            'id': ids,
            'category': categories,
            'value1': np.random.randint(0, 1000, n_records),
            'value2': np.random.randint(0, 1000, n_records),
            'effective_from': np.tile(dates[:-1], 10000),
            'effective_to': np.tile(dates[1:], 10000),
            'as_of_from': pd.Timestamp('2024-01-01'),
            'as_of_to': None
        })
        
        # Create updates
        update_ids = np.random.choice(range(10000), n_updates)
        updates = pd.DataFrame({
            'id': update_ids,
            'category': np.random.choice(['A', 'B', 'C', 'D'], n_updates),
            'value1': np.random.randint(0, 1000, n_updates),
            'value2': np.random.randint(0, 1000, n_updates),
            'effective_from': pd.to_datetime(
                np.random.choice(pd.date_range('2024-01-01', '2024-11-01', periods=100), n_updates)
            ),
            'effective_to': pd.to_datetime(
                np.random.choice(pd.date_range('2024-02-01', '2024-12-31', periods=100), n_updates)
            ),
            'as_of_from': pd.Timestamp('2024-07-21'),
            'as_of_to': None
        })
        
        # Ensure effective_to > effective_from
        mask = updates['effective_to'] <= updates['effective_from']
        updates.loc[mask, 'effective_to'] = updates.loc[mask, 'effective_from'] + pd.Timedelta(days=30)
        
        # Time the operation
        import time
        start_time = time.time()
        
        to_expire, to_insert = self.processor.compute_changes(
            current, updates, '2024-07-21'
        )
        
        end_time = time.time()
        elapsed = end_time - start_time
        
        print(f"\nPerformance test results:")
        print(f"- Processed {n_records} current records and {n_updates} updates")
        print(f"- Time elapsed: {elapsed:.2f} seconds")
        print(f"- Rows to expire: {len(to_expire)}")
        print(f"- Rows to insert: {len(to_insert)}")
        print(f"- Throughput: {(n_records + n_updates) / elapsed:.0f} records/second")
        
        # Should complete within reasonable time
        self.assertLess(elapsed, 5.0, "Operation took too long")
    
    def test_multiple_id_columns(self):
        """Test with multiple identifying columns."""
        processor = BitemporalTimeseriesProcessor(
            id_columns=['instrument', 'exchange', 'currency'],
            value_columns=['price', 'volume']
        )
        
        current = pd.DataFrame({
            'instrument': ['AAPL', 'AAPL', 'GOOGL'],
            'exchange': ['NYSE', 'NASDAQ', 'NASDAQ'],
            'currency': ['USD', 'USD', 'USD'],
            'price': [150.0, 151.0, 2800.0],
            'volume': [1000000, 900000, 500000],
            'effective_from': pd.to_datetime(['2024-01-01', '2024-01-01', '2024-01-01']),
            'effective_to': pd.to_datetime(['2024-12-31', '2024-12-31', '2024-12-31']),
            'as_of_from': pd.to_datetime(['2024-01-01', '2024-01-01', '2024-01-01']),
            'as_of_to': [None, None, None]
        })
        
        updates = pd.DataFrame({
            'instrument': ['AAPL'],
            'exchange': ['NYSE'],  # Only updates NYSE, not NASDAQ
            'currency': ['USD'],
            'price': [155.0],
            'volume': [1200000],
            'effective_from': pd.to_datetime(['2024-06-01']),
            'effective_to': pd.to_datetime(['2024-09-01']),
            'as_of_from': pd.to_datetime(['2024-07-21']),
            'as_of_to': [None]
        })
        
        to_expire, to_insert = processor.compute_changes(
            current, updates, '2024-07-21'
        )
        
        # Should only expire the NYSE record
        self.assertEqual(len(to_expire), 1)
        self.assertEqual(to_expire.iloc[0]['exchange'], 'NYSE')
        
        # NASDAQ record should remain untouched
        self.assertNotIn('NASDAQ', to_expire['exchange'].values)


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


if __name__ == '__main__':
    unittest.main(verbose=2)