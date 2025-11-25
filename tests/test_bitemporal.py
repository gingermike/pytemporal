from datetime import datetime
from typing import List, Tuple, Literal

from pandas._testing import assert_frame_equal
import pandas as pd

import pytest

from pytemporal import BitemporalTimeseriesProcessor, INFINITY_TIMESTAMP

from tests.scenarios.conflation import (
    conflation,
    conflation_three_segments,
    conflation_partial,
    conflation_non_consecutive,
    conflation_mixed_ids,
    conflation_unsorted_input,
    conflation_with_current_state,
    conflation_different_fields
)
from tests.scenarios.basic import overwrite, insert, unrelated_state, append_tail, append_tail_exact, append_head, \
    append_head_exact, intersect, no_change, full_state_basic, full_state_delete, _merge_consecutive_rows
from tests.scenarios.complex import overlay_two, overlay_multiple, multi_intersection_single_point, \
    multi_intersection_multiple_point, multi_field, extend_current_row, extend_update, no_change_with_intersection
from tests.scenarios.defaults import default_id_columns, default_value_columns, default_columns

scenarios = [
    #basic
    insert,
    overwrite,
    unrelated_state,
    append_tail,
    append_tail_exact,
    append_head,
    append_head_exact,
    intersect,
    no_change,
    full_state_basic,
    full_state_delete,
    _merge_consecutive_rows,

    #complex
    overlay_two,
    overlay_multiple,
    multi_intersection_single_point,
    multi_intersection_multiple_point,
    multi_field,
    extend_current_row,
    extend_update,
    no_change_with_intersection,

    #conflation
    conflation,
    conflation_three_segments,
    conflation_partial,
    conflation_non_consecutive,
    conflation_mixed_ids,
    conflation_unsorted_input,
    conflation_with_current_state,
    conflation_different_fields
]


@pytest.mark.parametrize(
    ("current_state", "updates", "expected", "update_mode", "scenario_id"),
    [scenario.data() + tuple([scenario.update_mode, scenario.id]) for scenario in scenarios],
    ids=[scenario.id for scenario in scenarios]
)
def test_update_scenarios(current_state: List,
                          updates: List,
                          expected: Tuple[List, List],
                          update_mode: Literal["delta", "full_state"],
                          scenario_id: str):

    # Assemble
    processor = BitemporalTimeseriesProcessor(
        id_columns=default_id_columns,
        value_columns=default_value_columns
    )

    current_state_df = pd.DataFrame(current_state, columns=default_columns)
    updates_df = pd.DataFrame(updates, columns=default_columns)

    # Enable conflation for all conflation scenarios
    conflate_inputs = scenario_id.startswith("conflation")

    # Act
    expire, insert = processor.compute_changes(
        pd.DataFrame(current_state_df, columns=default_columns),
        pd.DataFrame(updates_df, columns=default_columns),
        update_mode=update_mode,
        conflate_inputs=conflate_inputs
    )
    expire = expire.sort_values(by=default_id_columns + ["effective_from"]).reset_index(drop=True)
    insert = insert.sort_values(by=default_id_columns + ["effective_from"]).reset_index(drop=True)

    # Assert
    expected_expire, expected_insert = expected

    expected_expire_df = pd.DataFrame(expected_expire, columns=default_columns).sort_values(
        by=default_id_columns + ["effective_from"]).reset_index(drop=True)
    expected_insert_df = pd.DataFrame(expected_insert, columns=default_columns).sort_values(
        by=default_id_columns + ["effective_from"]).reset_index(drop=True)

    columns_no_as_of_to = list(default_columns)
    columns_no_as_of_to.remove("as_of_to")
    assert_frame_equal(expected_expire_df[columns_no_as_of_to], expire[columns_no_as_of_to],
                       check_dtype=False,
                       check_index_type=False)
    assert_frame_equal(expected_insert_df[columns_no_as_of_to], insert[columns_no_as_of_to],
                       check_dtype=False,
                       check_index_type=False)

    assert all([x == INFINITY_TIMESTAMP for x in insert["as_of_to"].to_list()])
    assert all([x > pd.Timestamp.now().normalize() for x in expire["as_of_to"].to_list()])


def test_bitemporal_head_slice():

    processor = BitemporalTimeseriesProcessor(
        id_columns=["id", "field"],
        value_columns=["mv", "price"]
    )

    current_state = [
            [
                1234, "test", 300, 400,
                    pd.to_datetime("2020-01-01"), pd.to_datetime("2021-01-01"),
                    pd.to_datetime("2025-01-01"), pd.Timestamp.max
            ],
            [
                1234, "fielda", 400, 500,
                    pd.to_datetime("2020-01-01"), pd.to_datetime("2021-01-01"),
                    pd.to_datetime("2025-01-01"), pd.Timestamp.max
            ],
        ]

    update_state = [
        [
            1234, "test", 400, 300,
                pd.to_datetime("2019-01-01"), pd.to_datetime("2020-06-01"),
                pd.to_datetime(datetime.now()), pd.Timestamp.max
        ]
    ]



    expire, insert = processor.compute_changes(
        pd.DataFrame(current_state, columns=default_columns),
        pd.DataFrame(update_state, columns=default_columns),
        update_mode="delta"
    )

    assert len(expire) == 1

    assert len(insert) == 2
    assert insert.loc[0]["effective_from"] == pd.to_datetime('2019-01-01')
    assert insert.loc[0]["effective_to"] == pd.to_datetime('2020-06-01')
    assert insert.loc[1]["effective_from"] == pd.to_datetime('2020-06-01')
    assert insert.loc[1]["effective_to"] == pd.to_datetime('2021-01-01')


def test_bitemporal_tail_slice():

    processor = BitemporalTimeseriesProcessor(
        id_columns=["id", "field"],
        value_columns=["mv", "price"]
    )

    current_state = [
            [
                1234, "test", 300, 400,
                    pd.to_datetime("2020-01-01"), pd.to_datetime("2021-01-01"),
                    pd.to_datetime("2025-01-01"), pd.Timestamp.max
            ],
            [
                1234, "fielda", 400, 500,
                    pd.to_datetime("2020-01-01"), pd.to_datetime("2021-01-01"),
                    pd.to_datetime("2025-01-01"), pd.Timestamp.max
            ],
        ]

    update_state = [
        [
            1234, "test", 400, 300,
                pd.to_datetime("2020-06-01"), pd.to_datetime("2022-01-01"),
                pd.to_datetime(datetime.now()), pd.Timestamp.max
        ]
    ]



    expire, insert = processor.compute_changes(
        pd.DataFrame(current_state, columns=default_columns),
        pd.DataFrame(update_state, columns=default_columns),
        update_mode="delta"
    )

    assert len(expire) == 1

    assert len(insert) == 2
    insert = insert.sort_values(by=["effective_from"])
    assert insert.loc[0]["effective_from"] == pd.to_datetime('2020-01-01')
    assert insert.loc[0]["effective_to"] == pd.to_datetime('2020-06-01')
    assert insert.loc[1]["effective_from"] == pd.to_datetime('2020-06-01')
    assert insert.loc[1]["effective_to"] == pd.to_datetime('2022-01-01')


def test_bitemporal_total_overwrite():

    processor = BitemporalTimeseriesProcessor(
        id_columns=["id", "field"],
        value_columns=["mv", "price"]
    )

    current_state = [
            [
                1234, "test", 300, 400,
                    pd.to_datetime("2020-01-01"), pd.to_datetime("2021-01-01"),
                    pd.to_datetime("2025-01-01"), pd.Timestamp.max
            ],
            [
                1234, "fielda", 400, 500,
                    pd.to_datetime("2020-01-01"), pd.to_datetime("2021-01-01"),
                    pd.to_datetime("2025-01-01"), pd.Timestamp.max
            ],
        ]

    update_state = [
        [
            1234, "test", 400, 300,
                pd.to_datetime("2019-01-01"), pd.to_datetime("2022-01-01"),
                pd.to_datetime(datetime.now()), pd.Timestamp.max
        ]
    ]

    expire, insert = processor.compute_changes(
        pd.DataFrame(current_state, columns=default_columns),
        pd.DataFrame(update_state, columns=default_columns),
        update_mode="delta"
    )

    assert len(expire) == 1

    assert len(insert) == 1


def test_bitemporal_two_updates():

    processor = BitemporalTimeseriesProcessor(
        id_columns=["id", "field"],
        value_columns=["mv", "price"]
    )

    current_state = [
            [
                1234, "test", 300, 400,
                    pd.to_datetime("2020-01-01"), pd.to_datetime("2021-01-01"),
                    pd.to_datetime("2025-01-01"), pd.Timestamp.max
            ],
            [
                1234, "fielda", 400, 500,
                    pd.to_datetime("2020-01-01"), pd.to_datetime("2021-01-01"),
                    pd.to_datetime("2025-01-01"), pd.Timestamp.max
            ],
        ]

    update_state = [
        [
            1234, "fielda", 400, 300,
                pd.to_datetime("2019-01-01"), pd.to_datetime("2020-03-01"),
                pd.to_datetime(datetime.now()), pd.Timestamp.max
        ],
        [
            1234, "fielda", 400, 300,
            pd.to_datetime("2020-06-01"), pd.to_datetime("2021-03-01"),
            pd.to_datetime(datetime.now()), pd.Timestamp.max
        ]
    ]

    expire, insert = processor.compute_changes(
        pd.DataFrame(current_state, columns=default_columns),
        pd.DataFrame(update_state, columns=default_columns),
        update_mode="delta"
    )

    assert len(expire) == 1

    assert len(insert) == 3
    assert insert.loc[0]["effective_from"] == pd.to_datetime('2019-01-01')
    assert insert.loc[0]["effective_to"] == pd.to_datetime('2020-03-01')
    assert insert.loc[1]["effective_from"] == pd.to_datetime('2020-03-01')
    assert insert.loc[1]["effective_to"] == pd.to_datetime('2020-06-01')
    assert insert.loc[2]["effective_from"] == pd.to_datetime('2020-06-01')
    assert insert.loc[2]["effective_to"] == pd.to_datetime('2021-03-01')


def test_bitemporal_update_multiple_current():
    processor = BitemporalTimeseriesProcessor(
        id_columns=["id", "field"],
        value_columns=["mv", "price"]
    )

    current_state = [
        [
            1234, "test", 300, 400,
            pd.to_datetime("2020-01-01"), pd.to_datetime("2021-01-01"),
            pd.to_datetime("2025-01-01"), pd.Timestamp.max
        ],
        [
            1234, "test", 500, 600,
            pd.to_datetime("2021-01-01"), pd.to_datetime("2022-01-01"),
            pd.to_datetime("2025-01-01"), pd.Timestamp.max
        ],
        [
            1234, "test", 700, 800,
            pd.to_datetime("2022-01-01"), pd.to_datetime("2023-01-01"),
            pd.to_datetime("2025-01-01"), pd.Timestamp.max
        ],
        [
            1234, "fielda", 400, 500,
            pd.to_datetime("2020-01-01"), pd.to_datetime("2021-01-01"),
            pd.to_datetime("2025-01-01"), pd.Timestamp.max
        ],
    ]

    update_state = [
        [
            1234, "test", 200, 300,
            pd.to_datetime("2020-10-01"), pd.to_datetime("2022-03-01"),
            pd.to_datetime(datetime.now()), pd.Timestamp.max
        ]
    ]

    expire, insert = processor.compute_changes(
        pd.DataFrame(current_state, columns=default_columns),
        pd.DataFrame(update_state, columns=default_columns),
        update_mode="delta"
    )

    assert len(expire) == 3

    assert len(insert) == 3
    assert insert.loc[0]["effective_from"] == pd.to_datetime('2020-01-01')
    assert insert.loc[0]["effective_to"] == pd.to_datetime('2020-10-01')
    assert insert.loc[1]["effective_from"] == pd.to_datetime('2020-10-01')
    assert insert.loc[1]["effective_to"] == pd.to_datetime('2022-03-01')
    assert insert.loc[2]["effective_from"] == pd.to_datetime('2022-03-01')
    assert insert.loc[2]["effective_to"] == pd.to_datetime('2023-01-01')


def test_backfill_skips_future_records():
    """
    Test: Backfill scenario - records with effective_from > system_date should NOT be tombstoned.

    This tests the fix for the "invalid range" bug where tombstoning records during backfill
    created effective_from > effective_to ranges, which violate database constraints.

    Scenario:
    - Current state has a record starting on 2024-01-02
    - Backfill with system_date=2024-01-01 (earlier than existing record)
    - The existing record should NOT be tombstoned (would create invalid range)
    """
    processor = BitemporalTimeseriesProcessor(
        id_columns=["parent_id", "child_id"],
        value_columns=["path_length"]
    )

    # Current state: Record exists for Day 2
    current_state = pd.DataFrame([
        {
            'parent_id': 2, 'child_id': 10, 'path_length': 1,
            'effective_from': pd.Timestamp('2024-01-02'),  # Future date from backfill perspective
            'effective_to': INFINITY_TIMESTAMP,
            'as_of_from': pd.Timestamp('2024-01-02'),
            'as_of_to': INFINITY_TIMESTAMP,
            'value_hash': 'existing_record'
        }
    ])

    # Incoming: Backfill Day 1 data (doesn't include the Day 2 record)
    incoming_data = pd.DataFrame([
        {
            'parent_id': 1, 'child_id': 2, 'path_length': 1,
            'effective_from': pd.Timestamp('2024-01-01'),
            'effective_to': pd.Timestamp('2024-01-02'),
            'as_of_from': pd.Timestamp('2024-01-01'),
            'as_of_to': INFINITY_TIMESTAMP,
            'value_hash': 'backfill_record'
        }
    ])

    # Backfill date is BEFORE the existing record's effective_from
    expiries, inserts = processor.compute_changes(
        current_state, incoming_data,
        system_date='2024-01-01',  # Backfill date - earlier than existing record
        update_mode='full_state'   # This mode expires missing records
    )

    # The record with parent_id=2 should NOT be expired because:
    # - Its effective_from (2024-01-02) > system_date (2024-01-01)
    # - Tombstoning it would create an invalid range: effective_from > effective_to
    assert len(expiries) == 0, "No records should be expired when their effective_from > system_date"

    # Only the backfill record should be inserted (no tombstone for the "future" record)
    assert len(inserts) == 1, "Only the backfill record should be inserted"
    assert inserts.iloc[0]['parent_id'] == 1, "Inserted record should be the backfill record"

    # CRITICAL: Verify no inserted record has effective_from > effective_to
    for i, row in inserts.iterrows():
        eff_from = row['effective_from']
        eff_to = row['effective_to']
        # Skip infinity check (infinity is always valid)
        if eff_to != INFINITY_TIMESTAMP:
            assert eff_from <= eff_to, \
                f"Invalid range detected: effective_from ({eff_from}) > effective_to ({eff_to})"


def test_backfill_mixed_tombstone_eligibility():
    """
    Test: Backfill with mixed records - some valid to tombstone, some not.

    This tests that the filter correctly handles a mix of:
    - Records that CAN be tombstoned (effective_from <= system_date)
    - Records that should be SKIPPED (effective_from > system_date)
    """
    processor = BitemporalTimeseriesProcessor(
        id_columns=["id", "field"],
        value_columns=["mv", "price"]
    )

    # Current state: Mix of records with different effective_from dates
    current_state = pd.DataFrame([
        # Record starting BEFORE backfill date - CAN be tombstoned
        [1, "test", 10, 20, pd.Timestamp("2024-01-01"), INFINITY_TIMESTAMP,
         pd.Timestamp("2024-01-01"), INFINITY_TIMESTAMP],
        # Record starting ON backfill date - CAN be tombstoned
        [2, "test", 30, 40, pd.Timestamp("2024-01-05"), INFINITY_TIMESTAMP,
         pd.Timestamp("2024-01-05"), INFINITY_TIMESTAMP],
        # Record starting AFTER backfill date - should NOT be tombstoned
        [3, "test", 50, 60, pd.Timestamp("2024-01-10"), INFINITY_TIMESTAMP,
         pd.Timestamp("2024-01-10"), INFINITY_TIMESTAMP],
    ], columns=["id", "field", "mv", "price", "effective_from", "effective_to",
                "as_of_from", "as_of_to"])

    # Backfill with no updates for existing IDs (all would be considered for tombstoning)
    updates = pd.DataFrame([
        [99, "test", 100, 200, pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-05"),
         pd.Timestamp("2024-01-01"), INFINITY_TIMESTAMP],
    ], columns=["id", "field", "mv", "price", "effective_from", "effective_to",
                "as_of_from", "as_of_to"])

    # System date is 2024-01-05 (midpoint)
    expiries, inserts = processor.compute_changes(
        current_state, updates,
        system_date='2024-01-05',
        update_mode='full_state'
    )

    # Records id=1 and id=2 should be expired (effective_from <= system_date)
    # Record id=3 should NOT be expired (effective_from > system_date)
    assert len(expiries) == 2, "Only records with effective_from <= system_date should be expired"

    expired_ids = set(expiries['id'].tolist())
    assert 1 in expired_ids, "Record id=1 should be expired"
    assert 2 in expired_ids, "Record id=2 should be expired"
    assert 3 not in expired_ids, "Record id=3 should NOT be expired (effective_from > system_date)"

    # CRITICAL: Verify no inserted record has effective_from > effective_to
    for i, row in inserts.iterrows():
        eff_from = row['effective_from']
        eff_to = row['effective_to']
        if eff_to != INFINITY_TIMESTAMP:
            assert eff_from <= eff_to, \
                f"Invalid range detected at row {i}: effective_from ({eff_from}) > effective_to ({eff_to})"


def test_backfill_does_not_merge_tombstone_with_open_ended():
    """
    Test: Backfill should NOT merge tombstones with open-ended updates.

    This tests the fix for the "missing inserts during backfill" bug where
    tombstones (bounded records) were incorrectly merged with open-ended updates,
    causing the update to be lost.

    Scenario:
    - Day 1: Record exists [2024-01-01, infinity)
    - Day 2: Record is tombstoned [2024-01-01, 2024-01-02) because it was removed
    - Backfill: Re-add the record for Day 2 [2024-01-02, infinity)
    - Expected: Insert the new record separately, DON'T merge with tombstone
    """
    processor = BitemporalTimeseriesProcessor(
        id_columns=['parent_id', 'child_id', 'path_length'],
        value_columns=['depth']
    )

    # Current state after Day 2: contains the tombstone from Day 1
    current_state = pd.DataFrame([
        # Tombstone: record was closed at Day 2
        {'parent_id': 2, 'child_id': 3, 'path_length': 1, 'depth': 0,
         'effective_from': pd.Timestamp('2024-01-01'),
         'effective_to': pd.Timestamp('2024-01-02'),  # BOUNDED - tombstone
         'as_of_from': pd.Timestamp('2024-01-02'),
         'as_of_to': INFINITY_TIMESTAMP,
         'value_hash': 'hash_a'},
    ])

    # Backfill: Re-add the record for Day 2
    backfill = pd.DataFrame([
        {'parent_id': 2, 'child_id': 3, 'path_length': 1, 'depth': 0,
         'effective_from': pd.Timestamp('2024-01-02'),
         'effective_to': INFINITY_TIMESTAMP,  # OPEN-ENDED
         'as_of_from': pd.Timestamp('2024-01-02'),
         'as_of_to': INFINITY_TIMESTAMP,
         'value_hash': 'hash_a'},  # Same hash as tombstone
    ])

    expiries, inserts = processor.compute_changes(
        current_state, backfill,
        system_date='2024-01-02',
        update_mode='full_state'
    )

    # The tombstone should NOT be expired (it's historical record)
    assert len(expiries) == 0, "Tombstone should not be expired during backfill"

    # The new record should be inserted separately (not merged with tombstone)
    assert len(inserts) == 1, "Backfill record should be inserted"

    # Verify the inserted record has the correct temporal range
    inserted = inserts.iloc[0]
    assert inserted['effective_from'] == pd.Timestamp('2024-01-02'), \
        "Inserted record should start at 2024-01-02"
    assert inserted['effective_to'] == INFINITY_TIMESTAMP, \
        "Inserted record should be open-ended"

    # CRITICAL: The insert should NOT have been merged with the tombstone
    # If merged incorrectly, effective_from would be 2024-01-01
    assert inserted['effective_from'] != pd.Timestamp('2024-01-01'), \
        "BUG: Record was incorrectly merged with tombstone!"


def test_exact_match_with_multiple_current_records():
    """
    Test: When multiple current records have the same hash but different effective dates,
    the algorithm should find the one with an exact temporal match.

    Bug fix: Previously, the algorithm would stop at the FIRST matching hash and not
    check if other records with the same hash had an exact temporal match.

    Scenario:
    - Current state has two records for ID (1, 2, 1):
      - [2024-01-01, infinity) with hash 'a'  (Day 1)
      - [2024-01-02, infinity) with hash 'a'  (Day 2)
    - Update sends record [2024-01-02, infinity) with hash 'a'
    - Expected: NO insert needed (exact match exists)
    """
    processor = BitemporalTimeseriesProcessor(
        id_columns=['parent_id', 'child_id', 'path_length'],
        value_columns=['depth']
    )

    # Current state has two records for the same ID with same hash but different dates
    current_state = pd.DataFrame([
        # Day 1 record
        {'parent_id': 1, 'child_id': 2, 'path_length': 1, 'depth': 0,
         'effective_from': pd.Timestamp('2024-01-01'),
         'effective_to': INFINITY_TIMESTAMP,
         'as_of_from': pd.Timestamp('2024-01-01'),
         'as_of_to': INFINITY_TIMESTAMP,
         'value_hash': 'hash_a'},
        # Day 2 record - same ID, same hash, different effective_from
        {'parent_id': 1, 'child_id': 2, 'path_length': 1, 'depth': 0,
         'effective_from': pd.Timestamp('2024-01-02'),
         'effective_to': INFINITY_TIMESTAMP,
         'as_of_from': pd.Timestamp('2024-01-02'),
         'as_of_to': INFINITY_TIMESTAMP,
         'value_hash': 'hash_a'},
    ])

    # Update sends the same record as Day 2
    update = pd.DataFrame([
        {'parent_id': 1, 'child_id': 2, 'path_length': 1, 'depth': 0,
         'effective_from': pd.Timestamp('2024-01-02'),
         'effective_to': INFINITY_TIMESTAMP,
         'as_of_from': pd.Timestamp('2024-01-02'),
         'as_of_to': INFINITY_TIMESTAMP,
         'value_hash': 'hash_a'},
    ])

    expiries, inserts = processor.compute_changes(
        current_state, update,
        system_date='2024-01-02',
        update_mode='full_state'
    )

    # No expiries needed - records are correct
    assert len(expiries) == 0, "No expiries expected - records are correct"

    # CRITICAL: No inserts needed - exact match exists
    # Bug: Previously this would insert because it found 2024-01-01 first (non-exact match)
    assert len(inserts) == 0, \
        "BUG: Record was inserted even though exact match exists in current state"


def test_deduplication_with_same_hash_different_ids():
    """
    Test: Records with same hash but different IDs should NOT be deduplicated.

    Bug fix: The deduplication logic was incorrectly treating records as duplicates
    if they had the same (effective_from, effective_to, value_hash), ignoring ID columns.
    This caused records from different ID groups to be incorrectly dropped.

    Scenario:
    - Current state: A->B (1->2) with hash 'same_hash'
    - Incoming: A->B (1->2), B->C (2->3), A->C (1->3) all with 'same_hash'
    - Expected: Insert B->C and A->C (new IDs), skip A->B (already exists)
    """
    processor = BitemporalTimeseriesProcessor(
        id_columns=['parent_id', 'child_id', 'path_length'],
        value_columns=['depth']
    )

    current_state = pd.DataFrame([{
        'parent_id': 1, 'child_id': 2, 'path_length': 1, 'depth': 0,
        'effective_from': pd.Timestamp('2024-01-01'),
        'effective_to': INFINITY_TIMESTAMP,
        'as_of_from': pd.Timestamp('2024-01-01'),
        'as_of_to': INFINITY_TIMESTAMP,
        'value_hash': 'same_hash'
    }])

    incoming = pd.DataFrame([
        {'parent_id': 1, 'child_id': 2, 'path_length': 1, 'depth': 0,
         'effective_from': pd.Timestamp('2024-01-01'), 'effective_to': INFINITY_TIMESTAMP,
         'as_of_from': pd.Timestamp('2024-01-01'), 'as_of_to': INFINITY_TIMESTAMP,
         'value_hash': 'same_hash'},
        {'parent_id': 2, 'child_id': 3, 'path_length': 1, 'depth': 0,  # B->C - NEW
         'effective_from': pd.Timestamp('2024-01-01'), 'effective_to': INFINITY_TIMESTAMP,
         'as_of_from': pd.Timestamp('2024-01-01'), 'as_of_to': INFINITY_TIMESTAMP,
         'value_hash': 'same_hash'},
        {'parent_id': 1, 'child_id': 3, 'path_length': 2, 'depth': 0,  # A->C - NEW
         'effective_from': pd.Timestamp('2024-01-01'), 'effective_to': INFINITY_TIMESTAMP,
         'as_of_from': pd.Timestamp('2024-01-01'), 'as_of_to': INFINITY_TIMESTAMP,
         'value_hash': 'same_hash'},
    ])

    expiries, inserts = processor.compute_changes(
        current_state, incoming,
        system_date='2024-01-01',
        update_mode='full_state'
    )

    assert len(expiries) == 0, "No expiries expected"
    assert len(inserts) == 2, \
        f"Expected 2 inserts (B->C and A->C), got {len(inserts)}"

    # Verify the correct records were inserted
    has_bc = any((inserts['parent_id'] == 2) & (inserts['child_id'] == 3))
    has_ac = any((inserts['parent_id'] == 1) & (inserts['child_id'] == 3))
    assert has_bc, "BUG: B->C (2->3) was incorrectly deduplicated"
    assert has_ac, "BUG: A->C (1->3) was incorrectly deduplicated"


def test_exact_match_priority_over_adjacent():
    """
    Test: Exact match should have priority over adjacent match when searching.

    Scenario:
    - Current has adjacent record [2024-01-01, 2024-01-02) with same hash
    - Current also has exact match [2024-01-02, infinity) with same hash
    - Update sends [2024-01-02, infinity) with same hash
    - Expected: Find exact match (no insert), NOT merge with adjacent
    """
    processor = BitemporalTimeseriesProcessor(
        id_columns=['parent_id', 'child_id', 'path_length'],
        value_columns=['depth']
    )

    current_state = pd.DataFrame([
        # Adjacent record (would be a merge candidate)
        {'parent_id': 1, 'child_id': 2, 'path_length': 1, 'depth': 0,
         'effective_from': pd.Timestamp('2024-01-01'),
         'effective_to': pd.Timestamp('2024-01-02'),  # Ends at 2024-01-02
         'as_of_from': pd.Timestamp('2024-01-01'),
         'as_of_to': INFINITY_TIMESTAMP,
         'value_hash': 'hash_a'},
        # Exact match record
        {'parent_id': 1, 'child_id': 2, 'path_length': 1, 'depth': 0,
         'effective_from': pd.Timestamp('2024-01-02'),
         'effective_to': INFINITY_TIMESTAMP,
         'as_of_from': pd.Timestamp('2024-01-02'),
         'as_of_to': INFINITY_TIMESTAMP,
         'value_hash': 'hash_a'},
    ])

    update = pd.DataFrame([
        {'parent_id': 1, 'child_id': 2, 'path_length': 1, 'depth': 0,
         'effective_from': pd.Timestamp('2024-01-02'),
         'effective_to': INFINITY_TIMESTAMP,
         'as_of_from': pd.Timestamp('2024-01-02'),
         'as_of_to': INFINITY_TIMESTAMP,
         'value_hash': 'hash_a'},
    ])

    expiries, inserts = processor.compute_changes(
        current_state, update,
        system_date='2024-01-02',
        update_mode='full_state'
    )

    # Should find exact match - no changes needed
    assert len(expiries) == 0, "No expiries expected - exact match found"
    assert len(inserts) == 0, \
        "No inserts expected - exact match should be found, not merged with adjacent"