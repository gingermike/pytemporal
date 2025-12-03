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

def test_backfill_does_not_expire_adjacent_same_value_record():
    """
    Bug fix: Multi-day backfill should not pull in adjacent records.

    This tests the fix for the "exclusion constraint violation" bug where
    backfilling Day 2 data incorrectly expired Day 1 because Day 1 was adjacent
    to the update and had the same value hash.

    Scenario:
    - Day 1: [2024-01-01, 2024-01-02) with weight=100
    - Day 2: [2024-01-02, 2024-01-03) with weight=200
    - Day 3: [2024-01-03, 2024-01-04) with weight=300
    - Backfill Day 2 with weight=100 (same as Day 1!)

    Expected: Only Day 2 should be expired and updated
    Bug: Day 1 was also expired because it was adjacent and had same hash as update
    """
    processor = BitemporalTimeseriesProcessor(
        id_columns=['parent_id', 'child_id', 'source'],
        value_columns=['weight']
    )

    # Current state: Three consecutive days
    current_state = pd.DataFrame([
        # Day 1: weight=100
        {'parent_id': 1, 'child_id': 2, 'source': 'arm', 'weight': 100,
         'effective_from': pd.Timestamp('2024-01-01'),
         'effective_to': pd.Timestamp('2024-01-02'),
         'as_of_from': pd.Timestamp('2024-01-01 10:00:00'),
         'as_of_to': INFINITY_TIMESTAMP},
        # Day 2: weight=200 (will be corrected to 100)
        {'parent_id': 1, 'child_id': 2, 'source': 'arm', 'weight': 200,
         'effective_from': pd.Timestamp('2024-01-02'),
         'effective_to': pd.Timestamp('2024-01-03'),
         'as_of_from': pd.Timestamp('2024-01-02 10:00:00'),
         'as_of_to': INFINITY_TIMESTAMP},
        # Day 3: weight=300
        {'parent_id': 1, 'child_id': 2, 'source': 'arm', 'weight': 300,
         'effective_from': pd.Timestamp('2024-01-03'),
         'effective_to': pd.Timestamp('2024-01-04'),
         'as_of_from': pd.Timestamp('2024-01-03 10:00:00'),
         'as_of_to': INFINITY_TIMESTAMP},
    ])

    # Backfill: Correct Day 2 to have weight=100 (same as Day 1!)
    update = pd.DataFrame([
        {'parent_id': 1, 'child_id': 2, 'source': 'arm', 'weight': 100,
         'effective_from': pd.Timestamp('2024-01-02'),
         'effective_to': pd.Timestamp('2024-01-03'),
         'as_of_from': pd.Timestamp('2024-01-10 10:00:00'),
         'as_of_to': INFINITY_TIMESTAMP},
    ])

    expiries, inserts = processor.compute_changes(
        current_state, update,
        system_date='2024-01-10',
        update_mode='delta'
    )

    # CRITICAL: Only 1 expiry (Day 2), NOT 2 (Day 1 + Day 2)
    assert len(expiries) == 1, \
        f"BUG: Expected 1 expiry (Day 2 only), got {len(expiries)}. Day 1 was incorrectly expired!"

    # Verify the expired record is Day 2, not Day 1
    expired_eff_from = expiries['effective_from'].tolist()
    assert pd.Timestamp('2024-01-01') not in expired_eff_from, \
        "BUG: Day 1 (2024-01-01) was incorrectly expired!"
    assert pd.Timestamp('2024-01-02') in expired_eff_from, \
        "Day 2 (2024-01-02) should be expired"

    # Should have exactly 1 insert (the corrected Day 2)
    assert len(inserts) == 1, \
        f"Expected 1 insert (corrected Day 2), got {len(inserts)}"

    # Verify the insert is for Day 2 range, NOT merged with Day 1
    insert_eff_from = inserts.iloc[0]['effective_from']
    assert insert_eff_from == pd.Timestamp('2024-01-02'), \
        f"BUG: Insert starts at {insert_eff_from}, expected 2024-01-02. Was incorrectly merged with Day 1!"


def test_extension_still_works_with_single_current_record():
    """
    Test: Extension scenario should still work (single current + adjacent update).

    This ensures the backfill fix doesn't break the legitimate extension behavior
    where a single current record + adjacent update with same values should merge.
    """
    processor = BitemporalTimeseriesProcessor(
        id_columns=['parent_id', 'child_id', 'source'],
        value_columns=['weight']
    )

    # Single current record
    current_state = pd.DataFrame([
        {'parent_id': 1, 'child_id': 2, 'source': 'arm', 'weight': 100,
         'effective_from': pd.Timestamp('2024-01-01'),
         'effective_to': pd.Timestamp('2024-01-02'),
         'as_of_from': pd.Timestamp('2024-01-01 10:00:00'),
         'as_of_to': INFINITY_TIMESTAMP},
    ])

    # Adjacent update with same values (extension)
    update = pd.DataFrame([
        {'parent_id': 1, 'child_id': 2, 'source': 'arm', 'weight': 100,
         'effective_from': pd.Timestamp('2024-01-02'),
         'effective_to': pd.Timestamp('2024-01-03'),
         'as_of_from': pd.Timestamp('2024-01-10 10:00:00'),
         'as_of_to': INFINITY_TIMESTAMP},
    ])

    expiries, inserts = processor.compute_changes(
        current_state, update,
        system_date='2024-01-10',
        update_mode='delta'
    )

    # Should expire the current record (merging)
    assert len(expiries) == 1, \
        "Extension scenario: current record should be expired for merging"

    # Should have 1 merged insert
    assert len(inserts) == 1, \
        "Extension scenario: should have 1 merged insert"

    # Verify the merged record spans [2024-01-01, 2024-01-03)
    merged_from = inserts.iloc[0]['effective_from']
    merged_to = inserts.iloc[0]['effective_to']

    assert merged_from == pd.Timestamp('2024-01-01'), \
        f"Merged record should start at 2024-01-01, got {merged_from}"
    assert merged_to == pd.Timestamp('2024-01-03'), \
        f"Merged record should end at 2024-01-03, got {merged_to}"


def test_update_contained_in_current_is_no_op():
    """
    Test: When update is fully contained within current record with same values,
    it should be a NO-OP (no expiries, no inserts).

    This is a regression test for a bug where the full_state mode would incorrectly
    insert a new record even when the update was completely covered by existing state.

    Scenario:
    - Current: A->B effective=[2024-01-01, infinity) with hash X
    - Update: A->B effective=[2024-01-02, 2024-01-03) with hash X (same values)
    - Expected: NO-OP (current already covers this period with same values)
    """
    processor = BitemporalTimeseriesProcessor(
        id_columns=['parent_id', 'child_id', 'source'],
        value_columns=['weight']  # Use value columns so hash is meaningful
    )

    # Current state: open-ended record from 2024-01-01
    current_state = pd.DataFrame([
        {'parent_id': 1, 'child_id': 2, 'source': 'arm', 'weight': 100,
         'effective_from': pd.Timestamp('2024-01-01'),
         'effective_to': INFINITY_TIMESTAMP,
         'as_of_from': pd.Timestamp('2024-01-01 10:00:00'),
         'as_of_to': INFINITY_TIMESTAMP},
    ])

    # Backfill update: bounded period WITHIN current range, SAME values
    backfill_update = pd.DataFrame([
        {'parent_id': 1, 'child_id': 2, 'source': 'arm', 'weight': 100,
         'effective_from': pd.Timestamp('2024-01-02'),
         'effective_to': pd.Timestamp('2024-01-03'),
         'as_of_from': pd.Timestamp('2024-01-05 10:00:00'),
         'as_of_to': INFINITY_TIMESTAMP},
    ])

    expiries, inserts = processor.compute_changes(
        current_state, backfill_update,
        system_date='2024-01-05',
        update_mode='full_state'
    )

    # Should be NO-OP: no expiries, no inserts
    assert len(expiries) == 0, \
        f"BUG: Expected 0 expiries (current covers update), got {len(expiries)}"
    assert len(inserts) == 0, \
        f"BUG: Expected 0 inserts (current covers update with same values), got {len(inserts)}"


def test_mixed_bounded_and_open_ended_exact_match():
    """
    Regression test: When current state has a mix of open-ended and bounded records,
    and updates come in that match the bounded record exactly, it should be a NO-OP.

    Scenario:
    - Current state: 30 rows with effective [2025-10-10, infinity)
    - Current state: 1 row with effective [2025-10-10, 2025-10-11) (bounded/tombstone)
    - Updates: 31 rows ALL with effective [2025-10-10, 2025-10-11)

    For the 30 open-ended records:
    - Update is CONTAINED within current (same start, bounded end within infinity)
    - Should be NO-OP (current covers the period)

    For the 1 bounded record:
    - Update has EXACT same temporal range
    - Should be NO-OP (exact match)

    Bug: The bounded record was incorrectly being re-inserted.
    """
    processor = BitemporalTimeseriesProcessor(
        id_columns=['id'],
        value_columns=['value']
    )

    # Create current state: 30 open-ended + 1 bounded
    current_rows = []
    for i in range(30):
        current_rows.append({
            'id': i,
            'value': f'val_{i}',
            'effective_from': pd.Timestamp('2025-10-10'),
            'effective_to': INFINITY_TIMESTAMP,  # Open-ended
            'as_of_from': pd.Timestamp('2025-10-10 10:00:00'),
            'as_of_to': INFINITY_TIMESTAMP,
        })
    # Add the one bounded record
    current_rows.append({
        'id': 30,  # Different ID
        'value': 'val_30',
        'effective_from': pd.Timestamp('2025-10-10'),
        'effective_to': pd.Timestamp('2025-10-11'),  # Bounded
        'as_of_from': pd.Timestamp('2025-10-10 10:00:00'),
        'as_of_to': INFINITY_TIMESTAMP,
    })
    current_state = pd.DataFrame(current_rows)

    # Create updates: ALL 31 rows with bounded range
    update_rows = []
    for i in range(31):
        update_rows.append({
            'id': i,
            'value': f'val_{i}',  # Same values as current
            'effective_from': pd.Timestamp('2025-10-10'),
            'effective_to': pd.Timestamp('2025-10-11'),  # All bounded
            'as_of_from': pd.Timestamp('2025-10-11 10:00:00'),
            'as_of_to': INFINITY_TIMESTAMP,
        })
    updates = pd.DataFrame(update_rows)

    expiries, inserts = processor.compute_changes(
        current_state, updates,
        system_date='2025-10-11',
        update_mode='full_state'
    )

    # For the bounded record (id=30): exact match -> NO-OP
    # For the open-ended records (id=0-29): update contained in current -> NO-OP

    # Check that the bounded record (id=30) is NOT being re-inserted
    if len(inserts) > 0:
        inserted_ids = inserts['id'].tolist()
        assert 30 not in inserted_ids, \
            f"BUG: Bounded record (id=30) should NOT be re-inserted (exact match). " \
            f"Inserted IDs: {inserted_ids}"

    # Actually, for the 30 open-ended records, the update is NOT contained -
    # the update ends at 2025-10-11, but current extends to infinity.
    # These should result in inserts (new bounded segments) while current remains.
    # But the bounded record (id=30) should be an exact match -> NO-OP

    # Let's verify just the bounded record behavior
    bounded_inserts = inserts[inserts['id'] == 30] if len(inserts) > 0 else pd.DataFrame()
    assert len(bounded_inserts) == 0, \
        f"BUG: Bounded record (id=30) with exact temporal match should not be re-inserted. " \
        f"Got {len(bounded_inserts)} insert(s) for id=30"


def test_mixed_bounded_precomputed_hash_exact_match():
    """
    Variant test with pre-computed value_hash to match real-world scenario.

    Same as test_mixed_bounded_and_open_ended_exact_match but with explicit
    value_hash column already set (simulating data from a database).
    """
    processor = BitemporalTimeseriesProcessor(
        id_columns=['id'],
        value_columns=['value']
    )

    # Create current state: 30 open-ended + 1 bounded
    # All have pre-computed value_hash
    current_rows = []
    for i in range(30):
        current_rows.append({
            'id': i,
            'value': f'val_{i}',
            'value_hash': f'hash_{i}',  # Pre-computed hash
            'effective_from': pd.Timestamp('2025-10-10'),
            'effective_to': INFINITY_TIMESTAMP,  # Open-ended
            'as_of_from': pd.Timestamp('2025-10-10 10:00:00'),
            'as_of_to': INFINITY_TIMESTAMP,
        })
    # Add the one bounded record
    current_rows.append({
        'id': 30,
        'value': 'val_30',
        'value_hash': 'hash_30',  # Pre-computed hash
        'effective_from': pd.Timestamp('2025-10-10'),
        'effective_to': pd.Timestamp('2025-10-11'),  # Bounded
        'as_of_from': pd.Timestamp('2025-10-10 10:00:00'),
        'as_of_to': INFINITY_TIMESTAMP,
    })
    current_state = pd.DataFrame(current_rows)

    # Create updates: ALL 31 rows with bounded range and SAME hashes
    update_rows = []
    for i in range(31):
        update_rows.append({
            'id': i,
            'value': f'val_{i}',  # Same values as current
            'value_hash': f'hash_{i}',  # Same hash as current
            'effective_from': pd.Timestamp('2025-10-10'),
            'effective_to': pd.Timestamp('2025-10-11'),  # All bounded
            'as_of_from': pd.Timestamp('2025-10-11 10:00:00'),
            'as_of_to': INFINITY_TIMESTAMP,
        })
    updates = pd.DataFrame(update_rows)

    print(f"\nCurrent state bounded record (id=30):")
    print(current_state[current_state['id'] == 30][['id', 'effective_from', 'effective_to', 'value_hash']])
    print(f"\nUpdate for bounded record (id=30):")
    print(updates[updates['id'] == 30][['id', 'effective_from', 'effective_to', 'value_hash']])

    expiries, inserts = processor.compute_changes(
        current_state, updates,
        system_date='2025-10-11',
        update_mode='full_state'
    )

    print(f"\nTotal expiries: {len(expiries)}")
    print(f"Total inserts: {len(inserts)}")
    if len(inserts) > 0:
        print(f"Insert IDs: {inserts['id'].tolist()}")
        if 30 in inserts['id'].tolist():
            print(f"\nBounded record insert details:")
            print(inserts[inserts['id'] == 30])

    # Verify bounded record is not re-inserted
    bounded_inserts = inserts[inserts['id'] == 30] if len(inserts) > 0 else pd.DataFrame()
    assert len(bounded_inserts) == 0, \
        f"BUG: Bounded record (id=30) with exact temporal match should not be re-inserted. " \
        f"Got {len(bounded_inserts)} insert(s) for id=30"


def test_mixed_bounded_with_nanosecond_timestamps():
    """
    Test with nanosecond-precision timestamps (like database sources).

    Simulates data that comes from a database where timestamps might have
    nanosecond precision. This tests that the ns -> us conversion doesn't
    break exact match detection.
    """
    import numpy as np

    processor = BitemporalTimeseriesProcessor(
        id_columns=['id'],
        value_columns=['value']
    )

    # Create timestamps with explicit nanosecond precision
    # Simulate database data where timestamps might have subtle differences
    eff_from_current = np.datetime64('2025-10-10T00:00:00.000000000', 'ns')
    eff_to_bounded = np.datetime64('2025-10-11T00:00:00.000000000', 'ns')
    eff_to_infinity = INFINITY_TIMESTAMP

    # Simulate slightly different nanosecond timestamps (like from different DB queries)
    eff_from_update = np.datetime64('2025-10-10T00:00:00.000000001', 'ns')  # 1 nanosecond different
    eff_to_update = np.datetime64('2025-10-11T00:00:00.000000001', 'ns')  # 1 nanosecond different

    # Create current state
    current_rows = []
    for i in range(30):
        current_rows.append({
            'id': i,
            'value': f'val_{i}',
            'effective_from': pd.Timestamp(eff_from_current),
            'effective_to': eff_to_infinity,  # Open-ended
            'as_of_from': pd.Timestamp('2025-10-10 10:00:00'),
            'as_of_to': INFINITY_TIMESTAMP,
        })
    # Add bounded record
    current_rows.append({
        'id': 30,
        'value': 'val_30',
        'effective_from': pd.Timestamp(eff_from_current),
        'effective_to': pd.Timestamp(eff_to_bounded),  # Bounded
        'as_of_from': pd.Timestamp('2025-10-10 10:00:00'),
        'as_of_to': INFINITY_TIMESTAMP,
    })
    current_state = pd.DataFrame(current_rows)

    # Create updates with slightly different nanoseconds
    update_rows = []
    for i in range(31):
        update_rows.append({
            'id': i,
            'value': f'val_{i}',  # Same values
            'effective_from': pd.Timestamp(eff_from_update),  # 1 ns different!
            'effective_to': pd.Timestamp(eff_to_update),  # 1 ns different!
            'as_of_from': pd.Timestamp('2025-10-11 10:00:00'),
            'as_of_to': INFINITY_TIMESTAMP,
        })
    updates = pd.DataFrame(update_rows)

    print(f"\nCurrent effective_from (id=30): {current_state[current_state['id']==30]['effective_from'].values[0]}")
    print(f"Update effective_from (id=30): {updates[updates['id']==30]['effective_from'].values[0]}")
    print(f"Current effective_to (id=30): {current_state[current_state['id']==30]['effective_to'].values[0]}")
    print(f"Update effective_to (id=30): {updates[updates['id']==30]['effective_to'].values[0]}")

    expiries, inserts = processor.compute_changes(
        current_state, updates,
        system_date='2025-10-11',
        update_mode='full_state'
    )

    print(f"\nTotal expiries: {len(expiries)}")
    print(f"Total inserts: {len(inserts)}")
    if len(inserts) > 0:
        print(f"Insert IDs: {inserts['id'].tolist()}")
        # Show the bounded record insert if present
        if 30 in inserts['id'].tolist():
            print(f"\nBounded record being re-inserted (BUG):")
            print(inserts[inserts['id'] == 30][['id', 'effective_from', 'effective_to']])

    # The bounded record should NOT be re-inserted
    # After ns -> us conversion, the timestamps should be the same
    bounded_inserts = inserts[inserts['id'] == 30] if len(inserts) > 0 else pd.DataFrame()
    assert len(bounded_inserts) == 0, \
        f"BUG: Bounded record (id=30) should not be re-inserted after ns->us truncation. " \
        f"Got {len(bounded_inserts)} insert(s)"


def test_bounded_to_open_ended_extension_same_values():
    """
    Regression test: When a bounded (tombstone) record exists and an update
    with the SAME VALUES extends it to open-ended, the current should be
    expired and the update inserted.

    Scenario:
    - Current: [2025-10-10, 2025-10-11) with hash X (bounded/tombstone)
    - Update: [2025-10-10, infinity) with hash X (same values, extends to open-ended)
    - Expected: Expire current, insert update (no overlap!)

    This was a bug where the update was inserted WITHOUT expiring the current,
    causing an exclusion constraint violation on overlapping ranges.
    """
    processor = BitemporalTimeseriesProcessor(
        id_columns=['parent_id', 'child_id', 'source'],
        value_columns=['weight']
    )

    # Current state: bounded record (like a tombstone)
    current_state = pd.DataFrame([
        {'parent_id': 2, 'child_id': 3, 'source': 'arm', 'weight': 100,
         'effective_from': pd.Timestamp('2025-10-10'),
         'effective_to': pd.Timestamp('2025-10-11'),  # Bounded
         'as_of_from': pd.Timestamp('2025-10-10 10:00:00'),
         'as_of_to': INFINITY_TIMESTAMP},
    ])

    # Update: same ID, same values, but extends to infinity
    updates = pd.DataFrame([
        {'parent_id': 2, 'child_id': 3, 'source': 'arm', 'weight': 100,  # Same values!
         'effective_from': pd.Timestamp('2025-10-10'),
         'effective_to': INFINITY_TIMESTAMP,  # Now open-ended
         'as_of_from': pd.Timestamp('2025-10-11 10:00:00'),
         'as_of_to': INFINITY_TIMESTAMP},
    ])

    expiries, inserts = processor.compute_changes(
        current_state, updates,
        system_date='2025-10-11',
        update_mode='full_state'
    )

    # The bounded record should be EXPIRED (to avoid overlap)
    assert len(expiries) == 1, \
        f"Expected 1 expiry (the bounded record), got {len(expiries)}"

    # The new open-ended record should be INSERTED
    assert len(inserts) == 1, \
        f"Expected 1 insert (the extended record), got {len(inserts)}"

    # Verify the insert is the open-ended version
    assert inserts.iloc[0]['effective_to'] == INFINITY_TIMESTAMP, \
        f"Expected insert to be open-ended, got effective_to={inserts.iloc[0]['effective_to']}"
