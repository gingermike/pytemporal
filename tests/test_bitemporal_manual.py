from datetime import datetime
from typing import List, Tuple


from pandas._testing import assert_frame_equal
import pandas as pd

import pytest

from bitemporal_timeseries import BitemporalTimeseriesProcessor, POSTGRES_INFINITY
from tests.scenarios.basic import overwrite, insert, unrelated_state, append_tail, append_tail_exact, append_head, \
    append_head_exact, intersect
from tests.scenarios.complex import overlay_two, overlay_multiple, multi_intersection_single_point, \
    multi_intersection_multiple_point, multi_field, extend_current_row, extend_update
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

    #complex
    overlay_two,
    overlay_multiple,
    multi_intersection_single_point,
    multi_intersection_multiple_point,
    multi_field,
    extend_current_row,
    extend_update
]


@pytest.mark.parametrize(
    ("current_state", "updates", "expected"),
    [scenario.data() for scenario in scenarios],
    ids=[scenario.id for scenario in scenarios]
)
def test_update_scenarios(current_state: List, updates: List, expected: Tuple[List, List]):

    # Assemble
    processor = BitemporalTimeseriesProcessor(
        id_columns=default_id_columns,
        value_columns=default_value_columns
    )

    current_state_df = pd.DataFrame(current_state, columns=default_columns)
    updates_df = pd.DataFrame(updates, columns=default_columns)

    # Act
    expire, insert = processor.compute_changes(
        pd.DataFrame(current_state_df, columns=default_columns),
        pd.DataFrame(updates_df, columns=default_columns),
        update_mode="delta"
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

    assert all([x == POSTGRES_INFINITY for x in insert["as_of_to"].to_list()])
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