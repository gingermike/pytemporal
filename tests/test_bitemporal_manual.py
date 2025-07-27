from datetime import datetime

import pandas as pd

from bitemporal_timeseries import BitemporalTimeseriesProcessor

columns = ["id", "field", "mv", "price", "effective_from", "effective_to", "as_of_from", "as_of_to"]

def test_bitemporal_overwrite():

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
                pd.to_datetime("2020-01-01"), pd.to_datetime("2021-01-01"),
                pd.to_datetime(datetime.now()), pd.Timestamp.max
        ]
    ]



    expire, insert = processor.compute_changes(
        pd.DataFrame(current_state, columns=columns),
        pd.DataFrame(update_state, columns=columns),
        update_mode="delta"
    )

    assert len(expire) == 1
    assert len(insert) == 1


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
        pd.DataFrame(current_state, columns=columns),
        pd.DataFrame(update_state, columns=columns),
        update_mode="delta"
    )

    assert len(expire) == 1

    # TODO fails as current algorithm doesn't conflate
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
        pd.DataFrame(current_state, columns=columns),
        pd.DataFrame(update_state, columns=columns),
        update_mode="delta"
    )

    assert len(expire) == 1

    # TODO fails as current algorithm doesn't conflate
    assert len(insert) == 2
    insert = insert.sort_values(by=["effective_from"])
    assert insert.loc[0]["effective_from"] == pd.to_datetime('2020-01-01')
    assert insert.loc[0]["effective_to"] == pd.to_datetime('2020-06-01')
    assert insert.loc[1]["effective_from"] == pd.to_datetime('2020-06-01')
    assert insert.loc[1]["effective_to"] == pd.to_datetime('2022-01-01')