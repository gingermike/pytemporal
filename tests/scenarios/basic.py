from typing import Tuple, List
from pandas import to_datetime as pdt

from bitemporal_timeseries import POSTGRES_INFINITY
from tests.scenarios.defaults import pdt_now, pd_max, pdt_past, BitemporalScenario


def _insert() -> Tuple[List, List, Tuple]:
    """
    Defines a pure insert, no current state
    """
    return (
        [
        ],
        [
            [1234, "test", 300, 400, pdt("2020-01-01"), pdt("2021-01-01"), pdt_now, pd_max],
            [1234, "fielda", 400, 500, pdt("2020-01-01"), pdt("2021-01-01"), pdt_past, pd_max],
        ],
        (
            [
            ],
            [
                [1234, "test", 300, 400, pdt("2020-01-01"), pdt("2021-01-01"), pdt_now, pd_max],
                [1234, "fielda", 400, 500, pdt("2020-01-01"), pdt("2021-01-01"), pdt_past, pd_max]
            ]
        )
    )


def _overwrite() -> Tuple[List, List, Tuple]:
    """
    Defines a basic overwrite of a value
    """
    return (
        [
            [1234, "test", 300, 400, pdt("2020-01-01"), pdt("2021-01-01"), pdt_past, pd_max],
            [1234, "fielda", 400, 500, pdt("2020-01-01"), pdt("2021-01-01"), pdt_past, pd_max],
        ],
        [
            [1234, "test", 400, 300, pdt("2020-01-01"), pdt("2021-01-01"), pdt_now, pd_max]
        ],
        (
            [
                [1234, "test", 300, 400, pdt("2020-01-01"), pdt("2021-01-01"), pdt_past, pd_max],
            ],
            [
                [1234, "test", 400, 300, pdt("2020-01-01"), pdt("2021-01-01"), pdt_now, pd_max]
            ]
        )
    )


def _unrelated_state() -> Tuple[List, List, Tuple]:
    """
    Unrelated updates to the current state, should just be inserts
    """
    return (
        [
            [1234, "test", 300, 400, pdt("2020-01-01"), pdt("2021-01-01"), pdt_past, pd_max],
            [1234, "fielda", 400, 500, pdt("2020-01-01"), pdt("2021-01-01"), pdt_past, pd_max],
        ],
        [
            [4562, "test", 1, 1, pdt("2020-01-01"), POSTGRES_INFINITY, pdt_now, pd_max],
            [1234, "test", 2, 2, pdt("2022-01-01"), POSTGRES_INFINITY, pdt_now, pd_max]
        ],
        (
            [],
            [
                [4562, "test", 1, 1, pdt("2020-01-01"), POSTGRES_INFINITY, pdt_now, pd_max],
                [1234, "test", 2, 2, pdt("2022-01-01"), POSTGRES_INFINITY, pdt_now, pd_max]
            ]
        )
    )


insert = BitemporalScenario("insert", _insert)
overwrite = BitemporalScenario("overwrite", _overwrite)
unrelated_state = BitemporalScenario("unrelated_state", _unrelated_state)