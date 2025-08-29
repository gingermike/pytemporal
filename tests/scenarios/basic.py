from typing import Tuple, List
from pandas import to_datetime as pdt

from pytemporal import INFINITY_TIMESTAMP
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
            [4562, "test", 1, 1, pdt("2020-01-01"), INFINITY_TIMESTAMP, pdt_now, pd_max],
            [1234, "test", 2, 2, pdt("2022-01-01"), INFINITY_TIMESTAMP, pdt_now, pd_max],
            [1234, "fielda", 400, 500, pdt("2022-01-01"), pdt("2023-01-01"), pdt_past, pd_max],
        ],
        (
            [],
            [
                [4562, "test", 1, 1, pdt("2020-01-01"), INFINITY_TIMESTAMP, pdt_now, pd_max],
                [1234, "test", 2, 2, pdt("2022-01-01"), INFINITY_TIMESTAMP, pdt_now, pd_max],
                [1234, "fielda", 400, 500, pdt("2022-01-01"), pdt("2023-01-01"), pdt_past, pd_max],
            ]
        )
    )


def _append_tail() -> Tuple[List, List, Tuple]:
    """
    Update at the end of existing point
    """
    return (
        [
            [1234, "test", 300, 400, pdt("2020-01-01"), INFINITY_TIMESTAMP, pdt_past, pd_max],
        ],
        [
            [1234, "test", 2, 2, pdt("2022-06-30"), INFINITY_TIMESTAMP, pdt_now, pd_max]
        ],
        (
            [
                [1234, "test", 300, 400, pdt("2020-01-01"), INFINITY_TIMESTAMP, pdt_past, pd_max],
            ],
            [
                [1234, "test", 300, 400, pdt("2020-01-01"), pdt("2022-06-30"), pdt_now, pd_max],
                [1234, "test", 2, 2, pdt("2022-06-30"), INFINITY_TIMESTAMP, pdt_now, pd_max]
            ]
        )
    )


def _append_tail_exact() -> Tuple[List, List, Tuple]:
    """
    Update at the exact end of existing point
    """
    return (
        [
            [1234, "test", 300, 400, pdt("2020-01-01"), pdt("2020-06-30"), pdt_past, pd_max],
        ],
        [
            [1234, "test", 2, 2, pdt("2022-06-30"), INFINITY_TIMESTAMP, pdt_now, pd_max]
        ],
        (
            [],
            [
                [1234, "test", 2, 2, pdt("2022-06-30"), INFINITY_TIMESTAMP, pdt_now, pd_max]
            ]
        )
    )


def _append_head() -> Tuple[List, List, Tuple]:
    """
    Update at the start of existing point
    """
    return (
        [
            [1234, "test", 300, 400, pdt("2020-01-01"), INFINITY_TIMESTAMP, pdt_past, pd_max],
        ],
        [
            [1234, "test", 2, 2, pdt("2019-06-30"), pdt("2021-01-01"), pdt_now, pd_max]
        ],
        (
            [
                [1234, "test", 300, 400, pdt("2020-01-01"), INFINITY_TIMESTAMP, pdt_past, pd_max]
            ],
            [
                [1234, "test", 2, 2, pdt("2019-06-30"), pdt("2021-01-01"), pdt_now, pd_max],
                [1234, "test", 300, 400, pdt("2021-01-01"), INFINITY_TIMESTAMP, pdt_now, pd_max]
            ]
        )
    )


def _append_head_exact() -> Tuple[List, List, Tuple]:
    """
    Update at the exact start of existing point
    """
    return (
        [
            [1234, "test", 300, 400, pdt("2020-01-01"), INFINITY_TIMESTAMP, pdt_past, pd_max],
        ],
        [
            [1234, "test", 2, 2, pdt("2019-06-30"), pdt("2020-01-01"), pdt_now, pd_max]
        ],
        (
            [],
            [
                [1234, "test", 2, 2, pdt("2019-06-30"), pdt("2020-01-01"), pdt_now, pd_max]
            ]
        )
    )


def _intersect() -> Tuple[List, List, Tuple]:
    """
    Update in the middle of an existing point
    """
    return (
        [
            [1234, "test", 300, 400, pdt("2020-01-01"), INFINITY_TIMESTAMP, pdt_past, pd_max],
        ],
        [
            [1234, "test", 2, 2, pdt("2021-01-01"), pdt("2021-06-01"), pdt_now, pd_max]
        ],
        (
            [
                [1234, "test", 300, 400, pdt("2020-01-01"), INFINITY_TIMESTAMP, pdt_past, pd_max],
            ],
            [
                [1234, "test", 300, 400, pdt("2020-01-01"), pdt("2021-01-01"), pdt_now, pd_max],
                [1234, "test", 2, 2, pdt("2021-01-01"), pdt("2021-06-01"), pdt_now, pd_max],
                [1234, "test", 300, 400, pdt("2021-06-01"), INFINITY_TIMESTAMP, pdt_now, pd_max]
            ]
        )
    )


def _no_change() -> Tuple[List, List, Tuple]:
    """
    Value has not changed, does not result in insert or expiration
    """
    return (
        [
            [1234, "test", 300, 400, pdt("2020-01-01"), INFINITY_TIMESTAMP, pdt_past, pd_max],
        ],
        [
            [1234, "test", 300, 400, pdt("2020-01-01"), INFINITY_TIMESTAMP, pdt_now, pd_max]
        ],
        (
            [],
            []
        )
    )


def _full_state_basic() -> Tuple[List, List, Tuple]:
    return (
        [
            [1, "test", 100.0, 250.0, pdt("2020-01-01"), pdt("2020-02-01"), pdt_past, pd_max],
            [2, "test2", 300.0, 400.0, pdt("2020-01-01"), pdt("2020-02-01"), pdt_past, pd_max],
        ],
        [
            [1, "test", 150.0, 250.0, pdt("2020-01-01"), pdt("2020-02-01"), pdt_now, pd_max],
            [2, "test2", 300.0, 400.0, pdt("2020-01-01"), pdt("2020-02-01"), pdt_now, pd_max],
            [3, "test3", 500.0, 600.0, pdt("2020-01-01"), pdt("2020-02-01"), pdt_now, pd_max],
        ],
        (
            [
                [1, "test", 100.0, 250.0, pdt("2020-01-01"), pdt("2020-02-01"), pdt_past, pd_max]
            ],
            [
                [1, "test", 150.0, 250.0, pdt("2020-01-01"), pdt("2020-02-01"), pdt_now, pd_max],
                [3, "test3", 500.0, 600.0, pdt("2020-01-01"), pdt("2020-02-01"), pdt_now, pd_max],
            ]
        )
    )


insert = BitemporalScenario("insert", _insert, "delta")
overwrite = BitemporalScenario("overwrite", _overwrite, "delta")
unrelated_state = BitemporalScenario("unrelated_state", _unrelated_state, "delta")
append_tail = BitemporalScenario("append_tail", _append_tail, "delta")
append_tail_exact = BitemporalScenario("append_tail_exact", _append_tail_exact, "delta")
append_head = BitemporalScenario("append_head", _append_head, "delta")
append_head_exact = BitemporalScenario("append_head_exact", _append_head_exact, "delta")
intersect = BitemporalScenario("intersect", _intersect, "delta")
no_change = BitemporalScenario("no_change", _no_change, "delta")
full_state_basic = BitemporalScenario("full_state_basic", _full_state_basic, "full_state")
