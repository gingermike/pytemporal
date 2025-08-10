from typing import Tuple, List
from pandas import to_datetime as pdt

from bitemporal_timeseries import INFINITY_TIMESTAMP
from tests.scenarios.defaults import pdt_now, pd_max, pdt_past, BitemporalScenario


def _overlay_two() -> Tuple[List, List, Tuple]:
    """
    Overlaps two data points
    """
    return (
        [
            [1234, "test", 300, 400, pdt("2020-01-01"), pdt("2020-06-30"), pdt_past, pd_max],
            [1234, "test", 300, 400, pdt("2020-06-30"), INFINITY_TIMESTAMP, pdt_past, pd_max],
        ],
        [
            [1234, "test", 2, 2, pdt("2020-03-01"), pdt("2020-11-01"), pdt_now, pd_max]
        ],
        (
            [
                [1234, "test", 300, 400, pdt("2020-01-01"), pdt("2020-06-30"), pdt_past, pd_max],
                [1234, "test", 300, 400, pdt("2020-06-30"), INFINITY_TIMESTAMP, pdt_past, pd_max],
            ],
            [
                [1234, "test", 300, 400, pdt("2020-01-01"), pdt("2020-03-01"), pdt_now, pd_max],
                [1234, "test", 2, 2, pdt("2020-03-01"), pdt("2020-11-01"), pdt_now, pd_max],
                [1234, "test", 300, 400, pdt("2020-11-01"), INFINITY_TIMESTAMP, pdt_now, pd_max]
            ]
        )
    )


def _overlay_multiple() -> Tuple[List, List, Tuple]:
    """
    Overlaps multiple data points
    """
    return (
        [
            [1234, "test", 300, 400, pdt("2020-01-01"), pdt("2020-06-30"), pdt_past, pd_max],
            [1234, "test", 200, 200, pdt("2020-06-30"), pdt("2020-07-31"), pdt_past, pd_max],
            [1234, "test", 100, 100, pdt("2020-07-31"), INFINITY_TIMESTAMP, pdt_past, pd_max]
        ],
        [
            [1234, "test", 2, 2, pdt("2020-03-01"), pdt("2020-11-01"), pdt_now, pd_max]
        ],
        (
            [
                [1234, "test", 300, 400, pdt("2020-01-01"), pdt("2020-06-30"), pdt_past, pd_max],
                [1234, "test", 200, 200, pdt("2020-06-30"), pdt("2020-07-31"), pdt_past, pd_max],
                [1234, "test", 100, 100, pdt("2020-07-31"), INFINITY_TIMESTAMP, pdt_past, pd_max]
            ],
            [
                [1234, "test", 300, 400, pdt("2020-01-01"), pdt("2020-03-01"), pdt_now, pd_max],
                [1234, "test", 2, 2, pdt("2020-03-01"), pdt("2020-11-01"), pdt_now, pd_max],
                [1234, "test", 100, 100, pdt("2020-11-01"), INFINITY_TIMESTAMP, pdt_now, pd_max]
            ]
        )
    )


def _multi_intersection_single_point() -> Tuple[List, List, Tuple]:
    """
    Intersects a single point with multiple updates
    """
    return (
        [
            [1234, "test", 100, 100, pdt("2020-01-01"), INFINITY_TIMESTAMP, pdt_past, pd_max]
        ],
        [
            [1234, "test", 2, 2, pdt("2020-03-01"), pdt("2020-11-01"), pdt_now, pd_max],
            [1234, "test", 3, 4, pdt("2020-11-01"), pdt("2020-12-01"), pdt_now, pd_max],
            [1234, "test", 4, 5, pdt("2020-12-01"), pdt("2021-06-01"), pdt_now, pd_max],
        ],
        (
            [
                [1234, "test", 100, 100, pdt("2020-01-01"), INFINITY_TIMESTAMP, pdt_past, pd_max]
            ],
            [
                [1234, "test", 100, 100, pdt("2020-01-01"), pdt("2020-03-01"), pdt_now, pd_max],
                [1234, "test", 2, 2, pdt("2020-03-01"), pdt("2020-11-01"), pdt_now, pd_max],
                [1234, "test", 3, 4, pdt("2020-11-01"), pdt("2020-12-01"), pdt_now, pd_max],
                [1234, "test", 4, 5, pdt("2020-12-01"), pdt("2021-06-01"), pdt_now, pd_max],
                [1234, "test", 100, 100, pdt("2021-06-01"), INFINITY_TIMESTAMP, pdt_now, pd_max],
            ]
        )
    )


def _multi_intersection_multiple_point() -> Tuple[List, List, Tuple]:
    """
    Intersects multiple points with mulitple updates
    """
    return (
        [
            [1234, "test", 100, 100, pdt("2020-01-01"), pdt("2021-01-01"), pdt_past, pd_max],
            [1234, "test", 200, 200, pdt("2021-01-01"), pdt("2022-01-01"), pdt_past, pd_max]
        ],
        [
            [1234, "test", 2, 2, pdt("2020-03-01"), pdt("2020-11-01"), pdt_now, pd_max],
            [1234, "test", 3, 4, pdt("2020-11-01"), pdt("2020-12-01"), pdt_now, pd_max],
            [1234, "test", 4, 5, pdt("2020-12-01"), pdt("2021-06-01"), pdt_now, pd_max],
        ],
        (
            [
                [1234, "test", 100, 100, pdt("2020-01-01"), pdt("2021-01-01"), pdt_past, pd_max],
                [1234, "test", 200, 200, pdt("2021-01-01"), pdt("2022-01-01"), pdt_past, pd_max]
            ],
            [
                [1234, "test", 100, 100, pdt("2020-01-01"), pdt("2020-03-01"), pdt_now, pd_max],
                [1234, "test", 2, 2, pdt("2020-03-01"), pdt("2020-11-01"), pdt_now, pd_max],
                [1234, "test", 3, 4, pdt("2020-11-01"), pdt("2020-12-01"), pdt_now, pd_max],
                [1234, "test", 4, 5, pdt("2020-12-01"), pdt("2021-06-01"), pdt_now, pd_max],
                [1234, "test", 200, 200, pdt("2021-06-01"), pdt("2022-01-01"), pdt_now, pd_max],
            ]
        )
    )


def _multi_field() -> Tuple[List, List, Tuple]:
    """
    Multiple fields with multiple updates
    """

    return (
        [
            [1234, "test", 100, 100, pdt("2020-01-01"), pdt("2021-01-01"), pdt_past, pd_max],
            [1234, "test_2", 200, 200, pdt("2021-02-01"), pdt("2022-01-01"), pdt_past, pd_max]
        ],
        [
            [1234, "test", 2, 2, pdt("2020-03-01"), pdt("2020-11-01"), pdt_now, pd_max],
            [1234, "test", 3, 4, pdt("2020-11-01"), pdt("2020-12-01"), pdt_now, pd_max],
            [1234, "test_2", 4, 5, pdt("2020-12-01"), pdt("2021-06-01"), pdt_now, pd_max],
        ],
        (
            [
                [1234, "test", 100, 100, pdt("2020-01-01"), pdt("2021-01-01"), pdt_past, pd_max],
                [1234, "test_2", 200, 200, pdt("2021-02-01"), pdt("2022-01-01"), pdt_past, pd_max]
            ],
            [
                [1234, "test", 100, 100, pdt("2020-01-01"), pdt("2020-03-01"), pdt_now, pd_max],
                [1234, "test", 2, 2, pdt("2020-03-01"), pdt("2020-11-01"), pdt_now, pd_max],
                [1234, "test", 3, 4, pdt("2020-11-01"), pdt("2020-12-01"), pdt_now, pd_max],
                [1234, "test", 100, 100, pdt("2020-12-01"), pdt("2021-01-01"), pdt_now, pd_max],

                [1234, "test_2", 4, 5, pdt("2020-12-01"), pdt("2021-06-01"), pdt_now, pd_max],
                [1234, "test_2", 200, 200, pdt("2021-06-01"), pdt("2022-01-01"), pdt_now, pd_max],

            ]
        )
    )


def _extend_current_row() -> Tuple[List, List, Tuple]:
    """
    Extends the row if values are equal
    """
    return (
        [
            [1234, "test", 100, 100, pdt("2020-01-01"), pdt("2021-01-01"), pdt_past, pd_max],
        ],
        [
            [1234, "test", 100, 100, pdt("2021-01-01"), pdt("2022-11-01"), pdt_now, pd_max],
        ],
        (
            [
                [1234, "test", 100, 100, pdt("2020-01-01"), pdt("2021-01-01"), pdt_past, pd_max],
            ],
            [
                [1234, "test", 100, 100, pdt("2020-01-01"), pdt("2022-11-01"), pdt_now, pd_max],
            ]
        )
    )


def _extend_update() -> Tuple[List, List, Tuple]:
    """
    Extends the update if values are equal
    """
    return (
        [
            [1234, "test", 100, 100, pdt("2020-01-01"), INFINITY_TIMESTAMP, pdt_past, pd_max],
        ],
        [
            [1234, "test", 100, 100, pdt("2019-01-01"), pdt("2020-01-01"), pdt_now, pd_max],
        ],
        (
            [
                [1234, "test", 100, 100, pdt("2020-01-01"), INFINITY_TIMESTAMP, pdt_past, pd_max],
            ],
            [
                [1234, "test", 100, 100, pdt("2019-01-01"), INFINITY_TIMESTAMP, pdt_now, pd_max],
            ]
        )
    )


def _no_change_with_intersection() -> Tuple[List, List, Tuple]:
    """
    Does not emit any updates if update is an overlap with no value change
    """
    return (
        [
            [1234, "test", 100, 100, pdt("2020-01-01"), INFINITY_TIMESTAMP, pdt_past, pd_max],
        ],
        [
            [1234, "test", 100, 100, pdt("2020-02-01"), pdt("2020-04-01"), pdt_now, pd_max],
        ],
        (
            [],
            []
        )
    )


overlay_two = BitemporalScenario("overlay_two", _overlay_two)
overlay_multiple = BitemporalScenario("overlay_multiple", _overlay_multiple)
multi_intersection_single_point = BitemporalScenario("multi_intersection_single_point", _multi_intersection_single_point)
multi_intersection_multiple_point = BitemporalScenario("multi_intersection_multiple_point", _multi_intersection_multiple_point)
multi_field = BitemporalScenario("multi_field", _multi_field)
extend_current_row = BitemporalScenario("extend_current_row", _extend_current_row)
extend_update = BitemporalScenario("extend_update", _extend_update)
no_change_with_intersection = BitemporalScenario("no_change_with_intersection", _no_change_with_intersection)
