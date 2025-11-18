from typing import Tuple, List

from pandas import to_datetime as pdt
from pytemporal import INFINITY_TIMESTAMP

from tests.scenarios.defaults import pdt_now, pd_max, BitemporalScenario


def _conflation() -> Tuple[List, List, Tuple]:
    """
    Overlaps two data points
    """
    return (
        [
        ],
        [
            [1234, "test", 2, 2, pdt("2020-03-01"), pdt("2020-11-01"), pdt_now, pd_max],
            [1234, "test", 2, 2, pdt("2020-11-01"), pdt("2021-11-01"), pdt_now, pd_max],
            [4567, "test_b", 1, 1, pdt("2020-03-01"), pdt("2020-11-01"), pdt_now, pd_max],
            [4567, "test_b", 1, 1, pdt("2020-11-01"), pdt("2021-11-01"), pdt_now, pd_max]
        ],
        (
            [
            ],
            [
                [1234, "test", 2, 2, pdt("2020-03-01"), pdt("2021-11-01"), pdt_now, pd_max],
                [4567, "test_b", 1, 1, pdt("2020-03-01"), pdt("2021-11-01"), pdt_now, pd_max]
            ]
        )
    )


conflation = BitemporalScenario("conflation", _conflation, "full_state")