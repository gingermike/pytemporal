from typing import Tuple, List

from pandas import to_datetime as pdt
from pytemporal import INFINITY_TIMESTAMP

from tests.scenarios.defaults import pdt_now, pd_max, BitemporalScenario


def _conflation() -> Tuple[List, List, Tuple]:
    """
    Basic conflation: Two consecutive segments with same values merge into one
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


def _conflation_three_segments() -> Tuple[List, List, Tuple]:
    """
    Three consecutive segments with same values merge into one
    """
    return (
        [],
        [
            [1234, "test", 10, 10, pdt("2020-01-01"), pdt("2020-04-01"), pdt_now, pd_max],
            [1234, "test", 10, 10, pdt("2020-04-01"), pdt("2020-07-01"), pdt_now, pd_max],
            [1234, "test", 10, 10, pdt("2020-07-01"), pdt("2020-10-01"), pdt_now, pd_max],
        ],
        (
            [],
            [
                [1234, "test", 10, 10, pdt("2020-01-01"), pdt("2020-10-01"), pdt_now, pd_max],
            ]
        )
    )


def _conflation_partial() -> Tuple[List, List, Tuple]:
    """
    Partial conflation: Some segments merge, others don't due to value changes
    """
    return (
        [],
        [
            # These two should merge (same values)
            [1234, "test", 5, 5, pdt("2020-01-01"), pdt("2020-06-01"), pdt_now, pd_max],
            [1234, "test", 5, 5, pdt("2020-06-01"), pdt("2020-12-01"), pdt_now, pd_max],
            # This one has different values, should NOT merge with above
            [1234, "test", 10, 10, pdt("2020-12-01"), pdt("2021-06-01"), pdt_now, pd_max],
            # These two should merge (same values, different from first group)
            [1234, "test", 10, 10, pdt("2021-06-01"), pdt("2021-12-01"), pdt_now, pd_max],
        ],
        (
            [],
            [
                [1234, "test", 5, 5, pdt("2020-01-01"), pdt("2020-12-01"), pdt_now, pd_max],
                [1234, "test", 10, 10, pdt("2020-12-01"), pdt("2021-12-01"), pdt_now, pd_max],
            ]
        )
    )


def _conflation_non_consecutive() -> Tuple[List, List, Tuple]:
    """
    Non-consecutive dates: Same ID and values but gaps in dates - should NOT conflate
    """
    return (
        [],
        [
            [1234, "test", 7, 7, pdt("2020-01-01"), pdt("2020-06-01"), pdt_now, pd_max],
            # Gap here: 2020-06-01 to 2020-07-01
            [1234, "test", 7, 7, pdt("2020-07-01"), pdt("2020-12-01"), pdt_now, pd_max],
        ],
        (
            [],
            [
                # Should remain as two separate records due to gap
                [1234, "test", 7, 7, pdt("2020-01-01"), pdt("2020-06-01"), pdt_now, pd_max],
                [1234, "test", 7, 7, pdt("2020-07-01"), pdt("2020-12-01"), pdt_now, pd_max],
            ]
        )
    )


def _conflation_mixed_ids() -> Tuple[List, List, Tuple]:
    """
    Mixed: Some IDs have conflation opportunities, others don't
    """
    return (
        [],
        [
            # ID 1234 - two segments that merge
            [1234, "field_a", 3, 3, pdt("2020-01-01"), pdt("2020-06-01"), pdt_now, pd_max],
            [1234, "field_a", 3, 3, pdt("2020-06-01"), pdt("2020-12-01"), pdt_now, pd_max],
            # ID 5678 - single segment, no merge opportunity
            [5678, "field_b", 8, 8, pdt("2020-01-01"), pdt("2020-12-01"), pdt_now, pd_max],
            # ID 9999 - three segments that all merge
            [9999, "field_c", 1, 2, pdt("2020-01-01"), pdt("2020-04-01"), pdt_now, pd_max],
            [9999, "field_c", 1, 2, pdt("2020-04-01"), pdt("2020-08-01"), pdt_now, pd_max],
            [9999, "field_c", 1, 2, pdt("2020-08-01"), pdt("2020-12-01"), pdt_now, pd_max],
        ],
        (
            [],
            [
                [1234, "field_a", 3, 3, pdt("2020-01-01"), pdt("2020-12-01"), pdt_now, pd_max],
                [5678, "field_b", 8, 8, pdt("2020-01-01"), pdt("2020-12-01"), pdt_now, pd_max],
                [9999, "field_c", 1, 2, pdt("2020-01-01"), pdt("2020-12-01"), pdt_now, pd_max],
            ]
        )
    )


def _conflation_unsorted_input() -> Tuple[List, List, Tuple]:
    """
    Unsorted input: Records out of order should still conflate correctly
    """
    return (
        [],
        [
            # Out of order: later segment comes first
            [1234, "test", 15, 20, pdt("2020-06-01"), pdt("2020-12-01"), pdt_now, pd_max],
            [1234, "test", 15, 20, pdt("2020-01-01"), pdt("2020-06-01"), pdt_now, pd_max],
            # Another ID, also out of order with three segments
            [5678, "test", 25, 30, pdt("2020-04-01"), pdt("2020-08-01"), pdt_now, pd_max],
            [5678, "test", 25, 30, pdt("2020-08-01"), pdt("2020-12-01"), pdt_now, pd_max],
            [5678, "test", 25, 30, pdt("2020-01-01"), pdt("2020-04-01"), pdt_now, pd_max],
        ],
        (
            [],
            [
                [1234, "test", 15, 20, pdt("2020-01-01"), pdt("2020-12-01"), pdt_now, pd_max],
                [5678, "test", 25, 30, pdt("2020-01-01"), pdt("2020-12-01"), pdt_now, pd_max],
            ]
        )
    )


def _conflation_with_current_state() -> Tuple[List, List, Tuple]:
    """
    Conflation with existing current state: Updates get conflated before processing
    """
    return (
        [
            # Existing record in current state
            [1234, "test", 100, 100, pdt("2019-01-01"), pdt("2020-01-01"), pdt_now, pd_max],
        ],
        [
            # Two consecutive updates that should conflate
            [1234, "test", 200, 200, pdt("2020-01-01"), pdt("2020-06-01"), pdt_now, pd_max],
            [1234, "test", 200, 200, pdt("2020-06-01"), pdt("2021-01-01"), pdt_now, pd_max],
        ],
        (
            [
                # Expire the old record
                [1234, "test", 100, 100, pdt("2019-01-01"), pdt("2020-01-01"), pdt_now, pd_max],
            ],
            [
                # Insert one conflated record (not two separate ones)
                [1234, "test", 200, 200, pdt("2020-01-01"), pdt("2021-01-01"), pdt_now, pd_max],
            ]
        )
    )


def _conflation_different_fields() -> Tuple[List, List, Tuple]:
    """
    Same ID but different field values - should NOT conflate across different fields
    """
    return (
        [],
        [
            # ID 1234 with field_a - these merge
            [1234, "field_a", 5, 10, pdt("2020-01-01"), pdt("2020-06-01"), pdt_now, pd_max],
            [1234, "field_a", 5, 10, pdt("2020-06-01"), pdt("2020-12-01"), pdt_now, pd_max],
            # ID 1234 with field_b - these merge separately
            [1234, "field_b", 7, 14, pdt("2020-01-01"), pdt("2020-06-01"), pdt_now, pd_max],
            [1234, "field_b", 7, 14, pdt("2020-06-01"), pdt("2020-12-01"), pdt_now, pd_max],
        ],
        (
            [],
            [
                [1234, "field_a", 5, 10, pdt("2020-01-01"), pdt("2020-12-01"), pdt_now, pd_max],
                [1234, "field_b", 7, 14, pdt("2020-01-01"), pdt("2020-12-01"), pdt_now, pd_max],
            ]
        )
    )


# Export all scenarios
conflation = BitemporalScenario("conflation", _conflation, "full_state")
conflation_three_segments = BitemporalScenario("conflation_three_segments", _conflation_three_segments, "full_state")
conflation_partial = BitemporalScenario("conflation_partial", _conflation_partial, "full_state")
conflation_non_consecutive = BitemporalScenario("conflation_non_consecutive", _conflation_non_consecutive, "full_state")
conflation_mixed_ids = BitemporalScenario("conflation_mixed_ids", _conflation_mixed_ids, "full_state")
conflation_unsorted_input = BitemporalScenario("conflation_unsorted_input", _conflation_unsorted_input, "full_state")
conflation_with_current_state = BitemporalScenario("conflation_with_current_state", _conflation_with_current_state, "full_state")
conflation_different_fields = BitemporalScenario("conflation_different_fields", _conflation_different_fields, "full_state")