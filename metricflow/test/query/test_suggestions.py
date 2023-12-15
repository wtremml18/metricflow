from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest

from metricflow.query.query_exceptions import InvalidQueryException
from metricflow.query.query_parser import MetricFlowQueryParser
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
from metricflow.test.snapshot_utils import assert_object_snapshot_equal

logger = logging.getLogger(__name__)


def test_group_by_item_suggestion_for_simple_metric(  # noqa: D
        request: FixtureRequest,
        mf_test_session_state: MetricFlowTestSessionState,
        query_parser: MetricFlowQueryParser
) -> None:
    with pytest.raises(InvalidQueryException) as e:
        query_parser.parse_and_validate_query(metric_names=("bookings",), group_by_names=("booking__is_instan",))

    assert_object_snapshot_equal(
        request=request,
        mf_test_session_state=mf_test_session_state,
        obj_id="result_0",
        obj=str(e.value),
    )


def test_group_by_item_suggestion_for_multiple_metrics(  # noqa: D
        request: FixtureRequest,
        mf_test_session_state: MetricFlowTestSessionState,
        query_parser: MetricFlowQueryParser
) -> None:
    with pytest.raises(InvalidQueryException) as e:
        query_parser.parse_and_validate_query(metric_names=("bookings", "listings"), group_by_names=("country_latest",))

    logger.error(f"Output is:\n{str(e.value)}")

    # assert_object_snapshot_equal(
    #     request=request,
    #     mf_test_session_state=mf_test_session_state,
    #     obj_id="result_0",
    #     obj=str(e.value),
    # )

