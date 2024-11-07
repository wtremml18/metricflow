from __future__ import annotations

import logging

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration

from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
from tests_metricflow.integration.conftest import IntegrationTestHelpers

logger = logging.getLogger(__name__)


def test_explain(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, it_helpers: IntegrationTestHelpers
) -> None:
    # explain_result: MetricFlowExplainResult = it_helpers.mf_engine.explain(
    #     MetricFlowQueryRequest.create_with_random_request_id(
    #         metric_names=("bookings", "bookings_per_booker"),
    #         group_by_names=("metric_time",),
    #     )
    # )
    #
    # logger.info(LazyFormat("Explain query", sql=explain_result.rendered_sql.sql_query))

    it_helpers.mf_engine.query(
        MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=("bookings", "bookings_per_booker"),
            group_by_names=("metric_time",),
        )
    )

    # logger.info(LazyFormat("Explain query", result=query_result.rendered_sql.sql_query))
