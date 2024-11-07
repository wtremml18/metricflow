from __future__ import annotations

from typing import FrozenSet, Mapping

from _pytest.fixtures import FixtureRequest
from metricflow_semantics.dag.mf_dag import DagId
from metricflow_semantics.mf_logging.lazy_formattable import LazyFormat
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.measure_spec import MeasureSpec
from metricflow_semantics.specs.spec_set import InstanceSpecSet
from metricflow_semantics.test_helpers.config_helpers import MetricFlowTestConfiguration
from metricflow_semantics.test_helpers.snapshot_helpers import assert_str_snapshot_equal

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.dataflow_plan import (
    DataflowPlanNode,
)
from metricflow.dataflow.nodes.filter_elements import FilterElementsNode
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.protocols.sql_client import SqlEngine
from metricflow.sql.optimizer.optimization_levels import SqlQueryOptimizationLevel
from metricflow.sql.render.common_dataflow_branches import find_common_branches
from metricflow.sql.render.sql_plan_renderer import DefaultSqlQueryPlanRenderer
from tests_metricflow.fixtures.manifest_fixtures import MetricFlowEngineTestFixture, SemanticManifestSetup


def convert_and_check(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    node: DataflowPlanNode,
    nodes_to_convert_to_cte: FrozenSet[DataflowPlanNode],
) -> None:
    """Convert the dataflow plan to SQL and compare with snapshots."""
    # Generate without CTEs
    sql_engine_type = SqlEngine.DUCKDB

    conversion_result = dataflow_to_sql_converter.convert_to_sql_query_plan(
        sql_engine_type=sql_engine_type,
        sql_query_plan_id=DagId.from_str("plan_0"),
        dataflow_plan_node=node,
        optimization_level=SqlQueryOptimizationLevel.O4,
        override_nodes_to_convert_to_cte=frozenset(),
    )
    sql_plan_without_cte = conversion_result.sql_plan
    renderer = DefaultSqlQueryPlanRenderer()
    initial_sql = renderer.render_sql_query_plan(sql_plan_without_cte).sql

    conversion_result = dataflow_to_sql_converter.convert_to_sql_query_plan(
        sql_engine_type=sql_engine_type,
        sql_query_plan_id=DagId.from_str("plan0_optimized"),
        dataflow_plan_node=node,
        optimization_level=SqlQueryOptimizationLevel.O4,
        override_nodes_to_convert_to_cte=nodes_to_convert_to_cte,
    )
    sql_plan_with_cte = conversion_result.sql_plan
    sql_with_cte = renderer.render_sql_query_plan(sql_plan_with_cte).sql

    assert_str_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        snapshot_id="result",
        snapshot_str=str(
            LazyFormat(
                initial_sql=initial_sql,
                sql_with_cte=sql_with_cte,
            )
        ),
    )


def test_use_cte_for_simple_dataflow_plan(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
    mf_engine_test_fixture_mapping: Mapping[SemanticManifestSetup, MetricFlowEngineTestFixture],
) -> None:
    """Test a simple case for generating a CTE for a specific dataflow plan node."""
    measure_spec = MeasureSpec(
        element_name="bookings",
    )
    source_node = mf_engine_test_fixture_mapping[SemanticManifestSetup.SIMPLE_MANIFEST].read_node_mapping[
        "bookings_source"
    ]
    filter_node = FilterElementsNode.create(
        parent_node=source_node, include_specs=InstanceSpecSet(measure_specs=(measure_spec,))
    )

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        node=filter_node,
        nodes_to_convert_to_cte=frozenset(
            [
                source_node,
            ]
        ),
    )


def test_cte_for_shared_metrics(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    column_association_resolver: ColumnAssociationResolver,
    dataflow_plan_builder: DataflowPlanBuilder,
    query_parser: MetricFlowQueryParser,
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter,
) -> None:
    """Check common branches in a query that uses derived metrics defined from metrics that are also in the query."""
    parse_result = query_parser.parse_and_validate_query(
        metric_names=("bookings", "bookings_per_booker"),
        group_by_names=("metric_time",),
    )
    dataflow_plan = dataflow_plan_builder.build_plan(parse_result.query_spec)
    common_nodes = find_common_branches(dataflow_plan.sink_node)

    convert_and_check(
        request=request,
        mf_test_configuration=mf_test_configuration,
        dataflow_to_sql_converter=dataflow_to_sql_converter,
        node=dataflow_plan.sink_node,
        nodes_to_convert_to_cte=frozenset(common_nodes),
    )
