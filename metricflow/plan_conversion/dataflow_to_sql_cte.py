from __future__ import annotations

import logging
from typing import FrozenSet, List, Optional, Sequence

from metricflow_semantics.dag.id_prefix import StaticIdPrefix
from metricflow_semantics.dag.sequential_id import SequentialIdGenerator
from metricflow_semantics.sql.sql_table import SqlTable

from metricflow.dataflow.dataflow_plan import DataflowPlanNode, DataflowPlanNodeVisitor
from metricflow.dataflow.nodes.add_generated_uuid import AddGeneratedUuidColumnNode
from metricflow.dataflow.nodes.aggregate_measures import AggregateMeasuresNode
from metricflow.dataflow.nodes.combine_aggregated_outputs import CombineAggregatedOutputsNode
from metricflow.dataflow.nodes.compute_metrics import ComputeMetricsNode
from metricflow.dataflow.nodes.constrain_time import ConstrainTimeRangeNode
from metricflow.dataflow.nodes.filter_elements import FilterElementsNode
from metricflow.dataflow.nodes.join_conversion_events import JoinConversionEventsNode
from metricflow.dataflow.nodes.join_over_time import JoinOverTimeRangeNode
from metricflow.dataflow.nodes.join_to_base import JoinOnEntitiesNode
from metricflow.dataflow.nodes.join_to_custom_granularity import JoinToCustomGranularityNode
from metricflow.dataflow.nodes.join_to_time_spine import JoinToTimeSpineNode
from metricflow.dataflow.nodes.metric_time_transform import MetricTimeDimensionTransformNode
from metricflow.dataflow.nodes.min_max import MinMaxNode
from metricflow.dataflow.nodes.order_by_limit import OrderByLimitNode
from metricflow.dataflow.nodes.read_sql_source import ReadSqlSourceNode
from metricflow.dataflow.nodes.semi_additive_join import SemiAdditiveJoinNode
from metricflow.dataflow.nodes.where_filter import WhereConstraintNode
from metricflow.dataflow.nodes.window_reaggregation_node import WindowReaggregationNode
from metricflow.dataflow.nodes.write_to_data_table import WriteToResultDataTableNode
from metricflow.dataflow.nodes.write_to_table import WriteToResultTableNode
from metricflow.dataset.sql_dataset import SqlDataSet
from metricflow.plan_conversion.dataflow_to_sql_subquery import DataflowNodeToSqlSubqueryVisitor
from metricflow.sql.sql_plan import (
    SqlCteNode,
    SqlTableNode,
)

logger = logging.getLogger(__name__)


class DataflowNodeToSqlCteVisitor(DataflowPlanNodeVisitor[SqlDataSet]):
    def __init__(  # noqa: D105
        self,
        dataflow_to_sql_subquery: DataflowNodeToSqlSubqueryVisitor,
        nodes_to_convert_to_cte: FrozenSet[DataflowPlanNode],
    ) -> None:
        self._dataflow_to_sql_subquery = dataflow_to_sql_subquery
        self._nodes_to_convert_to_cte = nodes_to_convert_to_cte
        self._generated_cte_nodes: List[SqlCteNode] = []
        self._visit_start_node: Optional[DataflowPlanNode] = None

    def generated_cte_nodes(self) -> Sequence[SqlCteNode]:
        """Returns the CTE nodes that have been generated"""
        return self._generated_cte_nodes

    def convert(self, node: DataflowPlanNode) -> SqlDataSet:
        self._generated_cte_nodes = []
        self._visit_start_node = node
        sql_data_set = node.accept(self)

    def _get_next_cte_alias(self) -> str:
        return SequentialIdGenerator.create_next_id(StaticIdPrefix.CTE).str_value

    def _default_handler(self, node: DataflowPlanNode) -> SqlDataSet:
        default_sql_dataset = node.accept(self._dataflow_to_sql_subquery)

        if node == self._visit_start_node:
            return SqlDataSet(instance_set=default_sql_dataset.instance_set, sql_node=default_sql_dataset.sql_node)

        if node not in self._nodes_to_convert_to_cte:
            return default_sql_dataset

        cte_alias = self._get_next_cte_alias()
        cte_node = SqlCteNode.create(
            select_statement=default_sql_dataset.sql_node,
            cte_alias=cte_alias,
        )
        self._generated_cte_nodes.append(cte_node)

        return SqlDataSet(
            instance_set=default_sql_dataset.instance_set,
            sql_node=SqlTableNode.create(SqlTable(schema_name=None, table_name=cte_alias)),
        )

    def visit_source_node(self, node: ReadSqlSourceNode) -> SqlDataSet:
        return self._default_handler(node)

    def visit_join_on_entities_node(self, node: JoinOnEntitiesNode) -> SqlDataSet:
        return self._default_handler(node)

    def visit_aggregate_measures_node(self, node: AggregateMeasuresNode) -> SqlDataSet:
        return self._default_handler(node)

    def visit_compute_metrics_node(self, node: ComputeMetricsNode) -> SqlDataSet:
        return self._default_handler(node)

    def visit_window_reaggregation_node(self, node: WindowReaggregationNode) -> SqlDataSet:
        return self._default_handler(node)

    def visit_order_by_limit_node(self, node: OrderByLimitNode) -> SqlDataSet:
        return self._default_handler(node)

    def visit_where_constraint_node(self, node: WhereConstraintNode) -> SqlDataSet:
        return self._default_handler(node)

    def visit_write_to_result_data_table_node(self, node: WriteToResultDataTableNode) -> SqlDataSet:
        return self._default_handler(node)

    def visit_write_to_result_table_node(self, node: WriteToResultTableNode) -> SqlDataSet:
        return self._default_handler(node)

    def visit_filter_elements_node(self, node: FilterElementsNode) -> SqlDataSet:
        return self._default_handler(node)

    def visit_combine_aggregated_outputs_node(self, node: CombineAggregatedOutputsNode) -> SqlDataSet:
        return self._default_handler(node)

    def visit_constrain_time_range_node(self, node: ConstrainTimeRangeNode) -> SqlDataSet:
        return self._default_handler(node)

    def visit_join_over_time_range_node(self, node: JoinOverTimeRangeNode) -> SqlDataSet:
        return self._default_handler(node)

    def visit_semi_additive_join_node(self, node: SemiAdditiveJoinNode) -> SqlDataSet:
        return self._default_handler(node)

    def visit_metric_time_dimension_transform_node(self, node: MetricTimeDimensionTransformNode) -> SqlDataSet:
        return self._default_handler(node)

    def visit_join_to_time_spine_node(self, node: JoinToTimeSpineNode) -> SqlDataSet:
        return self._default_handler(node)

    def visit_min_max_node(self, node: MinMaxNode) -> SqlDataSet:
        return self._default_handler(node)

    def visit_add_generated_uuid_column_node(self, node: AddGeneratedUuidColumnNode) -> SqlDataSet:
        return self._default_handler(node)

    def visit_join_conversion_events_node(self, node: JoinConversionEventsNode) -> SqlDataSet:
        return self._default_handler(node)

    def visit_join_to_custom_granularity_node(self, node: JoinToCustomGranularityNode) -> SqlDataSet:
        return self._default_handler(node)
