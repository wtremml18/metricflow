from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Mapping, OrderedDict, Sequence

import pytest
from dbt_semantic_interfaces.protocols import SemanticManifest
from dbt_semantic_interfaces.test_utils import as_datetime

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.dataflow_plan import BaseOutput, ReadSqlSourceNode
from metricflow.dataset.semantic_model_adapter import SemanticModelDataSet
from metricflow.engine.metricflow_engine import MetricFlowEngine
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.plan_conversion.column_resolver import DunderColumnAssociationResolver
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow.query.query_parser import MetricFlowQueryParser
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.test.fixtures.id_fixtures import IdNumberSpace, patch_id_generators_helper
from metricflow.test.fixtures.model_fixtures import (
    _build_time_spine_source_node,
    _data_set_to_read_nodes,
    _data_set_to_source_nodes,
    create_data_sets,
    load_semantic_manifest,
)
from metricflow.test.time.configurable_time_source import ConfigurableTimeSource

logger = logging.getLogger(__name__)


class SemanticManifestName(Enum):
    """Names of the semantic manifests used in testing. Listed under test/fixtures/semantic_manifest_yamls."""

    AMBIGUOUS_RESOLUTION_MANIFEST = "ambiguous_resolution_manifest"
    # Not included as it has intentional errors for running validations.
    # CONFIG_LINTER_MANIFEST = "config_linter_manifest"
    CYCLIC_JOIN_MANIFEST = "cyclic_join_manifest"
    DATA_WAREHOUSE_VALIDATION_MANIFEST = "data_warehouse_validation_manifest"
    EXTENDED_DATE_MANIFEST = "extended_date_manifest"
    JOIN_TYPES_MANIFEST = "join_types_manifest"
    MULTI_HOP_JOIN_MANIFEST = "multi_hop_join_manifest"
    PARTITIONED_MULTI_HOP_JOIN_MANIFEST = "partitioned_multi_hop_join_manifest"
    NON_SM_MANIFEST = "non_sm_manifest"
    SCD_MANIFEST = "scd_manifest"
    SIMPLE_MANIFEST = "simple_manifest"
    SIMPLE_MULTI_HOP_JOIN_MANIFEST = "simple_multi_hop_join_manifest"


@dataclass(frozen=True)
class MetricFlowEngineTestFixture:
    """Contains objects for testing the MF engine for a specific semantic manifest."""

    semantic_manifest: SemanticManifest
    semantic_manifest_lookup: SemanticManifestLookup
    column_association_resolver: ColumnAssociationResolver
    data_set_mapping: OrderedDict[str, SemanticModelDataSet]
    read_node_mapping: OrderedDict[str, ReadSqlSourceNode]
    source_nodes: Sequence[BaseOutput]
    dataflow_plan_builder: DataflowPlanBuilder
    dataflow_to_sql_converter: DataflowToSqlQueryPlanConverter
    query_parser: MetricFlowQueryParser
    metricflow_engine: MetricFlowEngine


@pytest.fixture(scope="session")
def mf_engine_test_fixture_mapping(
    template_mapping: Dict[str, str],
    sql_client: SqlClient,
) -> Mapping[SemanticManifestName, MetricFlowEngineTestFixture]:
    """Returns a mapping for all semantic manifests used in testing to the associated test fixture."""
    fixture_mapping: Dict[SemanticManifestName, MetricFlowEngineTestFixture] = {}

    with patch_id_generators_helper(IdNumberSpace.CONSISTENT_ID_REPOSITORY):
        for semantic_manifest_name in SemanticManifestName:
            try:
                build_result = load_semantic_manifest(semantic_manifest_name.value, template_mapping)
            except Exception as e:
                raise RuntimeError(f"Error while loading semantic manifest: {semantic_manifest_name}") from e

            semantic_manifest = build_result.semantic_manifest
            semantic_manifest_lookup = SemanticManifestLookup(semantic_manifest)
            data_set_mapping = create_data_sets(semantic_manifest_lookup)
            read_node_mapping = _data_set_to_read_nodes(data_set_mapping)
            source_nodes = _data_set_to_source_nodes(semantic_manifest_lookup, data_set_mapping)
            time_spine_source_node = _build_time_spine_source_node(semantic_manifest_lookup)
            column_association_resolver = DunderColumnAssociationResolver(semantic_manifest_lookup)
            node_output_resolver = DataflowPlanNodeOutputDataSetResolver(
                column_association_resolver=column_association_resolver,
                semantic_manifest_lookup=semantic_manifest_lookup,
            )
            query_parser = MetricFlowQueryParser(semantic_manifest_lookup=semantic_manifest_lookup)
            fixture_mapping[semantic_manifest_name] = MetricFlowEngineTestFixture(
                semantic_manifest=semantic_manifest,
                semantic_manifest_lookup=semantic_manifest_lookup,
                column_association_resolver=column_association_resolver,
                data_set_mapping=data_set_mapping,
                read_node_mapping=read_node_mapping,
                source_nodes=source_nodes,
                dataflow_plan_builder=DataflowPlanBuilder(
                    source_nodes=source_nodes,
                    read_nodes=tuple(read_node_mapping.values()),
                    time_spine_source_node=time_spine_source_node,
                    semantic_manifest_lookup=semantic_manifest_lookup,
                    node_output_resolver=node_output_resolver,
                    column_association_resolver=column_association_resolver,
                ),
                dataflow_to_sql_converter=DataflowToSqlQueryPlanConverter(
                    column_association_resolver=column_association_resolver,
                    semantic_manifest_lookup=semantic_manifest_lookup,
                ),
                query_parser=query_parser,
                metricflow_engine=MetricFlowEngine(
                    semantic_manifest_lookup=semantic_manifest_lookup,
                    sql_client=sql_client,
                    time_source=ConfigurableTimeSource(as_datetime("2020-01-01")),
                    query_parser=query_parser,
                    column_association_resolver=column_association_resolver,
                ),
            )

        return fixture_mapping
