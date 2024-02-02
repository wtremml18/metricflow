from __future__ import annotations

import logging
import os
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Mapping, Optional, Sequence

import pytest
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.parsing.dir_to_model import (
    SemanticManifestBuildResult,
    parse_directory_of_yaml_files_to_semantic_manifest,
)
from dbt_semantic_interfaces.protocols import SemanticManifest, SemanticModel
from dbt_semantic_interfaces.test_utils import as_datetime
from dbt_semantic_interfaces.validations.semantic_manifest_validator import SemanticManifestValidator

from metricflow.dataflow.builder.dataflow_plan_builder import DataflowPlanBuilder
from metricflow.dataflow.builder.node_data_set import DataflowPlanNodeOutputDataSetResolver
from metricflow.dataflow.builder.source_node import SourceNodeBuilder
from metricflow.dataflow.dataflow_plan import BaseOutput, MetricTimeDimensionTransformNode, ReadSqlSourceNode
from metricflow.dataset.convert_semantic_model import SemanticModelToDataSetConverter
from metricflow.dataset.semantic_model_adapter import SemanticModelDataSet
from metricflow.engine.metricflow_engine import MetricFlowEngine
from metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from metricflow.plan_conversion.column_resolver import DunderColumnAssociationResolver
from metricflow.plan_conversion.dataflow_to_sql import DataflowToSqlQueryPlanConverter
from metricflow.protocols.sql_client import SqlClient
from metricflow.query.query_parser import MetricFlowQueryParser
from metricflow.specs.column_assoc import ColumnAssociationResolver
from metricflow.test.fixtures.id_fixtures import IdNumberSpace, patch_id_generators_helper
from metricflow.test.fixtures.setup_fixtures import MetricFlowTestSessionState
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


def _data_set_to_read_nodes(data_sets: OrderedDict[str, SemanticModelDataSet]) -> OrderedDict[str, ReadSqlSourceNode]:
    """Return a mapping from the name of the semantic model to the dataflow plan node that reads from it."""
    return_dict: OrderedDict[str, ReadSqlSourceNode] = OrderedDict()
    for semantic_model_name, data_set in data_sets.items():
        return_dict[semantic_model_name] = ReadSqlSourceNode(data_set)
        logger.debug(
            f"For semantic model {semantic_model_name}, creating node_id {return_dict[semantic_model_name].node_id}"
        )

    return return_dict


def _data_set_to_source_nodes(
    semantic_manifest_lookup: SemanticManifestLookup, data_sets: OrderedDict[str, SemanticModelDataSet]
) -> Sequence[BaseOutput]:
    source_node_builder = SourceNodeBuilder(semantic_manifest_lookup)
    return source_node_builder.create_from_data_sets(list(data_sets.values()))


def _build_time_spine_source_node(semantic_manifest_lookup: SemanticManifestLookup) -> MetricTimeDimensionTransformNode:
    return SourceNodeBuilder.build_time_spine_source_node(
        time_spine_source=semantic_manifest_lookup.time_spine_source,
        data_set_converter=SemanticModelToDataSetConverter(
            column_association_resolver=DunderColumnAssociationResolver(semantic_manifest_lookup)
        ),
    )


def create_data_sets(
    semantic_manifest_lookup: SemanticManifestLookup,
) -> OrderedDict[str, SemanticModelDataSet]:
    """Convert the SemanticModels in the model to SqlDataSets.

    Key is the name of the semantic model, value is the associated data set.
    """
    # Use ordered dict and sort by name to get consistency when running tests.
    data_sets = OrderedDict()
    semantic_models: Sequence[SemanticModel] = semantic_manifest_lookup.semantic_manifest.semantic_models
    semantic_models = sorted(semantic_models, key=lambda x: x.name)

    converter = SemanticModelToDataSetConverter(
        column_association_resolver=DunderColumnAssociationResolver(semantic_manifest_lookup)
    )

    for semantic_model in semantic_models:
        data_sets[semantic_model.name] = converter.create_sql_source_data_set(semantic_model)

    return data_sets


def load_semantic_manifest(
    relative_manifest_path: str,
    template_mapping: Optional[Dict[str, str]] = None,
) -> SemanticManifestBuildResult:
    """Reads the manifest YAMLs from the standard location, applies transformations, runs validations."""
    yaml_file_directory = os.path.join(os.path.dirname(__file__), f"semantic_manifest_yamls/{relative_manifest_path}")
    build_result = parse_directory_of_yaml_files_to_semantic_manifest(
        yaml_file_directory, template_mapping=template_mapping
    )
    validator = SemanticManifestValidator[PydanticSemanticManifest]()
    validator.checked_validations(build_result.semantic_manifest)
    return build_result


@pytest.fixture(scope="session")
def template_mapping(mf_test_session_state: MetricFlowTestSessionState) -> Dict[str, str]:
    """Mapping for template variables in the model YAML files."""
    return {"source_schema": mf_test_session_state.mf_source_schema}
