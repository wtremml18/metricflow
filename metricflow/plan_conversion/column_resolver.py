import logging
from typing import Optional, Tuple

from metricflow.aggregation_properties import AggregationState
from metricflow.column_assoc import (
    SingleColumnCorrelationKey,
    ColumnAssociation,
    CompositeColumnCorrelationKey,
)
from metricflow.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow.specs import (
    MetadataSpec,
    MetricSpec,
    MeasureSpec,
    DimensionSpec,
    TimeDimensionSpec,
    EntitySpec,
    ColumnAssociationResolver,
)
from metricflow.model.semantic_model import SemanticModel
from dbt_semantic_interfaces.objects.time_granularity import TimeGranularity

logger = logging.getLogger(__name__)


class DefaultColumnAssociationResolver(ColumnAssociationResolver):
    """Implements the ColumnAssociationResolver."""

    def __init__(self, semantic_model: SemanticModel) -> None:  # noqa: D
        self._semantic_model = semantic_model

    def resolve_metric_spec(self, metric_spec: MetricSpec) -> ColumnAssociation:  # noqa: D
        return ColumnAssociation(
            column_name=metric_spec.element_name,
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )

    def resolve_measure_spec(self, measure_spec: MeasureSpec) -> ColumnAssociation:  # noqa: D
        return ColumnAssociation(
            column_name=measure_spec.element_name,
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )

    def resolve_dimension_spec(self, dimension_spec: DimensionSpec) -> ColumnAssociation:  # noqa: D
        return ColumnAssociation(
            column_name=StructuredLinkableSpecName(
                entity_link_names=tuple(x.element_name for x in dimension_spec.entity_links),
                element_name=dimension_spec.element_name,
            ).qualified_name,
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )

    def resolve_time_dimension_spec(  # noqa: D
        self, time_dimension_spec: TimeDimensionSpec, aggregation_state: Optional[AggregationState] = None
    ) -> ColumnAssociation:
        if time_dimension_spec.time_granularity == TimeGranularity.DAY:
            column_name = StructuredLinkableSpecName(
                entity_link_names=tuple(x.element_name for x in time_dimension_spec.entity_links),
                element_name=time_dimension_spec.element_name,
            ).qualified_name
        else:
            column_name = StructuredLinkableSpecName(
                entity_link_names=tuple(x.element_name for x in time_dimension_spec.entity_links),
                element_name=time_dimension_spec.element_name,
                time_granularity=time_dimension_spec.time_granularity,
            ).qualified_name

        return ColumnAssociation(
            column_name=column_name + (f"__{aggregation_state.value.lower()}" if aggregation_state else ""),
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )

    def resolve_entity_spec(self, entity_spec: EntitySpec) -> Tuple[ColumnAssociation, ...]:  # noqa: D
        sub_entity_references = []
        for data_source in self._semantic_model.user_configured_model.data_sources:
            for entity in data_source.identifiers:
                if entity.reference.element_name == entity_spec.element_name:
                    sub_entity_references = [sub_entity.reference for sub_entity in entity.entities]
                    break

        # composite identifier case
        if len(sub_entity_references) != 0:
            column_associations: Tuple[ColumnAssociation, ...] = ()
            for sub_entity_reference in sub_entity_references:
                if sub_entity_reference is not None:
                    sub_entity_name = f"{entity_spec.element_name}___{sub_entity_reference.element_name}"
                    sub_identifier = StructuredLinkableSpecName(
                        entity_link_names=tuple(x.element_name for x in entity_spec.entity_links),
                        element_name=sub_entity_name,
                    ).qualified_name
                    column_associations += (
                        ColumnAssociation(
                            column_name=sub_identifier,
                            composite_column_correlation_key=CompositeColumnCorrelationKey(
                                sub_identifier=StructuredLinkableSpecName(
                                    entity_link_names=(),
                                    element_name=sub_entity_name,
                                ).qualified_name
                            ),
                        ),
                    )
            return column_associations

        return (
            ColumnAssociation(
                column_name=StructuredLinkableSpecName(
                    entity_link_names=tuple(x.element_name for x in entity_spec.entity_links),
                    element_name=entity_spec.element_name,
                ).qualified_name,
                single_column_correlation_key=SingleColumnCorrelationKey(),
            ),
        )

    def resolve_metadata_spec(self, metadata_spec: MetadataSpec) -> ColumnAssociation:  # noqa: D
        return ColumnAssociation(
            column_name=metadata_spec.element_name,
            single_column_correlation_key=SingleColumnCorrelationKey(),
        )
