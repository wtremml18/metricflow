from __future__ import annotations

import logging
from dataclasses import dataclass

from dbt_semantic_interfaces.dataclass_serialization import SerializableDataclass
from dbt_semantic_interfaces.objects.constraints.filter_renderer import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    FilterFunctionCallRenderer,
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.objects.constraints.filter_renderer import FilterRenderer
from dbt_semantic_interfaces.objects.filters.where_filter import WhereFilter
from metricflow.specs import (
    ColumnAssociationResolver,
    LinkableSpecSet,
    TimeDimensionSpec,
    DimensionSpec,
    EntitySpec,
)
from metricflow.sql.sql_bind_parameters import SqlBindParameters

logger = logging.getLogger(__name__)


class WhereFilterResolutionException(Exception):  # noqa: D
    pass


def convert_to_dimension_spec(parameter_set: DimensionCallParameterSet) -> DimensionSpec:  # noqa: D
    return DimensionSpec(
        element_name=parameter_set.dimension_reference.element_name,
        entity_links=parameter_set.entity_path,
    )


def convert_to_time_dimension_spec(parameter_set: TimeDimensionCallParameterSet) -> TimeDimensionSpec:  # noqa: D
    return TimeDimensionSpec(
        element_name=parameter_set.time_dimension_reference.element_name,
        entity_links=parameter_set.entity_path,
        time_granularity=parameter_set.time_granularity,
    )


def convert_to_entity_spec(parameter_set: EntityCallParameterSet) -> EntitySpec:  # noqa: D
    return EntitySpec(
        element_name=parameter_set.entity_reference.element_name,
        entity_links=parameter_set.entity_path,
    )


@dataclass(frozen=True)
class ResolvedWhereFilter(SerializableDataclass):
    """Similar to the WhereFilter, but with the where_sql_template rendered and used elements extracted.

    For example:

    WhereFilter(where_sql_template="{{ dimension('country', entity_path=['listing']) }} == 'US'"))

    ->

    ResolvedWhereFilter(
        where_sql="listing__country == 'US'",
        bind_parameters: SqlBindParameters(),
        linkable_spec_set: LinkableSpecSet(
            dimension_specs=(
                DimensionSpec(
                    element_name='country',
                    entity_links=('listing',),
            ),
        )
    )
    """

    where_sql: str
    bind_parameters: SqlBindParameters
    linkable_spec_set: LinkableSpecSet

    @staticmethod
    def create_from_where_filter(  # noqa: D
        where_filter: WhereFilter,
        column_association_resolver: ColumnAssociationResolver,
        bind_parameters: SqlBindParameters = SqlBindParameters(),
    ) -> ResolvedWhereFilter:
        class _CallRenderer(FilterFunctionCallRenderer):  # noqa: D
            def render_dimension_call(self, dimension_call_parameter_set: DimensionCallParameterSet) -> str:  # noqa: D
                return column_association_resolver.resolve_dimension_spec(
                    convert_to_dimension_spec(dimension_call_parameter_set)
                ).column_name

            def render_time_dimension_call(  # noqa: D
                self, time_dimension_call_parameter_set: TimeDimensionCallParameterSet
            ) -> str:
                return column_association_resolver.resolve_time_dimension_spec(
                    convert_to_time_dimension_spec(time_dimension_call_parameter_set)
                ).column_name

            def render_entity_call(self, entity_call_parameter_set: EntityCallParameterSet) -> str:  # noqa: D
                # TODO: Change when composite identifiers are remove.
                entity_column_associations = column_association_resolver.resolve_entity_spec(
                    convert_to_entity_spec(entity_call_parameter_set)
                )

                if len(entity_column_associations) == 0:
                    raise RuntimeError(f"No column associations were returned for {entity_call_parameter_set}")
                elif len(entity_column_associations) > 1:
                    raise WhereFilterResolutionException(
                        f"{entity_call_parameter_set} refers to a composite identifier. Since composite "
                        f"identifiers consist of multiple columns, they can't be used in a filter."
                    )

                return entity_column_associations[0].column_name

        where_sql = FilterRenderer.render(
            templated_filter_sql=where_filter.where_sql_template,
            call_renderer=_CallRenderer(),
        )

        parameter_sets = where_filter.call_parameter_sets
        # dict.fromkeys() does a dedupe while preserving order.
        return ResolvedWhereFilter(
            where_sql=where_sql,
            bind_parameters=bind_parameters,
            linkable_spec_set=LinkableSpecSet(
                dimension_specs=tuple(
                    convert_to_dimension_spec(parameter_set)
                    for parameter_set in dict.fromkeys(parameter_sets.dimension_call_parameter_sets)
                ),
                time_dimension_specs=tuple(
                    convert_to_time_dimension_spec(parameter_set)
                    for parameter_set in dict.fromkeys(parameter_sets.time_dimension_call_parameter_sets)
                ),
                entity_specs=tuple(
                    convert_to_entity_spec(parameter_set)
                    for parameter_set in dict.fromkeys(parameter_sets.entity_call_parameter_sets)
                ),
            ),
        )

    def combine(self, other: ResolvedWhereFilter) -> ResolvedWhereFilter:  # noqa: D
        return ResolvedWhereFilter(
            where_sql=f"({self.where_sql}) AND ({other.where_sql})",
            bind_parameters=self.bind_parameters.combine(other.bind_parameters),
            linkable_spec_set=LinkableSpecSet.merge([self.linkable_spec_set, other.linkable_spec_set]),
        )
