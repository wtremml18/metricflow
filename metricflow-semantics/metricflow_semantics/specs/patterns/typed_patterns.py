from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence

from dbt_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    MetricCallParameterSet,
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME

from typing_extensions import override

from metricflow_semantics.model.linkable_element_property import LinkableElementProperty
from metricflow_semantics.model.semantics.element_filter import LinkableElementFilter
from metricflow_semantics.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow_semantics.specs.instance_spec import InstanceSpec, LinkableInstanceSpec
from metricflow_semantics.specs.patterns.entity_link_pattern import (
    EntityLinkPattern,
    EntityLinkPatternParameterSet,
    ParameterSetField,
)
from metricflow_semantics.specs.spec_set import group_specs_by_type
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern
from metricflow_semantics.specs.time_dimension_spec import TimeDimensionSpec


@dataclass(frozen=True)
class DimensionPattern(EntityLinkPattern):
    """Similar to EntityPathPattern but only matches dimensions / time dimensions.

    Analogous pattern for Dimension() in the object builder naming scheme.
    """

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[LinkableInstanceSpec]:
        spec_set = group_specs_by_type(candidate_specs)
        filtered_specs: Sequence[LinkableInstanceSpec] = spec_set.dimension_specs + spec_set.time_dimension_specs
        return super().match(filtered_specs)

    @staticmethod
    def from_call_parameter_set(  # noqa: D102
        dimension_call_parameter_set: DimensionCallParameterSet,
    ) -> DimensionPattern:
        return DimensionPattern(
            parameter_set=EntityLinkPatternParameterSet.from_parameters(
                fields_to_compare=(
                    ParameterSetField.ELEMENT_NAME,
                    ParameterSetField.ENTITY_LINKS,
                ),
                element_name=dimension_call_parameter_set.dimension_reference.element_name,
                entity_links=dimension_call_parameter_set.entity_path,
            )
        )

    @property
    @override
    def element_pre_filter(self) -> LinkableElementFilter:
        return super().element_pre_filter.merge(
            LinkableElementFilter(without_any_of=frozenset({LinkableElementProperty.METRIC}))
        )


@dataclass(frozen=True)
class TimeDimensionPattern(EntityLinkPattern):
    """Similar to EntityPathPattern but only matches time dimensions.

    Analogous pattern for TimeDimension() in the object builder naming scheme.
    """

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[LinkableInstanceSpec]:
        spec_set = group_specs_by_type(candidate_specs)
        return super().match(spec_set.time_dimension_specs)

    @staticmethod
    def from_call_parameter_set(
        time_dimension_call_parameter_set: TimeDimensionCallParameterSet,
    ) -> TimeDimensionPattern:
        """Create the pattern that represents 'TimeDimension(...)' in the object builder naming scheme.

        For this pattern, A None value for the time grain matches any grain. However, a None value for the date part
        means that the date part has to be None. This follows the interface defined by the object builder naming scheme.
        """
        fields_to_compare: List[ParameterSetField] = [
            ParameterSetField.ELEMENT_NAME,
            ParameterSetField.ENTITY_LINKS,
            ParameterSetField.DATE_PART,
        ]

        if time_dimension_call_parameter_set.time_granularity_name is not None:
            fields_to_compare.append(ParameterSetField.TIME_GRANULARITY)

        return TimeDimensionPattern(
            parameter_set=EntityLinkPatternParameterSet.from_parameters(
                fields_to_compare=tuple(fields_to_compare),
                element_name=time_dimension_call_parameter_set.time_dimension_reference.element_name,
                entity_links=time_dimension_call_parameter_set.entity_path,
                time_granularity_name=time_dimension_call_parameter_set.time_granularity_name,
                date_part=time_dimension_call_parameter_set.date_part,
            )
        )

    @property
    @override
    def element_pre_filter(self) -> LinkableElementFilter:
        return super().element_pre_filter.merge(
            LinkableElementFilter(without_any_of=frozenset({LinkableElementProperty.METRIC}))
        )


@dataclass(frozen=True)
class EntityPattern(EntityLinkPattern):
    """Similar to EntityPathPattern but only matches entities.

    Analogous pattern for Entity() in the object builder naming scheme.
    """

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[LinkableInstanceSpec]:
        spec_set = group_specs_by_type(candidate_specs)
        return super().match(spec_set.entity_specs)

    @staticmethod
    def from_call_parameter_set(entity_call_parameter_set: EntityCallParameterSet) -> EntityPattern:  # noqa: D102
        return EntityPattern(
            parameter_set=EntityLinkPatternParameterSet.from_parameters(
                fields_to_compare=(
                    ParameterSetField.ELEMENT_NAME,
                    ParameterSetField.ENTITY_LINKS,
                ),
                element_name=entity_call_parameter_set.entity_reference.element_name,
                entity_links=entity_call_parameter_set.entity_path,
            )
        )

    @property
    @override
    def element_pre_filter(self) -> LinkableElementFilter:
        return LinkableElementFilter(without_any_of=frozenset({LinkableElementProperty.METRIC}))

@dataclass(frozen=True)
class GroupByMetricPattern(EntityLinkPattern):
    """
    A pattern that matches 'group by metric' usage, e.g. Metric("revenue", group_by=["country"]).

    Additionally, it can adopt a parent's metric_time dimension if none is explicitly given
    in metric_call_parameter_set.group_by.
    """

    @staticmethod
    def from_call_parameter_set(
        metric_call_parameter_set: MetricCallParameterSet,
        parent_time_spec: Optional[TimeDimensionSpec] = None,
    ) -> GroupByMetricPattern:
        """
        Creates a GroupByMetricPattern from a MetricCallParameterSet, optionally inheriting a
        parent's time dimension if the user has not explicitly included metric_time in group_by.

        Steps:
          1) Convert group_by into a list of EntityReferences.
          2) If parent_time_spec is present and 'metric_time' is missing from group_by, inject it.
          3) Build EntityLinkPatternParameterSet with fields E.g. (ELEMENT_NAME, ENTITY_LINKS, METRIC_SUBQUERY_ENTITY_LINKS).
          4) Return a GroupByMetricPattern that can match group-by metric specs accordingly.
        """
        # Step 1: Gather 'group_by' references from MetricCallParameterSet.
        group_by_refs = list(metric_call_parameter_set.group_by)

        # Step 2: If the parent provides a time dimension and the user did NOT specify metric_time, add it.
        has_metric_time = any(ref.element_name == METRIC_TIME_ELEMENT_NAME for ref in group_by_refs)
        if parent_time_spec and not has_metric_time:
            group_by_refs.append(EntityReference(element_name=METRIC_TIME_ELEMENT_NAME))

        # Build final link sets. In a more advanced scenario, you'd differentiate subquery links vs. outer links,
        # but we store them identically here for simplicity.
        entity_links: List[EntityReference] = []
        metric_subquery_entity_links = set()

        for ref in group_by_refs:
            entity_links.append(ref)
            metric_subquery_entity_links.add(ref)

        # Step 3: Construct the EntityLinkPatternParameterSet with the needed fields.
        pattern_param_set = EntityLinkPatternParameterSet.from_parameters(
            fields_to_compare=(
                ParameterSetField.ELEMENT_NAME,
                ParameterSetField.ENTITY_LINKS,
                ParameterSetField.METRIC_SUBQUERY_ENTITY_LINKS,
            ),
            element_name=metric_call_parameter_set.metric_reference.element_name,
            entity_links=tuple(entity_links),
            metric_subquery_entity_links=tuple(metric_subquery_entity_links),
        )

        # Step 4: Return our new GroupByMetricPattern.
        return GroupByMetricPattern(parameter_set=pattern_param_set)