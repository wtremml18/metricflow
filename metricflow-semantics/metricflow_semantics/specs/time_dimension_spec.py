from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, List, Optional, Sequence, Tuple, Union

from dbt_semantic_interfaces.naming.keywords import METRIC_TIME_ELEMENT_NAME
from dbt_semantic_interfaces.references import DimensionReference, EntityReference, TimeDimensionReference
from dbt_semantic_interfaces.type_enums import DatePart, TimeGranularity
from typing_extensions import override

from metricflow_semantics.aggregation_properties import AggregationState
from metricflow_semantics.model.semantics.linkable_element import ElementPathKey, LinkableElementType
from metricflow_semantics.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow_semantics.specs.dimension_spec import DimensionSpec
from metricflow_semantics.specs.instance_spec import InstanceSpecVisitor
from metricflow_semantics.time.granularity import ExpandedTimeGranularity
from metricflow_semantics.visitor import VisitorOutputT


class TimeDimensionSpecField(Enum):
    """Fields of the time dimension spec.

    The value corresponds to the name of the field in the dataclass. This should contain all fields, but implementation
    is pending.
    """

    TIME_GRANULARITY = "time_granularity"


class TimeDimensionSpecComparisonKey:
    """A key that can be used for comparing / grouping time dimension specs.

    Useful for assessing if two time dimension specs are equal while ignoring specific attributes. Keys must have the
    same set of excluded attributes to be valid for comparison.

    This is useful for ambiguous group-by-item resolution where we want to select a time dimension regardless of the
    grain.
    """

    def __init__(self, source_spec: TimeDimensionSpec, exclude_fields: Sequence[TimeDimensionSpecField]) -> None:
        """Initializer.

        Args:
            source_spec: The spec that this key is based on.
            exclude_fields: The fields to ignore when determining equality.
        """
        self._excluded_fields = frozenset(exclude_fields)
        self._source_spec = source_spec

        # This is a list of field values of TimeDimensionSpec that we should use for comparison.
        spec_field_values_for_comparison: List[
            Union[str, Tuple[EntityReference, ...], TimeGranularity, Optional[DatePart]]
        ] = [self._source_spec.element_name, self._source_spec.entity_links]

        if TimeDimensionSpecField.TIME_GRANULARITY not in self._excluded_fields:
            spec_field_values_for_comparison.append(self._source_spec.time_granularity)

        spec_field_values_for_comparison.append(self._source_spec.date_part)

        self._spec_field_values_for_comparison = tuple(spec_field_values_for_comparison)

    @property
    def source_spec(self) -> TimeDimensionSpec:  # noqa: D102
        return self._source_spec

    @override
    def __eq__(self, other: Any) -> bool:  # type: ignore[misc]
        if not isinstance(other, TimeDimensionSpecComparisonKey):
            return False

        if self._excluded_fields != other._excluded_fields:
            return False

        return self._spec_field_values_for_comparison == other._spec_field_values_for_comparison

    @override
    def __hash__(self) -> int:
        return hash((self._excluded_fields, self._spec_field_values_for_comparison))


DEFAULT_TIME_GRANULARITY = TimeGranularity.DAY


@dataclass(frozen=True)
class TimeDimensionSpec(DimensionSpec):  # noqa: D101
    time_granularity: TimeGranularity = DEFAULT_TIME_GRANULARITY
    date_part: Optional[DatePart] = None

    # Used for semi-additive joins. Some more thought is needed, but this may be useful in InstanceSpec.
    aggregation_state: Optional[AggregationState] = None

    @property
    def without_first_entity_link(self) -> TimeDimensionSpec:  # noqa: D102
        assert len(self.entity_links) > 0, f"Spec does not have any entity links: {self}"
        return TimeDimensionSpec(
            element_name=self.element_name,
            entity_links=self.entity_links[1:],
            time_granularity=self.time_granularity,
            date_part=self.date_part,
        )

    @property
    def without_entity_links(self) -> TimeDimensionSpec:  # noqa: D102
        return TimeDimensionSpec(
            element_name=self.element_name,
            time_granularity=self.time_granularity,
            date_part=self.date_part,
            entity_links=(),
        )

    @property
    def reference(self) -> TimeDimensionReference:  # noqa: D102
        return TimeDimensionReference(element_name=self.element_name)

    @property
    def dimension_reference(self) -> DimensionReference:  # noqa: D102
        return DimensionReference(element_name=self.element_name)

    @property
    def qualified_name(self) -> str:  # noqa: D102
        return StructuredLinkableSpecName(
            entity_link_names=tuple(x.element_name for x in self.entity_links),
            element_name=self.element_name,
            time_granularity=self.time_granularity,
            date_part=self.date_part,
        ).qualified_name

    @property
    @override
    def element_path_key(self) -> ElementPathKey:
        return ElementPathKey(
            element_name=self.element_name,
            element_type=LinkableElementType.TIME_DIMENSION,
            entity_links=self.entity_links,
            time_granularity=ExpandedTimeGranularity.from_time_granularity(self.time_granularity),
            date_part=self.date_part,
        )

    @staticmethod
    def from_reference(reference: TimeDimensionReference) -> TimeDimensionSpec:
        """Initialize from a time dimension reference instance."""
        return TimeDimensionSpec(entity_links=(), element_name=reference.element_name)

    def accept(self, visitor: InstanceSpecVisitor[VisitorOutputT]) -> VisitorOutputT:  # noqa: D102
        return visitor.visit_time_dimension_spec(self)

    def with_grain(self, time_granularity: TimeGranularity) -> TimeDimensionSpec:  # noqa: D102
        return TimeDimensionSpec(
            element_name=self.element_name,
            entity_links=self.entity_links,
            time_granularity=time_granularity,
            date_part=self.date_part,
            aggregation_state=self.aggregation_state,
        )

    def with_aggregation_state(self, aggregation_state: AggregationState) -> TimeDimensionSpec:  # noqa: D102
        return TimeDimensionSpec(
            element_name=self.element_name,
            entity_links=self.entity_links,
            time_granularity=self.time_granularity,
            date_part=self.date_part,
            aggregation_state=aggregation_state,
        )

    def comparison_key(self, exclude_fields: Sequence[TimeDimensionSpecField] = ()) -> TimeDimensionSpecComparisonKey:
        """See TimeDimensionComparisonKey."""
        return TimeDimensionSpecComparisonKey(
            source_spec=self,
            exclude_fields=exclude_fields,
        )

    @staticmethod
    def generate_possible_specs_for_time_dimension(
        time_dimension_reference: TimeDimensionReference, entity_links: Tuple[EntityReference, ...]
    ) -> List[TimeDimensionSpec]:
        """Generate a list of time dimension specs with all combinations of granularity & date part."""
        time_dimension_specs: List[TimeDimensionSpec] = []
        for time_granularity in TimeGranularity:
            time_dimension_specs.append(
                TimeDimensionSpec(
                    element_name=time_dimension_reference.element_name,
                    entity_links=entity_links,
                    time_granularity=time_granularity,
                    date_part=None,
                )
            )
        for date_part in DatePart:
            for time_granularity in date_part.compatible_granularities:
                time_dimension_specs.append(
                    TimeDimensionSpec(
                        element_name=time_dimension_reference.element_name,
                        entity_links=entity_links,
                        time_granularity=time_granularity,
                        date_part=date_part,
                    )
                )
        return time_dimension_specs

    @property
    def is_metric_time(self) -> bool:  # noqa: D102
        return self.element_name == METRIC_TIME_ELEMENT_NAME
