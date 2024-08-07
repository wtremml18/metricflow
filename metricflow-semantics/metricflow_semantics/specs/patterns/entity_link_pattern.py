from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, List, Optional, Sequence, Tuple

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.parsing.text_input.ti_description import QueryItemType
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums import TimeGranularity
from dbt_semantic_interfaces.type_enums.date_part import DatePart
from more_itertools import is_sorted
from typing_extensions import override

from metricflow_semantics.mf_logging.pretty_print import mf_pformat_many
from metricflow_semantics.naming.mf_query_item_description import QueryableItemDescription
from metricflow_semantics.specs.instance_spec import InstanceSpec, LinkableInstanceSpec
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern
from metricflow_semantics.specs.spec_set import group_specs_by_type

logger = logging.getLogger(__name__)


class ParameterSetField(Enum):
    """The fields of the EntityLinkPatternParameterSet class used for matching in the EntityLinkPattern.

    Considering moving this to be a part of the specs module / classes.
    """

    ELEMENT_NAME = "element_name"
    ENTITY_LINKS = "entity_links"
    TIME_GRANULARITY = "time_granularity"
    DATE_PART = "date_part"
    METRIC_SUBQUERY_ENTITY_LINKS = "metric_subquery_entity_links"

    def __lt__(self, other: Any) -> bool:  # type: ignore[misc]
        """Allow for ordering so that a sequence of these can be consistently represented for test snapshots."""
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented


@dataclass(frozen=True)
class EntityLinkPatternParameterSet:
    """See EntityPathPattern for more details."""

    # Specify the field values to compare. None can't be used to signal "don't compare" because sometimes a pattern
    # needs to match a spec where the field is None. These should be sorted.
    fields_to_compare: Tuple[ParameterSetField, ...]
    element_types: Tuple[QueryItemType, ...]
    # The name of the element in the semantic model
    element_name: Optional[str] = None
    # The entities used for joining semantic models.
    entity_links: Optional[Tuple[EntityReference, ...]] = None
    # Properties of time dimensions to match.
    time_granularity: Optional[TimeGranularity] = None
    date_part: Optional[DatePart] = None
    metric_subquery_entity_links: Optional[Tuple[EntityReference, ...]] = None

    @staticmethod
    def from_parameters(  # noqa: D102
        fields_to_compare: Sequence[ParameterSetField],
        element_types: Optional[Sequence[QueryItemType]] = None,
        element_name: Optional[str] = None,
        entity_links: Optional[Sequence[EntityReference]] = None,
        time_granularity: Optional[TimeGranularity] = None,
        date_part: Optional[DatePart] = None,
        metric_subquery_entity_links: Optional[Tuple[EntityReference, ...]] = None,
    ) -> EntityLinkPatternParameterSet:
        return EntityLinkPatternParameterSet(
            element_types=tuple(sorted(set(element_types)))
            if element_types is not None
            else tuple(sorted(QueryItemType)),
            fields_to_compare=tuple(sorted(fields_to_compare)),
            element_name=element_name,
            entity_links=tuple(entity_links) if entity_links is not None else None,
            time_granularity=time_granularity,
            date_part=date_part,
            metric_subquery_entity_links=metric_subquery_entity_links,
        )

    @staticmethod
    def create_from_item_description(item_description: QueryableItemDescription) -> EntityLinkPatternParameterSet:
        """Create the parameter set that corresponds to the query-item description."""
        # Date part is always compared when available as the None value is meaningful.
        fields_to_compare = {
            ParameterSetField.ELEMENT_NAME,
            ParameterSetField.ENTITY_LINKS,
            ParameterSetField.DATE_PART,
        }
        item_type = item_description.item_type
        entity_links = item_description.entity_links
        metric_subquery_entity_links: Tuple[EntityReference, ...] = ()

        if item_type is None:
            element_types = tuple(QueryItemType)
        elif item_type is QueryItemType.DIMENSION:
            # A `Dimension()` can match a `TimeDimension()`
            element_types = (QueryItemType.DIMENSION, QueryItemType.TIME_DIMENSION)
            if item_description.time_granularity is not None:
                element_types = (QueryItemType.TIME_DIMENSION,)
                fields_to_compare.add(ParameterSetField.TIME_GRANULARITY)
            if item_description.date_part is not None:
                element_types = (QueryItemType.TIME_DIMENSION,)
        elif item_type is QueryItemType.TIME_DIMENSION:
            element_types = (QueryItemType.TIME_DIMENSION,)
            if item_description.time_granularity is not None:
                fields_to_compare.add(ParameterSetField.TIME_GRANULARITY)
        elif item_type is QueryItemType.ENTITY:
            element_types = (QueryItemType.ENTITY,)
        elif item_type is QueryItemType.METRIC:
            element_types = (QueryItemType.METRIC,)
            # Copied from `GroupByMetricPattern`.
            metric_subquery_entity_links = item_description.group_by_for_metric_item or ()
            # Temp: we don't have a parameter to specify the join path from the outer query to the metric subquery,
            # so just use the last entity. Will need to add another param for that later.
            entity_links = metric_subquery_entity_links[-1:]
            fields_to_compare.add(ParameterSetField.METRIC_SUBQUERY_ENTITY_LINKS)
        else:
            assert_values_exhausted(item_type)
        return EntityLinkPatternParameterSet.from_parameters(
            element_types=element_types,
            element_name=item_description.element_name,
            entity_links=entity_links,
            time_granularity=item_description.time_granularity,
            date_part=item_description.date_part,
            metric_subquery_entity_links=metric_subquery_entity_links,
            fields_to_compare=tuple(fields_to_compare),
        )

    def __post_init__(self) -> None:
        """Check that fields_to_compare is sorted so that patterns that do the same thing can be compared."""
        assert is_sorted(self.fields_to_compare)


@dataclass(frozen=True)
class EntityLinkPattern(SpecPattern):
    """A pattern that matches group-by-items using the entity-link-path specification.

    The entity link path specifies how a group-by-item for a metric query should be constructed. The group-by-item
    is obtained by joining the semantic model containing the measure to a semantic model containing the group-by-
    item using a specified entity. Additional semantic models can be joined using additional entities to obtain the
    group-by-item. The series of entities that are used form the entity path. Since the entity path does not specify
    which semantic models need to be used, additional resolution is done in later stages to generate the necessary SQL.

    The entity links that are specified is used as a suffix match.
    """

    parameter_set: EntityLinkPatternParameterSet

    @staticmethod
    def create_from_item_description(  # noqa: D102
        item_description: QueryableItemDescription,
    ) -> EntityLinkPattern:
        return EntityLinkPattern(EntityLinkPatternParameterSet.create_from_item_description(item_description))

    def _match_entity_links(
        self, entity_links: Sequence[EntityReference], candidate_specs: Sequence[LinkableInstanceSpec]
    ) -> Sequence[LinkableInstanceSpec]:
        num_links_to_check = len(entity_links)
        matching_specs: Sequence[LinkableInstanceSpec] = tuple(
            candidate_spec
            for candidate_spec in candidate_specs
            if (entity_links[-num_links_to_check:] == candidate_spec.entity_links[-num_links_to_check:])
        )

        if len(matching_specs) <= 1:
            return matching_specs

        # If multiple match, then return only the ones with the shortest entity link path. There could be multiple
        # e.g. booking__listing__country and listing__country will match with listing__country.
        shortest_entity_link_length = min(len(matching_spec.entity_links) for matching_spec in matching_specs)
        return tuple(spec for spec in matching_specs if len(spec.entity_links) == shortest_entity_link_length)

    @override
    def match(self, candidate_specs: Sequence[InstanceSpec]) -> Sequence[LinkableInstanceSpec]:
        # Checks that EntityLinkPatternParameterSetField is valid wrt to the parameter set.
        specs_by_type = group_specs_by_type(candidate_specs)
        element_types = self.parameter_set.element_types

        filtered_candidate_specs: List[LinkableInstanceSpec] = []
        for element_type in element_types:
            if element_type is QueryItemType.DIMENSION:
                filtered_candidate_specs.extend(specs_by_type.dimension_specs)
            elif element_type is QueryItemType.TIME_DIMENSION:
                filtered_candidate_specs.extend(specs_by_type.time_dimension_specs)
            elif element_type is QueryItemType.ENTITY:
                filtered_candidate_specs.extend(specs_by_type.entity_specs)
            elif element_type is QueryItemType.METRIC:
                filtered_candidate_specs.extend(specs_by_type.group_by_metric_specs)
            else:
                assert_values_exhausted(element_type)

        # Entity links could be a partial match, so it's handled separately.
        if ParameterSetField.ENTITY_LINKS in self.parameter_set.fields_to_compare:
            filtered_candidate_specs = list(
                self._match_entity_links(
                    entity_links=self.parameter_set.entity_links or (),
                    candidate_specs=filtered_candidate_specs,
                )
            )

        other_keys_to_check = set(
            field_to_compare.value for field_to_compare in self.parameter_set.fields_to_compare
        ).difference({ParameterSetField.ENTITY_LINKS.value})

        matching_specs: List[LinkableInstanceSpec] = []
        parameter_set_values = tuple(getattr(self.parameter_set, key_to_check) for key_to_check in other_keys_to_check)
        for spec in filtered_candidate_specs:
            spec_values = tuple(getattr(spec, key_to_check, None) for key_to_check in other_keys_to_check)
            if spec_values == parameter_set_values:
                matching_specs.append(spec)

        logger.error(
            mf_pformat_many(
                "Matched specs:",
                {
                    "parameter_set": self.parameter_set,
                    "candidate_specs": candidate_specs,
                    "matching_specs": matching_specs,
                },
            )
        )
        return matching_specs
