from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.naming.dundered import StructuredDunderedName
from dbt_semantic_interfaces.parsing.text_input.ti_description import ObjectBuilderItemDescription, QueryItemType
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums import DatePart, TimeGranularity


@dataclass(frozen=True)
class QueryableItemDescription:
    """A description of a queryable item in a Metricflow query. e.g. metric or a dimension.

    Different from `ObjectBuilderItemDescription` in that the fields are objects and the entity links have been
    standardized to follow the convention in MF.
    """

    item_type: Optional[QueryItemType]
    element_name: str
    entity_links: Tuple[EntityReference, ...]
    time_granularity: Optional[TimeGranularity]
    date_part: Optional[DatePart]
    group_by_for_metric_item: Optional[Tuple[EntityReference, ...]]
    descending: Optional[bool]

    @staticmethod
    def create_from_object_builder_description(
        object_builder_description: ObjectBuilderItemDescription,
    ) -> QueryableItemDescription:
        """Create from the DSI description."""
        structured_item_name = StructuredDunderedName.parse_name(object_builder_description.item_name)
        time_granularity: Optional[TimeGranularity] = None

        if structured_item_name.time_granularity is not None:
            time_granularity = structured_item_name.time_granularity

        if object_builder_description.time_granularity_name is not None:
            time_granularity = TimeGranularity(object_builder_description.time_granularity_name)

        date_part: Optional[DatePart] = None
        if object_builder_description.date_part_name is not None:
            date_part = DatePart(object_builder_description.date_part_name)

        entity_links = (
            tuple(EntityReference(entity_link) for entity_link in object_builder_description.entity_path)
            + structured_item_name.entity_links
        )
        item_type = object_builder_description.item_type
        group_by_for_metric_item: Optional[Tuple[EntityReference, ...]] = None

        # For metrics, the group-by corresponds to the entity links.
        if item_type is QueryItemType.METRIC:
            # Copied from `GroupByMetricPattern`.
            if len(object_builder_description.group_by_for_metric_item) != 1:
                raise RuntimeError(
                    "Currently only one group by item is allowed for Metric filters. "
                    "This should have been caught by validations."
                )
            metric_group_by = object_builder_description.group_by_for_metric_item[0]
            structured_metric_group_by = StructuredDunderedName.parse_name(metric_group_by)
            group_by_for_metric_item = structured_metric_group_by.entity_links + (
                EntityReference(structured_metric_group_by.element_name),
            )
        # If a dimension is specified with a grain / date part, it's considered a time dimension.
        elif item_type is QueryItemType.DIMENSION:
            if (
                object_builder_description.time_granularity_name is not None
                or object_builder_description.date_part_name is not None
            ):
                item_type = QueryItemType.TIME_DIMENSION
        elif item_type is QueryItemType.TIME_DIMENSION or item_type is QueryItemType.ENTITY:
            pass
        else:
            assert_values_exhausted(item_type)

        return QueryableItemDescription(
            item_type=item_type,
            element_name=structured_item_name.element_name,
            entity_links=entity_links,
            time_granularity=time_granularity,
            date_part=date_part,
            group_by_for_metric_item=group_by_for_metric_item,
            descending=object_builder_description.descending,
        )
