from __future__ import annotations

import itertools
import logging
from typing import List, Optional, Sequence, Tuple, override

from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilterIntersection
from dbt_semantic_interfaces.parsing.text_input.description_renderer import QueryItemDescriptionRenderer
from dbt_semantic_interfaces.parsing.text_input.ti_description import ObjectBuilderItemDescription
from dbt_semantic_interfaces.parsing.text_input.ti_processor import ObjectBuilderTextProcessor
from dbt_semantic_interfaces.parsing.text_input.valid_method import ConfiguredValidMethodMapping
from dbt_semantic_interfaces.protocols import WhereFilter, WhereFilterIntersection

from metricflow_semantics.model.semantics.linkable_element import LinkableElement
from metricflow_semantics.naming.mf_query_item_description import QueryableItemDescription
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_location import WhereFilterLocation
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_spec_lookup import (
    FilterSpecResolutionLookUp,
    ResolvedSpecLookUpKey,
)
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.instance_spec import LinkableInstanceSpec
from metricflow_semantics.specs.linkable_spec_set import LinkableSpecSet
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameters

logger = logging.getLogger(__name__)


class RenderSqlTemplateException(Exception):  # noqa: D101
    pass


class WhereSpecFactory:
    """Renders the SQL template in the WhereFilter and converts it to a WhereFilterSpec."""

    def __init__(  # noqa: D107
        self,
        column_association_resolver: ColumnAssociationResolver,
        spec_resolution_lookup: FilterSpecResolutionLookUp,
    ) -> None:
        self._column_association_resolver = column_association_resolver
        self._spec_resolution_lookup = spec_resolution_lookup

    def create_from_where_filter(  # noqa: D102
        self,
        filter_location: WhereFilterLocation,
        where_filter: WhereFilter,
    ) -> WhereFilterSpec:
        return self.create_from_where_filter_intersection(
            filter_location=filter_location,
            filter_intersection=PydanticWhereFilterIntersection(where_filters=[where_filter]),
        )[0]

    def create_from_where_filter_intersection(  # noqa: D102
        self,
        filter_location: WhereFilterLocation,
        filter_intersection: Optional[WhereFilterIntersection],
    ) -> Sequence[WhereFilterSpec]:
        if filter_intersection is None:
            return ()

        filter_specs: List[WhereFilterSpec] = []
        text_processor = ObjectBuilderTextProcessor()

        for where_filter in filter_intersection.where_filters:
            renderer = _SpecQueryItemRenderer(
                where_filter_location=filter_location,
                column_association_resolver=self._column_association_resolver,
                spec_resolution_lookup=self._spec_resolution_lookup,
            )
            rendered_where_sql = text_processor.render_template(
                jinja_template=where_filter.where_sql_template,
                renderer=renderer,
                valid_method_mapping=ConfiguredValidMethodMapping.DEFAULT_MAPPING,
            )
            rendered_specs = tuple(result[0] for result in renderer.rendered_specs_to_elements)
            linkable_elements = tuple(
                itertools.chain.from_iterable(result[1] for result in renderer.rendered_specs_to_elements)
            )
            filter_specs.append(
                WhereFilterSpec(
                    where_sql=rendered_where_sql,
                    bind_parameters=SqlBindParameters(),
                    linkable_spec_set=LinkableSpecSet.create_from_specs(rendered_specs),
                    linkable_element_unions=tuple(linkable_element.as_union for linkable_element in linkable_elements),
                )
            )
        return filter_specs


class _SpecQueryItemRenderer(QueryItemDescriptionRenderer):
    def __init__(
        self,
        where_filter_location: WhereFilterLocation,
        column_association_resolver: ColumnAssociationResolver,
        spec_resolution_lookup: FilterSpecResolutionLookUp,
    ) -> None:
        self._where_filter_location = where_filter_location
        self._column_association_resolver = column_association_resolver
        self._spec_resolution_lookup = spec_resolution_lookup
        self._rendered_specs_to_elements: List[Tuple[LinkableInstanceSpec, Sequence[LinkableElement]]] = []

    @property
    def rendered_specs_to_elements(self) -> Sequence[Tuple[LinkableInstanceSpec, Sequence[LinkableElement]]]:
        """Returns specs that were recorded by record_rendered_spec()."""
        return self._rendered_specs_to_elements

    @override
    def render_description(self, item_description: ObjectBuilderItemDescription) -> str:
        key = ResolvedSpecLookUpKey(
            filter_location=self._where_filter_location,
            item_description=QueryableItemDescription.create_from_object_builder_description(item_description),
        )
        resolved_spec = self._spec_resolution_lookup.checked_resolved_spec(key)
        resolved_elements = self._spec_resolution_lookup.checked_resolved_linkable_elements(key)
        self._rendered_specs_to_elements.append((resolved_spec, resolved_elements))
        column_association = self._column_association_resolver.resolve_spec(resolved_spec)

        return column_association.column_name
