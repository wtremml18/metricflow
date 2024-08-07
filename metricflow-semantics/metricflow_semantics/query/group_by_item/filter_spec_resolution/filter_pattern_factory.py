from __future__ import annotations

from abc import ABC, abstractmethod

from typing_extensions import override

from metricflow_semantics.naming.mf_query_item_description import QueryableItemDescription
from metricflow_semantics.specs.patterns.entity_link_pattern import EntityLinkPattern, EntityLinkPatternParameterSet
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern


class WhereFilterPatternFactory(ABC):
    """Interface that defines how spec patterns should be generated for the group-by-items specified in filters."""

    @abstractmethod
    def create_from_description(self, description: QueryableItemDescription) -> SpecPattern:  # noqa: D102
        raise NotImplementedError


class DefaultWhereFilterPatternFactory(WhereFilterPatternFactory):
    """Default implementation using patterns derived from EntityLinkPattern."""

    @override
    def create_from_description(self, item_description: QueryableItemDescription) -> SpecPattern:
        return EntityLinkPattern(EntityLinkPatternParameterSet.create_from_item_description(item_description))
