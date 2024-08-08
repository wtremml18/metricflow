from __future__ import annotations

import logging
from functools import lru_cache
from typing import Optional

from dbt_semantic_interfaces.errors import InvalidQuerySyntax
from dbt_semantic_interfaces.parsing.text_input.ti_exceptions import QueryItemJinjaException
from dbt_semantic_interfaces.parsing.text_input.ti_processor import ObjectBuilderTextProcessor
from dbt_semantic_interfaces.parsing.text_input.valid_method import ConfiguredValidMethodMapping
from typing_extensions import override

from metricflow_semantics.naming.mf_query_item_description import QueryableItemDescription
from metricflow_semantics.naming.naming_scheme import QueryItemNamingScheme
from metricflow_semantics.naming.object_builder_str import ObjectBuilderNameConverter
from metricflow_semantics.specs.instance_spec import InstanceSpec
from metricflow_semantics.specs.patterns.entity_link_pattern import (
    EntityLinkPattern,
    EntityLinkPatternParameterSet,
)
from metricflow_semantics.specs.patterns.spec_pattern import SpecPattern

logger = logging.getLogger(__name__)


class ObjectBuilderNamingScheme(QueryItemNamingScheme):
    """A naming scheme using a builder syntax like Dimension('metric_time').grain('day')."""

    def __init__(self) -> None:  # noqa: D107
        self._object_builder_text_processor = ObjectBuilderTextProcessor()

    @lru_cache(maxsize=4096)
    def _get_description(self, input_str: str) -> Optional[QueryableItemDescription]:
        try:
            dsi_description = self._object_builder_text_processor.get_description(
                query_item_input=input_str,
                valid_method_mapping=ConfiguredValidMethodMapping.DEFAULT_MAPPING_FOR_ORDER_BY,
            )
            return QueryableItemDescription.create_from_object_builder_description(dsi_description)
        except (QueryItemJinjaException, InvalidQuerySyntax):
            return None

    def _get_pattern_parameters(self, input_str: str) -> Optional[EntityLinkPatternParameterSet]:
        item_description = self._get_description(input_str)
        if item_description is None:
            return None

        return EntityLinkPatternParameterSet.create_from_item_description(item_description)

    @override
    def input_str(self, instance_spec: InstanceSpec) -> Optional[str]:
        return ObjectBuilderNameConverter.input_str_from_spec(instance_spec)

    @override
    def spec_pattern(self, input_str: str) -> SpecPattern:
        item_description = self._get_description(input_str)
        pattern_parameters = self._get_pattern_parameters(input_str)

        if item_description is None or pattern_parameters is None:
            raise ValueError(
                f"The specified input {repr(input_str)} does not match the input described by the object builder "
                f"pattern."
            )

        return EntityLinkPattern(pattern_parameters)

    @override
    def input_str_follows_scheme(self, input_str: str) -> bool:
        return self._get_pattern_parameters(input_str) is not None

    @override
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id()={hex(id(self))})"
