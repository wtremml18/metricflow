from __future__ import annotations

from typing import Optional, Sequence

from metricflow.naming.naming_scheme import QueryItemNamingScheme
from metricflow.specs.patterns.spec_pattern import SpecPattern
from metricflow.specs.specs import InstanceSpec


class GroupByItemSuggestionGenerator:
    """Returns specs that partially match a spec pattern created from user input. Used for suggestions in errors.

    Since suggestions are needed for group-by-items specified in the query and in where filters, an optional candidate
    filter can be specified to limit suggestions to the ones valid for the entire query. For use with where filters,
    a candidate filter is not needed as any available spec at a resolution node can be used.
    """

    def __init__(
            self,
            naming_scheme: QueryItemNamingScheme,
            input_spec_pattern: SpecPattern,
            candidate_filter: Optional[SpecPattern]) -> None:  # noqa: D
        self._naming_scheme = naming_scheme
        self._input_spec_pattern = input_spec_pattern
        self._candidate_filter = candidate_filter

    def suggested_specs(
        self,
        candidate_specs: Sequence[InstanceSpec],
        max_suggestions: int = 6,
    ) -> Sequence[InstanceSpec]:
        """Return the best specs that match the given pattern from candidate_specs and match the candidate_filer."""
        if self._candidate_filter is not None:
            candidate_specs = self._candidate_filter.match(candidate_specs)

        return self._input_spec_pattern.partially_match(candidate_specs=candidate_specs, max_items=max_suggestions)
