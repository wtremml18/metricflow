from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Tuple

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted

from metricflow.sql.optimizer.column_pruner import SqlColumnPrunerOptimizer
from metricflow.sql.optimizer.rewriting_sub_query_reducer import SqlRewritingSubQueryReducer
from metricflow.sql.optimizer.sql_query_plan_optimizer import SqlQueryPlanOptimizer
from metricflow.sql.optimizer.sub_query_reducer import SqlSubQueryReducer
from metricflow.sql.optimizer.table_alias_simplifier import SqlTableAliasSimplifier


class SqlQueryOptimizationLevel(Enum):
    """Defines the level of query optimization and the associated optimizers to apply."""

    O0 = "O0"
    O1 = "O1"
    O2 = "O2"
    O3 = "O3"
    O4 = "O4"
    O5 = "O5"


@dataclass(frozen=True)
class SqlQueryGenerationOptionSet:
    """Describe the options for generate SQL from the dataflow plan."""

    optimizers: Tuple[SqlQueryPlanOptimizer, ...]
    use_cte: bool


class SqlQueryGenerationOptionLookup:
    """Defines the different SQL generation optimizers / options that should be used at each level."""

    @staticmethod
    def options_for_level(  # noqa: D102
        level: SqlQueryOptimizationLevel, use_column_alias_in_group_by: bool
    ) -> SqlQueryGenerationOptionSet:
        optimizers: Tuple[SqlQueryPlanOptimizer, ...] = ()
        use_cte = False
        if level is SqlQueryOptimizationLevel.O0:
            pass
        elif level is SqlQueryOptimizationLevel.O1:
            optimizers = (SqlTableAliasSimplifier(),)
        elif level is SqlQueryOptimizationLevel.O2:
            optimizers = (SqlColumnPrunerOptimizer(), SqlTableAliasSimplifier())
        elif level is SqlQueryOptimizationLevel.O3:
            optimizers = (SqlColumnPrunerOptimizer(), SqlSubQueryReducer(), SqlTableAliasSimplifier())
        elif level is SqlQueryOptimizationLevel.O4:
            optimizers = (
                SqlColumnPrunerOptimizer(),
                SqlRewritingSubQueryReducer(use_column_alias_in_group_bys=use_column_alias_in_group_by),
                SqlTableAliasSimplifier(),
            )
        elif level is SqlQueryOptimizationLevel.O5:
            optimizers = (
                SqlColumnPrunerOptimizer(),
                SqlRewritingSubQueryReducer(use_column_alias_in_group_bys=use_column_alias_in_group_by),
                SqlTableAliasSimplifier(),
            )
            use_cte = True
        else:
            assert_values_exhausted(level)

        return SqlQueryGenerationOptionSet(optimizers=optimizers, use_cte=use_cte)

    # @staticmethod
    # def optimizers_for_level(
    #     level: SqlQueryOptimizationLevel, use_column_alias_in_group_by: bool
    # ) -> Sequence[SqlQueryPlanOptimizer]:
    #     """Return the optimizers that should be applied (in order) for each level."""
    #     if level is SqlQueryOptimizationLevel.O0:
    #         return ()
    #     elif level is SqlQueryOptimizationLevel.O1:
    #         return (SqlTableAliasSimplifier(),)
    #     elif level is SqlQueryOptimizationLevel.O2:
    #         return (SqlColumnPrunerOptimizer(), SqlTableAliasSimplifier())
    #     elif level is SqlQueryOptimizationLevel.O3:
    #         return (SqlColumnPrunerOptimizer(), SqlSubQueryReducer(), SqlTableAliasSimplifier())
    #     elif level is SqlQueryOptimizationLevel.O4:
    #         return (
    #             SqlColumnPrunerOptimizer(),
    #             SqlRewritingSubQueryReducer(use_column_alias_in_group_bys=use_column_alias_in_group_by),
    #             SqlTableAliasSimplifier(),
    #         )
