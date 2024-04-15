from __future__ import annotations

import logging

import pytest
from _pytest.fixtures import FixtureRequest
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.references import EntityReference, MeasureReference, MetricReference

from metricflow.model.semantics.linkable_element_properties import LinkableElementProperties
from metricflow.model.semantics.linkable_spec_resolver import SemanticModelJoinPath, SemanticModelJoinPathElement
from metricflow.model.semantics.metric_lookup import MetricLookup
from metricflow.model.semantics.semantic_model_lookup import SemanticModelLookup
from tests.fixtures.setup_fixtures import MetricFlowTestConfiguration
from tests.snapshot_utils import assert_linkable_element_set_snapshot_equal, assert_object_snapshot_equal

logger = logging.getLogger(__name__)


@pytest.fixture
def semantic_model_lookup(simple_semantic_manifest: SemanticManifest) -> SemanticModelLookup:  # noqa: D103
    return SemanticModelLookup(
        model=simple_semantic_manifest,
    )


@pytest.fixture
def metric_lookup(  # noqa: D103
    simple_semantic_manifest: SemanticManifest, semantic_model_lookup: SemanticModelLookup
) -> MetricLookup:
    return MetricLookup(semantic_manifest=simple_semantic_manifest, semantic_model_lookup=semantic_model_lookup)


def test_get_names(  # noqa: D103
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    semantic_model_lookup: SemanticModelLookup,
) -> None:
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj={
            "dimension_references": sorted([d.element_name for d in semantic_model_lookup.get_dimension_references()]),
            "measure_references": sorted([m.element_name for m in semantic_model_lookup.measure_references]),
            "entity_references": sorted([i.element_name for i in semantic_model_lookup.get_entity_references()]),
        },
    )


def test_get_elements(semantic_model_lookup: SemanticModelLookup) -> None:  # noqa: D103
    for dimension_reference in semantic_model_lookup.get_dimension_references():
        assert (
            semantic_model_lookup.get_dimension(dimension_reference=dimension_reference).reference
            == dimension_reference
        )
    for measure_reference in semantic_model_lookup.measure_references:
        measure_reference = MeasureReference(element_name=measure_reference.element_name)
        assert semantic_model_lookup.get_measure(measure_reference=measure_reference).reference == measure_reference


def test_get_semantic_models_for_measure(semantic_model_lookup: SemanticModelLookup) -> None:  # noqa: D103
    bookings_sources = semantic_model_lookup.get_semantic_models_for_measure(MeasureReference(element_name="bookings"))
    assert len(bookings_sources) == 1
    assert bookings_sources[0].name == "bookings_source"

    views_sources = semantic_model_lookup.get_semantic_models_for_measure(MeasureReference(element_name="views"))
    assert len(views_sources) == 1
    assert views_sources[0].name == "views_source"

    listings_sources = semantic_model_lookup.get_semantic_models_for_measure(MeasureReference(element_name="listings"))
    assert len(listings_sources) == 1
    assert listings_sources[0].name == "listings_latest"


def test_local_linked_elements_for_metric(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, metric_lookup: MetricLookup
) -> None:
    linkable_elements = metric_lookup.linkable_elements_for_metrics(
        [MetricReference(element_name="listings")],
        with_any_property=frozenset({LinkableElementProperties.LOCAL_LINKED}),
        without_any_property=frozenset({LinkableElementProperties.DERIVED_TIME_GRANULARITY}),
    )
    sorted_specs = sorted(linkable_elements.as_spec_set.as_tuple, key=lambda x: x.qualified_name)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=tuple(spec.qualified_name for spec in sorted_specs),
    )


def test_get_semantic_models_for_entity(semantic_model_lookup: SemanticModelLookup) -> None:  # noqa: D103
    entity_reference = EntityReference(element_name="user")
    linked_semantic_models = semantic_model_lookup.get_semantic_models_for_entity(entity_reference=entity_reference)
    assert len(linked_semantic_models) == 10


def test_linkable_elements_for_metrics(  # noqa: D103
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, metric_lookup: MetricLookup
) -> None:
    assert_linkable_element_set_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        set_id="result0",
        linkable_element_set=metric_lookup.linkable_elements_for_metrics(
            (MetricReference(element_name="views"),),
            without_any_property=frozenset(
                {
                    LinkableElementProperties.DERIVED_TIME_GRANULARITY,
                    LinkableElementProperties.METRIC_TIME,
                }
            ),
        ),
    )


def test_linkable_elements_for_measure(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    metric_lookup: MetricLookup,
) -> None:
    """Tests extracting linkable elements for a given measure input."""
    assert_linkable_element_set_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        set_id="result0",
        linkable_element_set=metric_lookup.linkable_elements_for_measure(
            measure_reference=MeasureReference(element_name="listings"),
        ),
    )


# TODO: test linkable metrics here
def test_linkable_elements_for_no_metrics_query(
    request: FixtureRequest,
    mf_test_configuration: MetricFlowTestConfiguration,
    metric_lookup: MetricLookup,
) -> None:
    """Tests extracting linkable elements for a dimension values query with no metrics."""
    linkable_elements = metric_lookup.linkable_elements_for_no_metrics_query(
        without_any_of={
            LinkableElementProperties.DERIVED_TIME_GRANULARITY,
        }
    )
    sorted_specs = sorted(linkable_elements.as_spec_set.as_tuple, key=lambda x: x.qualified_name)
    assert_object_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        obj_id="result0",
        obj=tuple(spec.qualified_name for spec in sorted_specs),
    )


def test_linkable_set_for_common_dimensions_in_different_models(
    request: FixtureRequest, mf_test_configuration: MetricFlowTestConfiguration, metric_lookup: MetricLookup
) -> None:
    """Tests case where a metric has dimensions with the same path.

    In this example, "ds" is defined in both "bookings_source" and "views_source".
    """
    assert_linkable_element_set_snapshot_equal(
        request=request,
        mf_test_configuration=mf_test_configuration,
        set_id="result0",
        linkable_element_set=metric_lookup.linkable_elements_for_metrics(
            (MetricReference(element_name="bookings_per_view"),),
            without_any_property=frozenset(
                {
                    LinkableElementProperties.DERIVED_TIME_GRANULARITY,
                }
            ),
        ),
    )


def test_get_valid_agg_time_dimensions_for_metric(  # noqa: D103
    metric_lookup: MetricLookup, semantic_model_lookup: SemanticModelLookup
) -> None:
    for metric_name in ["views", "listings", "bookings_per_view"]:
        metric_reference = MetricReference(metric_name)
        metric = metric_lookup.get_metric(metric_reference)
        metric_agg_time_dims = metric_lookup.get_valid_agg_time_dimensions_for_metric(metric_reference)
        measure_agg_time_dims = list(
            {
                semantic_model_lookup.get_agg_time_dimension_for_measure(measure.measure_reference)
                for measure in metric.input_measures
            }
        )
        if len(measure_agg_time_dims) == 1:
            for metric_agg_time_dim in metric_agg_time_dims:
                assert metric_agg_time_dim.reference == measure_agg_time_dims[0]
        else:
            assert len(metric_agg_time_dims) == 0


def test_get_agg_time_dimension_specs_for_measure(semantic_model_lookup: SemanticModelLookup) -> None:  # noqa: D103
    for measure_name in ["bookings", "views", "listings"]:
        measure_reference = MeasureReference(measure_name)
        agg_time_dim_specs = semantic_model_lookup.get_agg_time_dimension_specs_for_measure(measure_reference)
        agg_time_dim_reference = semantic_model_lookup.get_agg_time_dimension_for_measure(measure_reference)
        for spec in agg_time_dim_specs:
            assert spec.reference == agg_time_dim_reference


def test_linkable_metrics_for_measure(  # noqa: D103
    metric_lookup: MetricLookup,
    semantic_model_lookup: SemanticModelLookup,
) -> None:
    measure_reference = MeasureReference(element_name="listings")

    actual_metrics = {
        linkable_metric
        for linkable_metric_tuple in metric_lookup.linkable_elements_for_measure(
            measure_reference=measure_reference
        ).path_key_to_linkable_metrics.values()
        for linkable_metric in linkable_metric_tuple
    }

    semantic_models = semantic_model_lookup.get_semantic_models_for_measure(measure_reference)
    assert len(semantic_models) == 1
    measure_semantic_model = semantic_models[0]

    already_seen = set()

    expected_single_hop_metrics = [
        linkable_metric
        for linkable_metrics in metric_lookup._linkable_spec_resolver.get_joinable_metrics_for_semantic_model(
            semantic_model=measure_semantic_model
        ).path_key_to_linkable_metrics.values()
        for linkable_metric in linkable_metrics
    ]

    for expected_metric in expected_single_hop_metrics:
        assert expected_metric in actual_metrics
        assert expected_metric not in already_seen
        already_seen.add(expected_metric)
        actual_metrics.remove(expected_metric)

    expected_multi_hop_metrics = []
    for entity in measure_semantic_model.entities:
        next_semantic_models = metric_lookup._linkable_spec_resolver._entity_to_semantic_model[entity.name]
        for next_semantic_model in next_semantic_models:
            if next_semantic_model.name == measure_semantic_model.name:
                continue
            # TODO: use SemanticModelJoinPath.from_single_element when merged
            join_path = SemanticModelJoinPath(
                (
                    SemanticModelJoinPathElement(
                        semantic_model_reference=next_semantic_model.reference, join_on_entity=entity.reference
                    ),
                )
            )
            for linkable_metrics in metric_lookup._linkable_spec_resolver.get_joinable_metrics_for_semantic_model(
                semantic_model=next_semantic_model,
                using_join_path=join_path,
            ).path_key_to_linkable_metrics.values():
                for linkable_metric in linkable_metrics:
                    expected_multi_hop_metrics.append(linkable_metric)

    for expected_metric in expected_multi_hop_metrics:
        assert expected_metric in actual_metrics
        assert expected_metric not in already_seen
        already_seen.add(expected_metric)
        actual_metrics.remove(expected_metric)

    # Check that we didn't return any unexpected linkable metrics.
    assert len(actual_metrics) == 0, f"Didn't find linkable metrics: {actual_metrics}"
