from __future__ import annotations

from typing import List, Optional, Sequence

from dbt_semantic_interfaces.objects.common import Metadata
from dbt_semantic_interfaces.objects.elements.dimension import Dimension
from dbt_semantic_interfaces.objects.elements.identifier import Identifier
from dbt_semantic_interfaces.objects.elements.measure import Measure
from dbt_semantic_interfaces.objects.base import ModelWithMetadataParsing, HashableBaseModel
from dbt_semantic_interfaces.references import DataSourceReference, LinkableElementReference, MeasureReference
from metricflow.object_utils import ExtendedEnum


class DataSourceOrigin(ExtendedEnum):
    """Describes how data sources were created

    Impacts determination of validity and duration of storage
    """

    SOURCE = "source"  # "input" data sources
    DERIVED = "derived"  # generated by the semantic layer originating (perhaps indirectly) from sources


class MutabilityType(ExtendedEnum):
    """How data at the physical layer is expected to behave"""

    UNKNOWN = "UNKNOWN"
    IMMUTABLE = "IMMUTABLE"  # never changes
    APPEND_ONLY = "APPEND_ONLY"  # appends along an orderable column
    DS_APPEND_ONLY = "DS_APPEND_ONLY"  # appends along daily column
    FULL_MUTATION = "FULL_MUTATION"  # no guarantees, everything may change


class MutabilityTypeParams(HashableBaseModel):
    """Type params add additional context to mutability"""

    min: Optional[str]
    max: Optional[str]
    update_cron: Optional[str]
    along: Optional[str]


class Mutability(HashableBaseModel):
    """Describes the mutability properties of a data source"""

    type: MutabilityType
    type_params: Optional[MutabilityTypeParams]


class DataSource(HashableBaseModel, ModelWithMetadataParsing):
    """Describes a data source"""

    name: str
    description: Optional[str]
    sql_table: Optional[str]
    sql_query: Optional[str]
    dbt_model: Optional[str]

    identifiers: Sequence[Identifier] = []
    measures: Sequence[Measure] = []
    dimensions: Sequence[Dimension] = []

    mutability: Mutability = Mutability(type=MutabilityType.FULL_MUTATION)

    origin: DataSourceOrigin = DataSourceOrigin.SOURCE
    metadata: Optional[Metadata]

    @property
    def identifier_references(self) -> List[LinkableElementReference]:  # noqa: D
        return [i.reference for i in self.identifiers]

    @property
    def dimension_references(self) -> List[LinkableElementReference]:  # noqa: D
        return [i.reference for i in self.dimensions]

    @property
    def measure_references(self) -> List[MeasureReference]:  # noqa: D
        return [i.reference for i in self.measures]

    def get_measure(self, measure_reference: MeasureReference) -> Measure:  # noqa: D
        for measure in self.measures:
            if measure.reference == measure_reference:
                return measure

        raise ValueError(
            f"No dimension with name ({measure_reference.element_name}) in data source with name ({self.name})"
        )

    def get_dimension(self, dimension_reference: LinkableElementReference) -> Dimension:  # noqa: D
        for dim in self.dimensions:
            if dim.reference == dimension_reference:
                return dim

        raise ValueError(f"No dimension with name ({dimension_reference}) in data source with name ({self.name})")

    def get_identifier(self, identifier_reference: LinkableElementReference) -> Identifier:  # noqa: D
        for ident in self.identifiers:
            if ident.reference == identifier_reference:
                return ident

        raise ValueError(f"No identifier with name ({identifier_reference}) in data source with name ({self.name})")

    @property
    def has_validity_dimensions(self) -> bool:
        """Returns True if there are validity params set on one or more dimensions"""
        return any([dim.validity_params is not None for dim in self.dimensions])

    @property
    def validity_start_dimension(self) -> Optional[Dimension]:
        """Returns the validity window start dimension, if one is set"""
        validity_start_dims = [dim for dim in self.dimensions if dim.validity_params and dim.validity_params.is_start]
        if not validity_start_dims:
            return None
        assert (
            len(validity_start_dims) == 1
        ), "Found more than one validity start dimension. This should have been blocked in validation!"
        return validity_start_dims[0]

    @property
    def validity_end_dimension(self) -> Optional[Dimension]:
        """Returns the validity window end dimension, if one is set"""
        validity_end_dims = [dim for dim in self.dimensions if dim.validity_params and dim.validity_params.is_end]
        if not validity_end_dims:
            return None
        assert (
            len(validity_end_dims) == 1
        ), "Found more than one validity end dimension. This should have been blocked in validation!"
        return validity_end_dims[0]

    @property
    def partitions(self) -> List[Dimension]:  # noqa: D
        return [dim for dim in self.dimensions or [] if dim.is_partition]

    @property
    def partition(self) -> Optional[Dimension]:  # noqa: D
        partitions = self.partitions
        if not partitions:
            return None
        if len(partitions) > 1:
            raise ValueError(f"too many partitions for data source {self.name}")
        return partitions[0]

    @property
    def reference(self) -> DataSourceReference:  # noqa: D
        return DataSourceReference(data_source_name=self.name)
