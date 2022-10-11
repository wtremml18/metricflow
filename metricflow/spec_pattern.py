from abc import ABC, abstractmethod
from typing import TypeVar, Generic

from metricflow.references import ElementReference, IdentifierReference
from metricflow.specs import InstanceSpec, IdentifierSpec

SpecT = TypeVar("SpecT", bound=InstanceSpec)


class SpecPattern(Generic[SpecT], ABC):

    @abstractmethod
    def matches(self, spec: SpecT) -> bool:
        raise NotImplementedError


ReferenceT = TypeVar("ReferenceT", bound=ElementReference)


class IdentifierReferencePattern(SpecPattern[IdentifierSpec]):
    def __init__(self, reference: IdentifierReference) -> None:
        self._reference = reference

    def matches(self, spec: IdentifierSpec) -> bool:
        return spec.element_name == self._reference.element_name
