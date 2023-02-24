"""
A Serializer is a class that can convert Python objects into a bytes file-like object and vice versa.
Must inherit from NamedClass to be registered.
"""
import pickle
from typing import BinaryIO, TypeVar, Any
from abc import ABCMeta, abstractmethod

from ..registrar import create_registrar


# Instantiate a new registrar
SerializerRegistrar = create_registrar("SerializerRegistrar")


class Serializer(SerializerRegistrar, metaclass=ABCMeta):
    """
    Base class which supports serialize and deserialize
    """
    @abstractmethod
    def serialize(self, fp: BinaryIO, obj):
        pass

    @abstractmethod
    def deserialize(self, fp: BinaryIO) -> Any:
        pass


class DumpLoadSerializer(Serializer):
    """
    Simple wrapper for dump/load interface (pickle, dill)
    """
    MODULE = None

    def serialize(self, fp: BinaryIO, obj):
        self.MODULE.dump(obj, fp)

    def deserialize(self, fp: BinaryIO) -> Any:
        return self.MODULE.load(fp)


class PickleSerializer(DumpLoadSerializer, registered_name="pickle"):
    """A basic pickle serializer"""
    MODULE = pickle


S = TypeVar("S", bound=Serializer)

def get_serializer(serialization: str | int | type[S]) -> S:
    """
    Create and return a Serializer instance, based on its unique identifier,
    either a name, an integer ID or its class.

    Parameters
    ----------
    serialization : str, int or a Serializer class
        Unique identifier of the serializer.

    Returns
    -------
    Serializer
        A new Serializer instance.
    """
    if isinstance(serialization, type):
        if not issubclass(serialization, Serializer):
            raise ValueError("Invalid serialization class given")
        cls = serialization
    else:
        # Get the BaseSerializer class from the registrar
        cls = SerializerRegistrar.get(serialization)
        if cls is None:
            raise ValueError(f"Unknown serialization '{serialization}'")
    # Return a new instance
    return cls()
