"""
A Compressor is a class that compresses bytes into shorter bytes and vice versa.
Must inherit from NamedClass to be registered.
"""
import zlib
import gzip
import bz2
import lzma
from typing import TypeVar
from abc import ABCMeta, abstractmethod

from ..registrar import create_registrar


# Instantiate a new registrar
CompressorRegistrar = create_registrar("CompressorRegistrar")


class Compressor(CompressorRegistrar, metaclass=ABCMeta):
    """
    Base class which supports compression and decompression
    """
    @abstractmethod
    def compress(self, data: bytes) -> bytes:
        pass

    @abstractmethod
    def decompress(self, data: bytes) -> bytes:
        pass


class StandardCompressor(Compressor):
    """
    Simple base class for "standard" compressors, which provide a compress/decompress interface.
    Must set the MODULE class member.
    """
    MODULE = None

    def compress(self, data: bytes) -> bytes:
        return self.MODULE.compress(data)

    def decompress(self, data: bytes) -> bytes:
        return self.MODULE.decompress(data)


# Several built-in compressors

class ZlibSerializer(StandardCompressor, registered_name="zlib"):
    MODULE = zlib


class GzipSerializer(StandardCompressor, registered_name="gzip"):
    MODULE = gzip


class Bz2Serializer(StandardCompressor, registered_name="bz2"):
    MODULE = bz2


class LzmaSerializer(StandardCompressor, registered_name="lzma"):
    MODULE = lzma


class EmptyCompressor(Compressor, registered_name="empty"):
    """
    Pass-through compressor
    """
    def compress(self, data: bytes) -> bytes:
        return data

    def decompress(self, data: bytes) -> bytes:
        return data


C = TypeVar("C", bound=Compressor)

def get_compressor(compression: str | int | type[C]) -> C:
    """
    Create and return a Compressor instance, based on its unique identifier,
    either a name, an integer ID or its class.

    Parameters
    ----------
    compression : str, int or a Compressor class
        Unique identifier of the compressor.

    Returns
    -------
    Compressor
        A new Compressor instance.
    """
    if isinstance(compression, type):
        if not issubclass(compression, Compressor):
            raise ValueError("Invalid compression class given")
        cls = compression
    else:
        # Get the Compressor class from the registrar
        cls = CompressorRegistrar.get(compression)
        if cls is None:
            raise ValueError(f"Unknown compression '{compression}'")
    # Return a new instance
    return cls()
