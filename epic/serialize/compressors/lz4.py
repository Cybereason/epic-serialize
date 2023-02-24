"""
Non-default Compressor for LZ4.
This is NOT a dependency - install lz4 manually as needed.
"""
from epic.common.importguard import ImportGuard

with ImportGuard("pip install lz4"):
    import lz4.block

from .base import Compressor


class Lz4Compressor(Compressor, registered_name="lz4"):

    def compress(self, data: bytes) -> bytes:
        return lz4.block.compress(data)

    def decompress(self, data: bytes) -> bytes:
        return lz4.block.decompress(data)
