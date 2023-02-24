import pytest

import os
import itertools
import multiprocessing
from io import BytesIO

from epic.serialize.registrar import create_registrar
from epic.serialize.serializers import get_serializer
from epic.serialize.serializers.base import SerializerRegistrar
from epic.serialize.compressors import get_compressor, EmptyCompressor
from epic.serialize.compressors.base import CompressorRegistrar
# Import to register subclasses
from epic.serialize.serializers.dill import DillSerializer
from epic.serialize.compressors.lz4 import Lz4Compressor
# Import main classes
from epic.serialize.spoc import SpocReader, SpocWriter, SpocError


# explicit string (not calculated automatically), for testing
MODULE_NAME = 'epic.serialize.tests.test_spoc'


SERIALIZATIONS = list(filter(lambda x: isinstance(x, str), SerializerRegistrar.REGISTRAR.keys()))
COMPRESSIONS = list(filter(lambda x: isinstance(x, str), CompressorRegistrar.REGISTRAR.keys()))


# Test variables
ITEMS = [{x:str(x)} for x in range(10000)]
DATA = (b"\x00" * 10000) + os.urandom(10000)


class TestRegistrar:

    def test_registrar(self):
        MyRegistrar = create_registrar("MyRegistrar")

        class A(MyRegistrar, registered_name="a"):
            pass

        assert MyRegistrar.get(A.NAME) == MyRegistrar.get(A.INT_ID) == A
        with pytest.raises(ValueError):
            class B(MyRegistrar, registered_name="a"):
                pass

class TestSerializerRegistrars:

    @pytest.mark.parametrize("serialization", SERIALIZATIONS)
    def test_registrar(self, serialization):
        serializer = get_serializer(serialization)
        assert get_serializer(serializer.__class__).__class__ == serializer.__class__
        assert get_serializer(serializer.INT_ID).INT_ID == serializer.INT_ID
        assert get_serializer(serializer.NAME).NAME == serializer.NAME

    def test_non_existing(self):
        with pytest.raises(ValueError):
            get_serializer("non_existing")
        with pytest.raises(ValueError):
            class C:
                pass
            get_serializer(C)


class TestCompressorRegistrars:

    @pytest.mark.parametrize("compression", COMPRESSIONS)
    def test_registrar(self, compression):
        compressor = get_compressor(compression)
        assert get_compressor(compressor.__class__).__class__ == compressor.__class__
        assert get_compressor(compressor.INT_ID).INT_ID == compressor.INT_ID
        assert get_compressor(compressor.NAME).NAME == compressor.NAME

    def test_non_existing(self):
        with pytest.raises(ValueError):
            get_compressor("non_existing")
        with pytest.raises(ValueError):
            class C:
                pass
            get_compressor(C)


class TestSerializers:

    @staticmethod
    def _test_serializer(serialization):
        serializer = get_serializer(serialization)
        buffer = BytesIO()
        for item in ITEMS:
            serializer.serialize(buffer, item)
        buffer.seek(0)
        assert [serializer.deserialize(buffer) for _ in range(len(ITEMS))] == ITEMS

    @pytest.mark.parametrize("serialization", SERIALIZATIONS)
    def test_serializer(self, serialization):
        self._test_serializer(serialization)

class TestCompressors:

    @staticmethod
    def _test_compressor(compression):
        compressor = get_compressor(compression)
        compressed_data = compressor.compress(DATA)
        assert compressor.decompress(compressed_data) == DATA

    @pytest.mark.parametrize("compression", COMPRESSIONS)
    def test_compressors(self, compression):
        self._test_compressor(compression)

    def test_empty_compressor(self):
        compressor = EmptyCompressor()
        assert DATA == compressor.compress(DATA) == compressor.decompress(DATA)

class TestSpoc:

    def test_standalone_syntax(self, tmp_path):
        tmp_file = str(tmp_path / "test_standalone_syntax.spoc")
        # Create SpocWriter, write items, close
        spocw = SpocWriter(tmp_file)
        spocw.write(ITEMS)
        spocw.write(ITEMS)
        spocw.write(123)
        spocw.close()
        # Create SpocReader, read items, close
        spocr = SpocReader(tmp_file)
        items = list(spocr)
        spocr.close()
        assert items == ITEMS + ITEMS + [123,]
        with pytest.raises(IOError):
            spocw.write(ITEMS)
        with pytest.raises(IOError):
            list(spocr)

    def test_with_syntax(self, tmp_path):
        tmp_file = str(tmp_path / "test_with_syntax.spoc")
        with SpocWriter(tmp_file) as spocw:
            spocw.write(ITEMS)
            spocw.write(ITEMS)
            spocw.write(123)
        with SpocReader(tmp_file) as spocr:
            assert list(spocr) == ITEMS + ITEMS + [123,]
        with pytest.raises(IOError):
            spocw.write(ITEMS)
        with pytest.raises(IOError):
            list(spocr)

    def test_write_overwrite_read(self, tmp_path):
        tmp_file = str(tmp_path / "test_write_overwrite_read.spoc")
        # Create new file
        with SpocWriter(tmp_file) as spocw:
            spocw.write(ITEMS)
        # Overwrite existing file
        with SpocWriter(tmp_file) as spocw:
            spocw.write(ITEMS)
        with pytest.raises(IOError):
            spocw.write(ITEMS)
        assert list(SpocReader(tmp_file)) == ITEMS

    def test_create_new_with_append(self, tmp_path):
        tmp_file = str(tmp_path / "test_create_new_with_append.spoc")
        # Create new file, even though append is True
        with SpocWriter(tmp_file, append=True) as spocw:
            spocw.write(ITEMS)
        assert list(SpocReader(tmp_file)) == ITEMS

    def test_append_read(self, tmp_path):
        tmp_file = str(tmp_path / "test_append_read.spoc")
        # Create new file
        with SpocWriter(tmp_file) as spocw:
            spocw.write(ITEMS)
        # Open for append
        with SpocWriter(tmp_file, append=True) as spocw:
            spocw.write(ITEMS)
        with SpocWriter(tmp_file, append=True) as spocw:
            spocw.write(123)
        with pytest.raises(IOError):
            spocw.write(ITEMS)
        assert list(SpocReader(tmp_file)) == ITEMS + ITEMS + [123,]

    def test_multi_max_chunk_size(self, tmp_path):
        tmp_file = str(tmp_path / "test_multi_max_chunk_size.spoc")
        # Create new file
        with SpocWriter(tmp_file) as spocw:
            spocw.write(ITEMS)
        # Re-open with different chunk size
        with SpocWriter(tmp_file, append=True, max_chunk_size=456) as spocw:
            spocw.write(ITEMS)
            spocw.write(123)
        assert list(SpocReader(tmp_file)) == ITEMS + ITEMS + [123,]

    @pytest.mark.parametrize("serialization,compression", list(itertools.product(SERIALIZATIONS, COMPRESSIONS)))
    def test_serializer_compressor(self, tmp_path, serialization, compression):
        filename = str(tmp_path / f"{serialization}_{compression}.spoc")
        with SpocWriter(filename, serialization=serialization, compression=compression) as spocw:
            spocw.write(ITEMS)
        assert list(SpocReader(filename)) == ITEMS

    # Verify the output of these slices matches a regular slice over a list
    # (assuming input is ITEMS of size 10000)
    SLICE_INDICES = [
        (0, 10),                # i[0:10]
        (1000, 2000),           # i[1000:2000]
        (1000, None),           # i[1000:]
        (None, 1000),           # i[:1000]
        (None, None),           # i[:]
        (10, 10),               # i[10:10] == []
        (-10, -2),              # i[-10:-2]
        (-2, -10),              # i[-2:-10] == []
        (-100, None),           # i[-100:]
        (None, -100),           # i[:-100]
        (9000, 11000),          # i[9000:11000] == i[9000:]
        (100000, 200000),       # i[100000:200000] == []
        (100, 50),              # i[100:50] == []
        (100, 100),             # i[100:100] == []
        (-100000, None),        # i[-100000:] == i[:]
        (-100000, -200000),     # i[-100000:-200000] == []
    ]

    @pytest.mark.parametrize("start_idx,end_idx", SLICE_INDICES)
    def test_slicing(self, tmp_path, start_idx, end_idx):
        tmp_file = str(tmp_path / "test_slicing.spoc")
        with SpocWriter(tmp_file) as spocw:
            spocw.write(ITEMS)
        with SpocReader(tmp_file) as spocr:
            ssr = spocr[start_idx:end_idx]
            items = ITEMS[start_idx:end_idx]
            assert len(ssr) == len(items)
            assert list(ssr) == items

    def test_bad_slicing(self, tmp_path):
        tmp_file = str(tmp_path / "test_bad_slicing.spoc")
        with SpocWriter(tmp_file) as spocw:
            spocw.write(ITEMS)
        with SpocReader(tmp_file) as spocr:
            with pytest.raises(ValueError):
                list(spocr[0:1000:-1])

    ACCESS_INDICES = [0, -1, 2000, -2000]

    @pytest.mark.parametrize("idx", ACCESS_INDICES)
    def test_index_access(self, tmp_path, idx):
        tmp_file = str(tmp_path / "test_index_access.spoc")
        with SpocWriter(tmp_file) as spocw:
            spocw.write(ITEMS)
        with SpocReader(tmp_file) as spocr:
            assert spocr[idx] == ITEMS[idx]

    def test_bad_index_access(self, tmp_path):
        tmp_file = str(tmp_path / "test_bad_index_access.spoc")
        with SpocWriter(tmp_file) as spocw:
            spocw.write(ITEMS)
        with SpocReader(tmp_file) as spocr:
            # Index access beyond the range of the file (either positive or negative)
            with pytest.raises(IndexError):
                _ = spocr[len(ITEMS)]
            with pytest.raises(IndexError):
                _ = spocr[1000000]
            with pytest.raises(IndexError):
                _ = spocr[-1000000]

    def test_bad_format(self, tmp_path):
        tmp_file = str(tmp_path / "test_bad_format.spoc")
        with SpocWriter(tmp_file) as spocw:
            spocw.write(ITEMS)
        # Overwrite first chunk
        with open(tmp_file, "rb+") as fp:
            fp.seek(SpocWriter.HEADER_SIZE, os.SEEK_SET)
            fp.write(b'\x00' * 1000)
        with SpocReader(tmp_file) as spocr:
            with pytest.raises(SpocError):
                list(spocr)
        # Truncate header
        with open(tmp_file, "rb+") as fp:
            fp.truncate(5)
        with pytest.raises(SpocError):
            list(SpocReader(tmp_file))
        # Overwrite header
        with open(tmp_file, "rb+") as fp:
            fp.write(b'\x00' * SpocWriter.HEADER_SIZE)
        with pytest.raises(SpocError):
            list(SpocReader(tmp_file))

    @staticmethod
    def _proc_func(mp_list, spoc_slice):
        # Simply append the relevant slice items to the multiprocessing list
        mp_list.extend(list(spoc_slice))

    def test_multiprocessing_slicing(self, tmp_path):
        tmp_file = str(tmp_path / "test_multiprocessing_slicing.spoc")
        # Create new file
        with SpocWriter(tmp_file) as spocw:
            spocw.write(ITEMS)
        # Create a new list for return values
        manager = multiprocessing.Manager()
        mp_list = manager.list()
        # Create a new process
        p = multiprocessing.Process(target=self._proc_func, args=(mp_list, SpocReader(tmp_file)[1000:2000]))
        p.start()
        p.join()
        assert list(mp_list) == ITEMS[1000:2000]
