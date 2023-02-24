"""
Spoc, or Serialized Python Objects Chunks, is a multi-serialization file format that allows
to easily serialize multiple Python objects into a file which later allows quick reading and slicing.

Consider the case where a dict with 100M items is to be serialized to disk. By simply using pickle.dump,
for example, the entire serialization will be done first in memory, and only then written to file, which
may be slow and cause out-of-memory errors. In addition, it's sometimes desirable to serialize objects
one-by-one (for example, while traversing an iterator).

In addition to serializing objects (in "chunks", as the name implies), Spoc also applies compression
before writing to disk, thus potentially saving a lot of space.

The two main classes are SpocWriter, which can either create a new file or append to an existing one,
and SpocReader, which allows reading, iteration and random access via index or slice.

SpocReader slices may also be passed to other process (they are picklable), which is useful when
dividing jobs over multiple slices of the same Spoc file.

The SpocReader/SpocWriter classes fully support the 'with' statement, and this is the preferred syntax.

Currently, the following serialization schemes are supported:
 - pickle (builtin)
 - dill (requires installation)

Currently, the following compression algorithms are supported:
 - zlib (builtin)
 - bz2 (builtin)
 - gzip (builtin)
 - lzma (builtin)
 - lz4 (requires installation)

These parameters can be passed (by name or class) to the SpocWriter, but will only be used if
the file is new or overwritten (not appended to). SpocReader as well as SpocWriter, when used to append
to an existing file, interpret the serializer and compressor from the file's header.

Examples
--------
Create new file with explicit serialization/compression:

>>> with SpocWriter(filename, serialization="pickle", compression="gzip") as spocw:
...     for item in items:
...         spocw.write(item)

Open an existing file for appending:

>>> with SpocWriter(filename, append=True) as spocw:
...     spocw.write(more_items)

Read all items:

>>> with SpocReader(filename) as spocr:
...     read_items = list(spocr)

Read sliced items:

>>> sliced_items = list(SpocReader(filename)[10:-1])
>>> sliced_item = SpocReader(filename)[1000]
"""
import os
import struct
from io import BytesIO
from typing import BinaryIO, Any, overload
from collections.abc import Iterator, Iterable

# TODO: support GeneralizedPath or pathlib.Path
from epic.common.general import to_list
from epic.logging import ClassLogger

from .serializers import Serializer, get_serializer
from .compressors import Compressor, get_compressor


def write_int(fp: BinaryIO, i: int):
    # Write a single 8-byte integer to the given file object
    fp.write(struct.pack("<Q", i))


class SpocError(Exception):
    pass


class Spoc:
    """
    Common base class for both reader and writer
    Parameters
    ----------
    fp : BinaryIO
        An opened file-like object (correct open mode is implied).
    """
    # Each file has a 64-byte header which begins with a 4-byte magic for identification
    # The format is as follows:
    # 0-4: magic (SCF!)
    # 4-12: total count of items in file
    # 12-20: serialization scheme ID
    # 20-28: compression algorithm ID
    # 28-64: reserved (zeroes)
    FILE_HEADER_MAGIC = b"SCF!"
    HEADER_SIZE = 64
    # Each chunk has a 20-byte header which begins with a 4-byte magic for identification
    # The format is as follows:
    # 0-4: magic (SCH!)
    # 4-12: size of chunk in bytes
    # 12-20: count of items in chunk
    CHUNK_HEADER_MAGIC = b"SCH!"

    INT_SIZE = 8

    LOGGER = ClassLogger()

    def _read_int(self) -> int:
        """
        Read a single 8-byte integer from the current fp position
        """
        t = self.fp.read(self.INT_SIZE)
        if len(t) != self.INT_SIZE:
            raise SpocError(f"Cannot read enough bytes for a full int")
        return struct.unpack("<Q", t)[0]

    def __init__(self, fp: BinaryIO):
        self.fp = fp
        # Read the header
        self.fp.seek(0, os.SEEK_SET)
        self.LOGGER.debug(f"Reading SPOC header")
        if self.fp.read(len(self.FILE_HEADER_MAGIC)) != self.FILE_HEADER_MAGIC:
            raise SpocError("Invalid file header magic")
        self.total_count = self._read_int()
        self.LOGGER.debug(f"File contains {self.total_count} items")
        # Interpret serializer and compressor from header
        self.serializer = get_serializer(self._read_int())
        self.LOGGER.debug(f"File serializer: {self.serializer.NAME}")
        self.compressor = get_compressor(self._read_int())
        self.LOGGER.debug(f"File compressor: {self.compressor.NAME}")

    def close(self):
        """
        Close the file, if it was successfully opened. Items cannot be read from a closed file.
        """
        if not hasattr(self, "fp"):
            return
        if not self.fp.closed:
            self.fp.close()

    def __del__(self):
        self.close()

    def __len__(self):
        return self.total_count

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class SpocWriter(Spoc):
    """
    Main class for serializing items.

    Parameters
    ----------
    filename : str
        The name of the .spoc file to be opened or created (no extension is enforced).
    serialization : str | Serializer (default: "pickle")
        The serialization to be used. Ignored if existing file is opened for append.
    compression : str | Compressor (default: "bz2")
        The compression to be used. Ignored if existing file is opened for append.
        Note that using "empty" will result in no compression (only serialization)
    append : bool (default: False)
        Determined whether to append to an existing file (the serialization & compression
        parameters are ignored and read from the file header instead) or create a new Spoc file.
    max_chunk_size : int (default: 1024)
        Determines the "chunk size" - i.e, how many consecutive items should be serialized
        and compressed to file. Note that it's possible to open the same for append multiple time,
        each with a different max_chunk_size. This shouldn't usually be modified, unless the Python
        objects to be serialized are expected to be either very big (decrease max_chunk_size)
        or very small (increase max_chunk_size). The last chunk is always flushed on close.

    Examples
    --------
    >>> with SpocWriter("myfile.spoc") as spocw:
    >>>     # Write multiple items from iterable
    >>>     spocw.write(items)
    >>>     # Write single item at a time
    >>>     for item in items:
    >>>         spocw.write(item)
    """

    def __init__(self, filename: str, serialization: str | Serializer = "pickle",
                 compression: str | Compressor = "bz2", append: bool = False,
                 max_chunk_size: int = 1024):
        self.filename = filename
        self.max_chunk_size = max_chunk_size
        # If the file does not exist or append is False - we always create a new file
        overwrite_mode = not append or not os.path.isfile(self.filename)
        if overwrite_mode:
            # Create a new file with new serializer/compressor
            serializer = get_serializer(serialization)
            compressor = get_compressor(compression)
            self.LOGGER.debug(f"Creating or overwriting {self.filename!r}")
            fp = open(self.filename, "wb+")
            self._create_new(fp, serializer, compressor)
            super().__init__(fp)
        else:
            self.LOGGER.debug(f"Opening existing file {self.filename!r}")
            fp = open(self.filename, "rb+")
            super().__init__(fp)
        self.current_chunk = []

    def close(self):
        """
        If the file is currently open, flush any lingering chunk and close it.
        Items cannot be added to a closed file.
        """
        if not hasattr(self, "fp"):
            return
        if not self.fp.closed:
            self.LOGGER.debug(f"Closing file {self.filename!r}")
            if len(self.current_chunk) > 0:
                self.LOGGER.debug(f"Flushing last chunk")
                self._write_chunk(self.current_chunk)
                self.current_chunk.clear()
            self.fp.flush()
            self.fp.close()

    def _write_int(self, i: int):
        """
        Write the given integer to the current position in the file
        """
        write_int(self.fp, i)

    @classmethod
    def _create_new(cls, fp: BinaryIO, serializer: Serializer, compressor: Compressor):
        """
        A class method that is used by the ctor to create a new empty Spoc file (only the header).
        The header is then read as usual by the constructor.
        """
        fp.truncate()
        fp.seek(0, os.SEEK_SET)
        fp.write(cls.FILE_HEADER_MAGIC)
        # Zero items in file
        write_int(fp, 0)
        # Write the integer ids of the serializer and compressor - these are now used for this file
        write_int(fp, serializer.INT_ID)
        write_int(fp, compressor.INT_ID)
        # Pad with zeros until HEADER_SIZE
        fp.write(b'\x00' * (cls.HEADER_SIZE - len(cls.FILE_HEADER_MAGIC) - (cls.INT_SIZE * 3)))

    def _pack(self, items: list) -> bytes:
        """
        Convert the given items to a stream of bytes using the serializer and compressor.
        """
        buffer = BytesIO()
        for item in items:
            self.serializer.serialize(buffer, item)
        buffer.seek(0)
        return self.compressor.compress(buffer.read())

    def _write_chunk(self, items: list):
        """
        Write the given list of items (chunk) to the end of the file
        """
        data = self._pack(items)
        self.fp.seek(0, os.SEEK_END)
        # Create chunk header
        self.fp.write(self.CHUNK_HEADER_MAGIC)
        self._write_int(len(data))
        self._write_int(len(items))
        self.fp.write(data)
        # Update total item count in header
        self.total_count += len(items)
        self.LOGGER.debug(f"Wrote {len(items)} more items, total count is {self.total_count}")
        self.fp.seek(len(self.FILE_HEADER_MAGIC), os.SEEK_SET)
        self._write_int(self.total_count)

    def write(self, items: Any | Iterable):
        """
        Main API function - appends the given iterable or object to the current chunk and writes it
        to file as needed

        Parameters
        ----------
        items : Any Python object, or an iterable
            A single Python object will be treated as an iterable of size 1.
        """
        if self.fp.closed:
            raise IOError("Cannot write to a closed file")
        for item in to_list(items):
            self.current_chunk.append(item)
            if len(self.current_chunk) == self.max_chunk_size:
                self._write_chunk(self.current_chunk)
                self.current_chunk.clear()


class SpocReader(Spoc):
    """
    Main class for reading serialized items.

    This class allows iteration over all items (SpocReader is iterable), as well as slicing and
    index access, which are relatively fast since during traversal only the chunk headers are read
    until the correct starting index is reached.

    When slicing, the returned object is lazy and picklable, only accessing the file when iterated.
    This is useful when dividing different slices of the same file between multiple workers/processes.

    Parameters
    ----------
    filename : str
        Filename to open for reading. Must be a valid Spoc file.

    Examples
    --------
    >>> with SpocReader(filename) as spocr:
    >>>     for item in spocr:
    >>>     # Do something

    >>> spocr = SpocReader(filename)
    >>> all_items = list(spocr)
    >>> sliced_items = list(spocr[start_idx:end_idx])
    >>> single_item = spocr[idx]

    >>> lazy_slice = spocr[start_idx:end_idx]
    >>> # Can pass the slice to a different process for easy parallelization.
    >>> # Each process will open the same file, but work on different parts.
    >>> multiprocessing.Process(target=some_function, args=(lazy_slice,)).start()
    """
    def __init__(self, filename: str):
        self.filename = filename
        fp = open(self.filename, "rb")
        fp.seek(0, os.SEEK_END)
        self.file_size = fp.tell()
        super().__init__(fp)

    def _unpack(self, data: bytes, count: int) -> list:
        """
        Given a chunk of binary data and a known-in-advance count, unpack the data into items.
        We always should know the count from the chunk header.
        """
        buffer = BytesIO()
        buffer.write(self.compressor.decompress(data))
        buffer.seek(0)
        return [self.serializer.deserialize(buffer) for _ in range(count)]

    def _read_chunk(self) -> list:
        """
        Read the chunk from the current file position and unpack the items.
        """
        if self.fp.read(len(self.CHUNK_HEADER_MAGIC)) != self.CHUNK_HEADER_MAGIC:
            raise SpocError("Invalid chunk header magic")
        size = self._read_int()
        count = self._read_int()
        data = self.fp.read(size)
        if len(data) != size:
            raise SpocError("Mismatched chunk size")
        items = self._unpack(data, count)
        if len(items) != count:
            raise SpocError("Mismatched items count")
        return items

    def _iter_items(self, items_slice: slice) -> Iterator:
        """
        Parameters
        ----------
        items_slice : slice
            The range on which to iterate. If the range is empty, simply returns (i.e: end of iteration).

        Returns
        -------
            An iterator over the given slice (this function is a generator).
        """
        if self.fp.closed:
            raise IOError("Cannot read from a closed file")
        # Convert the given slice to actual start/end indices
        start_idx, end_idx, step = items_slice.indices(self.total_count)
        if step <= 0:
            raise ValueError("Slice step must be positive")
        # If end_idx is smaller than start_idx, there's nothing to iterate
        if start_idx >= end_idx:
            return
        # Read the first chunk header and start skipping ahead without actually reading chunks' content
        cur_idx = 0
        self.fp.seek(self.HEADER_SIZE, os.SEEK_SET)
        while True:
            # If we reached the end of the file - stop
            if self.fp.tell() == self.file_size:
                return
            # Read chunk header
            if self.fp.read(len(self.CHUNK_HEADER_MAGIC)) != self.CHUNK_HEADER_MAGIC:
                raise SpocError("Invalid chunk header magic")
            chunk_size = self._read_int()
            chunk_count = self._read_int()
            # Is this the right chunk? If not - skip it and continue
            if cur_idx + chunk_count < start_idx:
                cur_idx += chunk_count
                self.fp.seek(chunk_size, os.SEEK_CUR)
                continue
            # OK, this is the right chunk - read it and move the current index to the right position
            data = self.fp.read(chunk_size)
            if len(data) != chunk_size:
                raise SpocError("Unexpected end of file")
            for item in self._unpack(data, chunk_count):
                if cur_idx >= start_idx:
                    yield item
                cur_idx += 1
                # Check for end of slice
                if end_idx <= cur_idx:
                    return

    def __iter__(self):
        # Iterate all items
        return self._iter_items(slice(None))

    @overload
    def __getitem__(self, item: int): ...
    @overload
    def __getitem__(self, item: slice) -> "SlicedSpocReader": ...

    def __getitem__(self, item):
        """
        Interface for both slicing and index access.

        Parameters
        ----------
        item : int or slice
            Either an integer index (must be smaller than total_count, or if negative its abs value
            must be no bigger than total_count) or a slice. This is a thin interface above iter_items.

        Returns
        -------
            Either a single item (for a valid int index) or a SlicedSpocReader for slice, which is a lazy
            wrapper that allows future access to the file at the given range.
        """
        if self.fp.closed:
            raise IOError("Cannot get items from a closed file")
        if isinstance(item, slice):
            # Return a new SlicedSpocReader lazy iterator
            return SlicedSpocReader(self.filename, item, self.total_count)
        else:
            if (item >= self.total_count) or (item < 0 and abs(item) > self.total_count):
                raise IndexError("Cannot access negative index larger than the file")
            # Special case for index == -1
            if item == -1:
                item_slice = slice(item, None)
            else:
                item_slice = slice(item, item+1)
            return next(self._iter_items(item_slice))


class SlicedSpocReader:
    """
    A thin lazy wrapper for iterating SpocReader on-demand.

    This class should never be created directly, only using SpocReader[:]

    Parameters
    ----------
    filename : str
        The .spoc filename.
    items_slice : slice
        The slice to iterate over (can be empty).
    total_count : int
        The (current) total number of items in the Spoc file.
    """
    def __init__(self, filename: str, items_slice: slice, total_count: int):
        self.filename = filename
        self.items_slice = items_slice
        start_idx, end_idx, step = self.items_slice.indices(total_count)
        if step <= 0:
            raise ValueError("Slice step must be positive")
        if start_idx >= end_idx:
            self.size = 0
        else:
            self.size = end_idx - start_idx

    # For tqdm etc.
    def __len__(self):
        return self.size

    def __iter__(self):
        # Can be called lazily in a different process
        yield from SpocReader(self.filename)._iter_items(self.items_slice)
