# Epic serialize - Easy Python objects serialization
[![Epic-serialize CI](https://github.com/Cybereason/epic-serialize/actions/workflows/ci.yml/badge.svg)](https://github.com/Cybereason/epic-serialize/actions/workflows/ci.yml)

## What is it?

The **epic-serialize** Python library provides a method for easily and efficiently serializing multiple Python objects
into a single file, and then deserializing them back in random-like access.

Here's a quick example:
```python
from epic.serialize import SpocReader, SpocWriter

with SpocWriter("myfile.spoc") as spocw:
    spocw.write(map(str, range(10)))
    spocw.write("--done--")

assert list(SpocReader("myfile.spoc")[-3:]) == ["8", "9", "--done--"]
```

## The SPOC format

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

## Usage examples

Create new file with explicit serialization/compression:

```python
with SpocWriter(filename, serialization="pickle", compression="gzip") as spocw:
    for item in items:
        spocw.write(item)
```

Open an existing file for appending:

```python
with SpocWriter(filename, append=True) as spocw:
    spocw.write(more_items)
```

Read all items:

```python
with SpocReader(filename) as spocr:
    read_items = list(spocr)
```

Read sliced items:

```python
sliced_items = list(SpocReader(filename)[10:-1])
sliced_item = SpocReader(filename)[1000]
```
