import itertools
import struct
import os
import threading

import pandas as pd
import dask
import dask.dataframe as dd
import dask.bag as db
import bson
from bson.codec_options import TypeCodec, TypeRegistry, CodecOptions
from bson.errors import InvalidBSON
from flatten_dict import flatten


class LockedIterator(object):
    def __init__(self, it):
        self.lock = threading.Lock()
        self.it = it.__iter__()

    def __iter__(self):
        return self

    def __next__(self):
        self.lock.acquire()
        try:
            return next(self.it)
        finally:
            self.lock.release()


class _ObjectIdCodec(TypeCodec):
    class Mock:
        pass

    python_type = Mock
    bson_type = bson.objectid.ObjectId

    def transform_python(self, value):
        raise NotImplementedError

    def transform_bson(self, value):
        return str(value)


_CODEC_OPTIONS = CodecOptions(type_registry=TypeRegistry([_ObjectIdCodec()]))
_UNPACK_INT = struct.Struct("<i").unpack


def _read_bson(
    open_files, partition_size, meta_take_count, flatten_document
) -> dd.DataFrame:

    file_desc = [file.open() for file in open_files]

    document_count = sum(_get_bson_document_count(fs) for fs in file_desc)

    for fs in file_desc:
        fs.seek(0)

    file_iterator = LockedIterator(
        itertools.chain(
            *[
                bson.decode_file_iter(fs, codec_options=_CODEC_OPTIONS)
                for fs in file_desc
            ]
        )
    )

    def document_spitter(_):
        return next(file_iterator)

    def flattener(document):
        return flatten(document, reducer="dot")

    def metadata_remover(document):
        document.pop("_id", None)
        document.pop("__v", None)
        return document

    bag = db.from_sequence(range(document_count), partition_size=partition_size)

    if flatten_document:
        document_bag = bag.map(document_spitter).map(flattener).map(metadata_remover)
    else:
        document_bag = bag.map(document_spitter).map(metadata_remover)

    with dask.config.set(scheduler="single-threaded"):
        head = document_bag.take(meta_take_count, warn=False)

    meta = pd.DataFrame(list(head))

    for fs in file_desc:
        fs.seek(0)

    return document_bag.to_dataframe(meta=meta)


def _get_bson_document_count(file_obj) -> int:
    count = 0
    while True:
        size_data = file_obj.read(4)
        if len(size_data) == 0:
            break
        elif len(size_data) != 4:
            raise InvalidBSON("cut off in middle of objsize")
        obj_size = _UNPACK_INT(size_data)[0] - 4
        file_obj.seek(obj_size, os.SEEK_CUR)
        count += 1
    return count
