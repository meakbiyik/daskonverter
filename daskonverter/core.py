import itertools

import bson
import dask.dataframe as dd
import dask.bag as db
import dask.bytes as dby


def convert_files(
    urlpath: str,
    targetpath: str,
    source_filetype: str = None,
    target_filetype: str = None,
    reader_kwargs: dict = {},
    writer_kwargs: dict = {},
):
    """Convert source files from urlpath to targetpath.

    To use with remote, ensure proper authentication. For GCS, this can be done
    via command `gcloud auth application-default login`

    If run from the console or in a notebook, `dask` may require
    `if __name__ == "__main__"` conditional. See dask/distributed/issues/2520
    for more information.

    Parameters
    ----------
    urlpath : str
        Source file path or glob. Possibly remote, if given with prefixes such as `gcs://`
    targetpath : str
        Target file path, possibly remote
    source_filetype : str, optional
        File type of the source. If not given, it is inferred from the extension, by default None
    target_filetype : str, optional
        File type of the target. If not given, it is inferred from the extension, by default None
    reader_kwargs : dict, optional
        Additional parameter passed to the dask reader (e.g. read_csv, read_json)
    writer_kwargs : dict, optional
        Additional parameter passed to the dask writer (e.g. to_csv, to_parquet)

    Returns
    -------
    str | DelayedTask
        The names of the file written if they were computed right away. If not, the delayed tasks
        associated to the writing of the files

    Examples
    --------

    >>> if __name__ == "__main__":
    >>>     convert_files("gcs://twitter-data-bucket/big_dump/snp500_articles.bson", "snp500_articles.csv")
    >>>     convert_files("gcs://daskonverter/mongodump.airpair.tags.bson", "test2.csv")
    >>>     convert_files("C:\\blah\\mongodump.airpair.tags.bson", "test.csv")
    >>>     convert_files(
                "C:\\blah\\mongodump.airpair.tags.bson",
                "test.parquet",
                write_index=False,
                write_metadata_file=False,
                partition_size=1024,
            )
    """

    if source_filetype is None:
        source_filetype = str(urlpath).split(".")[-1]

    source_filetype = source_filetype.lower()

    if source_filetype not in _FILETYPE_READERS:
        raise ValueError(
            f"Given source_filetype {source_filetype} is not in readable "
            f"filetypes {list(_FILETYPE_READERS.keys())}."
        )

    if target_filetype is None:
        target_filetype = str(targetpath).split(".")[-1]

    target_filetype = target_filetype.lower()

    if target_filetype not in _FILETYPE_WRITERS:
        raise ValueError(
            f"Given target_filetype {target_filetype} is not in readable "
            f"filetypes {list(_FILETYPE_WRITERS.keys())}."
        )

    open_files = dby.open_files(urlpath)
    reader, _reader_kwargs = _FILETYPE_READERS[source_filetype]
    _reader_kwargs.update(reader_kwargs)

    df = reader(open_files, **_reader_kwargs)

    writer, _writer_kwargs = _FILETYPE_WRITERS[target_filetype]
    _writer_kwargs.update(writer_kwargs)

    result = writer(df)(targetpath, **_writer_kwargs)

    if _writer_kwargs.get("compute", True):
        for file in open_files:
            file.close()

    return result


def _read_bson(open_files, partition_size) -> dd.DataFrame:
    def metadata_remover(document):
        document.pop("_id", None)
        document.pop("__v", None)
        return document

    file_iterator = itertools.chain(
        *[bson.decode_file_iter(file.open()) for file in open_files]
    )

    bag = db.from_sequence(file_iterator, partition_size=partition_size)
    df = bag.map(metadata_remover).to_dataframe()

    return df


_FILETYPE_READERS = {
    "bson": (_read_bson, {"partition_size": 1024}),
    "csv": (dd.read_csv, {}),
    "table": (dd.read_table, {}),
    "fwf": (dd.read_fwf, {}),
    "parquet": (dd.read_parquet, {}),
    "hdf": (dd.read_hdf, {}),
    "json": (dd.read_json, {}),
    "orc": (dd.read_orc, {}),
}

_FILETYPE_WRITERS = {
    "parquet": (lambda df: df.to_parquet, {}),
    "csv": (lambda df: df.to_csv, {"single_file": True, "index": False}),
    "hdf": (lambda df: df.to_hdf, {}),
    "json": (lambda df: df.to_json, {}),
}
