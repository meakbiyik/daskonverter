import bson
import dask.dataframe as dd
import dask.bag as db
import gcsfs


def convert_files(
    urlpath: str,
    targetpath: str,
    target_filetype=None,
    filetype=None,
    partition_size=1024,
    **dask_kwargs,
):
    """Convert source files from urlpath to targetpath.

    To use with remote, ensure proper authentication. For GCS, this can be done
    via command `gcloud auth application-default login`

    Parameters
    ----------
    urlpath : str
        Source file path or glob. Possibly remote, if given with prefixes such as `gcs://`
    targetpath : str
        Target file path, possibly remote
    target_filetype : str, optional
        File type of the target. If not given, it is inferred from the extension, by default None
    filetype : str, optional
        File type of the source. If not given, it is inferred from the extension, by default None
    partition_size : int, optional
        For BSON source only, number of documents per partition, by default 1024
    dask_kwargs : dict, optional
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
    >>>     convert_files(r"C:\blah\mongodump.airpair.tags.bson", "test.csv")
    >>>     convert_files(
    >>>         r"C:\blah\mongodump.airpair.tags.bson",
    >>>         "test.parquet",
    >>>         write_index=False,
    >>>         write_metadata_file=False,
    >>>         partition_size=1024,
    >>>     )
    """

    if filetype is None:
        filetype = str(urlpath).split(".")[-1]

    if filetype == "bson":
        if str(urlpath).startswith("gcs://"):
            fs = gcsfs.GCSFileSystem()
            opener = fs.open
        else:
            opener = open

        def metadata_remover(document):
            document.pop("_id", None)
            document.pop("__v", None)
            return document

        with opener(urlpath, "rb") as f:
            bag = db.from_sequence(
                bson.decode_file_iter(f), partition_size=partition_size
            )
            df = bag.map(metadata_remover).to_dataframe()
    else:
        df = getattr(dd, f"read_{filetype}")(urlpath)

    if target_filetype is None:
        target_filetype = str(targetpath).split(".")[-1]

    if target_filetype == "csv":
        dask_kwargs["single_file"] = dask_kwargs.get("single_file", True)
        dask_kwargs["index"] = dask_kwargs.get("index", False)

    return getattr(df, f"to_{target_filetype}")(targetpath, **dask_kwargs)
