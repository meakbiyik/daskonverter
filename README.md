# daskonverter

<!-- badges: start -->
[![Python package](https://github.com/meakbiyik/daskonverter/actions/workflows/Python-package.yaml/badge.svg)](https://github.com/meakbiyik/daskonverter/actions/workflows/Python-package.yaml)
<!-- badges: end -->

A small python package to convert big files from one format to another, even for remote files.

## Usage

The conversion is done using dask (either directly dataframe read/to methods, or via `dask.bag`s). Since the conversion is done in chunks, memory footprint is manageable even for very large files.

Supported input file types (as defined in `dask`/`pandas` except for BSON):

* bson: bin­ary-en­coded JSON-like doc­u­ment (BSON)
* csv: comma separated format
* fwf: table of fixed-width formatted lines
* table: general delimited file
* parquet: Parquet format
* hdf: Hierarchical Data Format (HDF)
* json: tree-like JSON format
* orc: ORC format

Supported output file types:

* csv: comma separated format
* parquet: Parquet format
* hdf: Hierarchical Data Format (HDF)
* json: tree-like JSON format

Additional arguments can be passed to reader and writers.

### via CLI

```bash
poetry run daskonverter [OPTIONS] SOURCE_PATH TARGET_PATH
```

### in Python

```python
if __name__ == "__main__":
    convert_files(source_path, target_path)
```

## Installation

To use the components with remote sources or targets, user needs to ensure proper authentication. For GCS, this can be done via command `gcloud auth application-default login`

### CLI

1. Install [Poetry](https://python-poetry.org/), a dependency management tool for Python, via the following command

    ```bash
    curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python
    ```

2. Clone this repository to your local, navigate into the folder and run

    ```bash
    poetry install --no-dev --extras cli
    ```

### Python

```bash
pip install git+https://github.com/meakbiyik/daskonverter.git
```

To use with GCS, BSON and parquet altogether:

```bash
pip install git+https://github.com/meakbiyik/daskonverter.git#egg=daskonverter[full]
```
