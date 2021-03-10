# daskonverter

<!-- badges: start -->
[![Python package](https://github.com/meakbiyik/daskonverter/actions/workflows/Python-package.yaml/badge.svg)](https://github.com/meakbiyik/daskonverter/actions/workflows/Python-package.yaml)
<!-- badges: end -->

A small python package to convert big source files from one format to another, even for remote files.

## Usage

```python
if __name__ == "__main__":

    convert_files("gcs://daskonverter/mongodump.airpair.tags.bson", "test2.csv")
    convert_files("C:\\blah\\mongodump.airpair.tags.bson", "gcs://daskonverter/test.csv")
```

## Installation

```bash
pip install git+https://github.com/meakbiyik/daskonverter.git
```

To use with GCS, BSON and parquet altogether:

```bash
pip install git+https://github.com/meakbiyik/daskonverter.git#egg=daskonverter[full]
```
