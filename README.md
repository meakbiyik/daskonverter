# daskonverter

A small python package to convert big source files from one format to another, even for remote files.

## Usage

```python
if __name__ == "__main__":

    convert_files("gcs://daskonverter/mongodump.airpair.tags.bson", "test2.csv")
    convert_files(r"C:\blah\mongodump.airpair.tags.bson", "test.csv")
    convert_files(
        r"C:\blah\mongodump.airpair.tags.bson",
        "test.parquet",
        write_index=False,
        write_metadata_file=False,
        partition_size=1024,
    )
```

## Installation

```bash
pip install git+https://github.com/meakbiyik/daskonverter.git
```

To use with GCS, BSON and parquet altogether:

```bash
pip install git+https://github.com/meakbiyik/daskonverter.git#egg=daskonverter[dslab]
```
