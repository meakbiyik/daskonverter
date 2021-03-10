from ast import literal_eval
from typing import Tuple
from unittest.mock import Mock

try:
    import click
    from click_help_colors import HelpColorsCommand
except:
    if __name__ == "__main__":
        raise ImportError(
            "Some of the optional dependencies for the cli module are not installed. "
            "Please install them either via `poetry install --no-dev -E cli` if you "
            "are working with the cloned repository, or via `pip install daskonverter[cli]` "
            "with pip."
        )
    else:
        click = Mock()
        HelpColorsCommand = Mock()

from daskonverter import convert_files


DESCRIPTION = """Convert source files from urlpath to targetpath.

To use with remote, ensure proper authentication. For GCS, this can be done
via console command `gcloud auth application-default login`

\b
Examples:
    poetry run daskonverter test.csv test.parquet
"""


@click.command(
    help=DESCRIPTION,
    cls=HelpColorsCommand,
    help_headers_color="yellow",
    help_options_color="green",
)
@click.argument("source_path", type=click.Path())
@click.argument("target_path", type=click.Path())
@click.option(
    "-s",
    "--source_filetype",
    default=None,
    help="File type of the source. If not given, it is "
    "inferred from the extension, by default None",
)
@click.option(
    "-t",
    "--target_filetype",
    default=None,
    help="File type of the target. If not given, it is "
    "inferred from the extension, by default None",
)
@click.option(
    "-r",
    "--reader-kwargs",
    multiple=True,
    type=click.STRING,
    help="Additional parameter passed to the dask reader "
    "(e.g. read_csv, read_json) as a key-value tuple "
    "(without spaces) in the form key:value to modify "
    "the visualization. See https://github.com/meakbiyik/daskonverter or "
    "the Python function docstring for the available parameters.",
)
@click.option(
    "-w",
    "--writer-kwargs",
    multiple=True,
    type=click.STRING,
    help="Additional parameter passed to the dask reader "
    "(e.g. read_csv, read_json) as a key-value tuple "
    "(without spaces) in the form key:value to modify "
    "the visualization. See https://github.com/meakbiyik/daskonverter or "
    "the Python function docstring for the available parameters.",
)
def cli(
    source_path: str,
    target_path: str,
    source_filetype: str,
    target_filetype: str,
    reader_kwargs: Tuple[str],
    writer_kwargs: Tuple[str],
):

    reader_kwargs = dict(arg.split(":") for arg in reader_kwargs)
    for k, v in reader_kwargs.items():
        try:
            reader_kwargs[k] = literal_eval(v)
        except ValueError:
            pass

    writer_kwargs = dict(arg.split(":") for arg in writer_kwargs)
    for k, v in writer_kwargs.items():
        try:
            writer_kwargs[k] = literal_eval(v)
        except ValueError:
            pass

    convert_files(
        source_path,
        target_path,
        source_filetype,
        target_filetype,
        reader_kwargs,
        writer_kwargs,
    )


if __name__ == "__main__":

    cli()
