from pathlib import Path

from daskonverter import convert_files


def test_convert_files_bson_2_csv(tmp_path: Path):
    bson = Path(__file__).parent.joinpath("resources", "small.bson")
    convert_files(bson, tmp_path.joinpath("test.csv"))


def test_convert_files_bson_2_parquet(tmp_path: Path):
    bson = Path(__file__).parent.joinpath("resources", "mongodump.airpair.tags.bson")
    convert_files(bson, tmp_path.joinpath("test.parquet"))


def test_convert_files_csv_2_parquet(tmp_path: Path):
    csv = Path(__file__).parent.joinpath("resources", "mongodump.airpair.tags.csv")
    convert_files(csv, tmp_path.joinpath("test.parquet"))
