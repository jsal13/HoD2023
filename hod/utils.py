from pathlib import Path
import polars as pl

DIR_PATH = Path.cwd()
DATA_PATH = DIR_PATH.joinpath("data/5784")
# Prefix the following with "noahs-", suffix with ".csv"
CSV_FILES = ["customers", "orders", "orders_items", "products"]


def get_data(
    files: str | list[str] = "all", data_path: Path | None = None
) -> dict[str, pl.DataFrame]:
    """
    Get data from CSV files.

    Args:
        files (str, optional): "all" for "all data" or a list of strs for the files required. Defaults to "all".
        data_path (str, optional): optional path to find csv files at.

    Returns:
        dict[str, pl.DataFrame]: Dict of pl.DFs.
    """
    if data_path is None:
        data_path = DATA_PATH

    csv_files: list[str] = []

    if files == "all":
        csv_files = CSV_FILES
    elif isinstance(files, str):
        csv_files = [files]
    elif isinstance(files, list):
        csv_files = files
    else:
        assert (
            False
        ), f"`files` must be a str or list of strs, not {type(files)}: {files}"

    return {
        f: pl.read_csv(data_path.joinpath(f"noahs-{f}.csv"), try_parse_dates=True)
        for f in csv_files
    }
