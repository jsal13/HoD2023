from functools import reduce

import polars as pl

from utils import get_data

LETTER_TO_BUTTON_LIST = [
    ("ABC", "2"),
    ("DEF", "3"),
    ("GHI", "4"),
    ("JKL", "5"),
    ("MNO", "6"),
    ("PQRS", "7"),
    ("TUV", "8"),
    ("WXYZ", "9"),
]
LETTER_TO_BUTTON_MAP = reduce(
    lambda x, y: x | y, map(lambda x: {y: x[1] for y in x[0]}, LETTER_TO_BUTTON_LIST)
)


def last_name_to_phone_number(lastname: str) -> str:
    """Translate a name to a number of the form 'XXX-XXX-XXXX' via phone buttons."""
    if len(lastname) != 10:
        return ""
    lbm = "".join(LETTER_TO_BUTTON_MAP[letter] for letter in lastname.upper())
    return f"{lbm[:3]}-{lbm[3:6]}-{lbm[6:]}"


def get_phone_number_eq_last_name(df: pl.DataFrame) -> str:
    return (
        df_customers.select(
            [
                pl.col("phone"),
                pl.col("name").str.split(by=" ").list.get(1).alias("last_name"),
            ]
        )
        # Filter out all rows where last name != 10.
        .filter(pl.col("last_name").str.len_chars() == 10)
        # Create the last name number mapping.
        .select(
            [
                pl.col("phone"),
                pl.col("last_name")
                .map_elements(lambda x: last_name_to_phone_number(lastname=x))
                .alias("last_name_number"),
            ]
            # Filter out names not equal to phone number.
        )
        .filter(pl.col("last_name_number") == pl.col("phone"))
        .get_column("phone")
        .item()
    )


if __name__ == "__main__":
    data = get_data("customers")
    df_customers = data["customers"]
    print("Day 1: ", get_phone_number_eq_last_name(df=df_customers))
