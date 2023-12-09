from datetime import datetime
from pathlib import Path

import polars as pl

from utils import get_data

data = get_data()
df_customers = data["customers"]
df_orders = data["orders"]
df_orders_items = data["orders_items"]
df_products = data["products"]

YEARS_OF_THE_RABBIT = [2035, 2023, 2011, 1999, 1987, 1975, 1963, 1951, 1939, 1927]
CANCER_SIGN = {"start": {"month": 6, "day": 22}, "end": {"month": 7, "day": 22}}
CONTRACTOR_NUMBER = "332-274-4185"
CONTRACTOR_CITY = (
    (
        df_customers.select(pl.col("citystatezip", "address", "phone")).filter(
            pl.col("phone") == CONTRACTOR_NUMBER
        )
    )
    .get_column("citystatezip")
    .item()
)


df_cancers_year_rabbit = df_customers.select(
    pl.col(["customerid", "name", "address", "citystatezip", "phone", "birthdate"]),
).filter(
    (pl.col("birthdate").dt.year().is_in(YEARS_OF_THE_RABBIT))
    & (
        # Birthdate is between the start and end dates for the sign.
        (
            (pl.col("birthdate").dt.month() == CANCER_SIGN["start"]["month"])
            & (pl.col("birthdate").dt.day() >= CANCER_SIGN["start"]["day"])
        )
        | (
            (pl.col("birthdate").dt.month() == CANCER_SIGN["end"]["month"])
            & (pl.col("birthdate").dt.day() <= CANCER_SIGN["end"]["day"])
        )
    )
    # From the "same neighborhood", so at least same city.
    & (pl.col("citystatezip") == CONTRACTOR_CITY)
)

sol = df_cancers_year_rabbit.select("phone").item()
print("Day 3:", sol)
