from pathlib import Path

import polars as pl

from utils import get_data


data = get_data()
df_customers = data["customers"]
df_orders = data["orders"]
df_orders_items = data["orders_items"]
df_products = data["products"]


def get_initials(name: str) -> str:
    return "".join(s[0].upper() for s in name.split(" "))


df_customer_initials = df_customers.select(
    pl.col("customerid"),
    pl.col("phone"),
    pl.col("name").map_elements(lambda s: get_initials(name=s)).alias("initials"),
).filter(pl.col("initials") == "JP")

df_orders_2017 = df_orders.select(
    pl.col("customerid"), pl.col("ordered").dt.year().alias("year"), pl.col("orderid")
).filter(pl.col("year") == 2017)

df_joined = (
    df_customer_initials.join(df_orders_2017, on="customerid")
    .join(df_orders_items.select(pl.col("orderid"), pl.col("sku")), on="orderid")
    .join(
        df_products.select(pl.col("sku"), pl.col("desc")).filter(
            pl.col("sku").str.contains("DLI")
        ),
        on="sku",
    )
)

val = df_joined.filter(pl.col("desc").str.contains("Coffee")).get_column("phone").item()
print(val)
