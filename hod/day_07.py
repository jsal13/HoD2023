import datetime
import polars as pl

from utils import get_data

data = get_data()
df_customers = data["customers"]
df_orders = data["orders"]
df_orders_items = data["orders_items"]
df_products = data["products"]

data = get_data()

df_customers = data["customers"]
df_orders = data["orders"]
df_orders_items = data["orders_items"]
df_products = data["products"]

colors = [
    "green",
    "mauve",
    "blue",
    "orange",
    "yellow",
    "purple",
    "amber",
    "puce",
    "magenta",
    "azure",
    "white",
    "red",
]
color_str_regex = "|".join(colors)

df_color_products = df_products.filter(
    (
        ~pl.col("sku").str.starts_with("PET"),
        ~pl.col("sku").str.starts_with("DLI"),
        (pl.col("desc").str.contains(color_str_regex)),
    )
)

df_orders_of_color_products = (
    df_color_products.join(df_orders_items, on="sku")
    .join(df_orders, on="orderid")
    .drop(
        ["dims_cm", "wholesale_cost", "qty", "unit_price", "shipped", "items", "total"]
    )
    .with_columns(pl.col("ordered").dt.date().alias("date"))
    .with_columns(pl.col("ordered").dt.hour().alias("hour"))
    .with_columns(pl.col("desc").str.extract("\((.*?)\)").alias("color"))
)

df_suspects = df_orders_of_color_products.join(df_customers, on="customerid").drop(
    ["address", "citystatezip", "birthdate", "phone", "timezone", "lat", "long"]
)

# Dates where Sherri Long.
sus_dates = (
    df_suspects.group_by(["date"])
    .agg([pl.col("name"), "color"])
    .filter(pl.col("name").list.contains("Sherri Long"))
    .get_column("date")
)

# Find someone closest to Sherri Long in time.
df_suspects.filter(pl.col("date").is_in(sus_dates)).select(
    ["customerid", "name", "desc", "ordered", "color"]
).sort("ordered")


print("Day 7:", df_customers.filter(pl.col("customerid") == 5783))
