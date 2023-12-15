import datetime
import polars as pl

from utils import get_data

data = get_data()
df_customers = data["customers"]
df_orders = data["orders"]
df_orders_items = data["orders_items"]
df_products = data["products"]

df_collectables = (
    df_products.select(["sku", "desc"])
    .filter(pl.col("sku").str.starts_with("COL"))
    .with_columns(
        pl.col("desc")
        .str.replace("\)", "")
        .str.replace("Noah's ", "")
        .str.split_exact(" (", n=1)
        .alias("split")
    )
    .unnest("split")
    .rename({"field_0": "item", "field_1": "color"})
    .drop("desc")
)

df_collectables_grouped = df_collectables.group_by(["item"]).agg("color", "sku")

df_customers_with_collectables = (
    df_collectables.join(df_orders_items, on="sku")
    .join(df_orders, on="orderid")
    .join(df_customers, on="customerid")
    .select(["customerid", "phone", "name", "sku", "item", "color", "orderid"])
)

df_customers_with_collectables_slim = df_customers_with_collectables.select(
    "name", "item", "color", "phone"
)

df_customers_with_collectables_slim.group_by(["name", "phone"]).agg(
    pl.col("color").count()
).sort("color", descending=True)

print("Day 8:", df_customers_with_collectables_slim)
