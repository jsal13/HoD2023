import datetime
import polars as pl

from utils import get_data

data = get_data()
df_customers = data["customers"]
df_orders = data["orders"]
df_orders_items = data["orders_items"]
df_products = data["products"]

# Cols that aren't important to us right now.
CUSTOMER_COLS_TO_DROP = [
    "ordered",
    "shipped",
    "address",
    "birthdate",
    "timezone",
    "lat",
    "long",
]

df_sale_items = (
    df_orders_items.join(df_products, on="sku")
    .filter(pl.col("unit_price") < pl.col("wholesale_cost"))
    .select(["orderid", "qty", "unit_price", "wholesale_cost"])
)

df_sale_customers = (
    df_sale_items.join(df_orders, on="orderid")
    .join(df_customers, on="customerid")
    .drop(CUSTOMER_COLS_TO_DROP)
)

df_grouped_sale_customers = df_sale_customers.group_by(
    ["customerid", "name", "citystatezip", "phone"]
).agg(
    [
        pl.col("unit_price").sum().alias("total_unit_price"),
        pl.col("wholesale_cost").sum().alias("total_wholesale_cost"),
    ]
)

df_grouped_sale_customers_sorted = df_grouped_sale_customers.with_columns(
    (pl.col("total_wholesale_cost") - pl.col("total_unit_price")).alias(
        "store_lost_dollars"
    )
).sort("store_lost_dollars", descending=True)


sol = df_customers.filter(
    pl.col("customerid").is_in(
        df_grouped_sale_customers_sorted.head(1).get_column("customerid")
    )
).drop(CUSTOMER_COLS_TO_DROP)

print("Day 6:", sol)
