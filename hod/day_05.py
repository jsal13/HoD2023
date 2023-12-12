import datetime
import polars as pl

from utils import get_data

data = get_data()
df_customers = data["customers"]
df_orders = data["orders"]
df_orders_items = data["orders_items"]
df_products = data["products"]

df_senior_cat_food = df_products.select(pl.col(["sku", "desc"])).filter(
    (
        pl.col("sku").str.starts_with("PET"),
        pl.col("desc").str.contains("Senior"),
        pl.col("desc").str.contains("Cat"),
    )
)

df_staten_island = df_customers.filter(
    pl.col("citystatezip").str.starts_with("Staten Island")
)

df_staten_island_cat_food_orders = (
    df_senior_cat_food.join(df_orders_items, on="sku")
    .join(df_orders, on="orderid")
    .join(df_staten_island, on="customerid")
    .select(
        [
            "name",
            "orderid",
            "qty",
            "customerid",
            "ordered",
            "birthdate",
            "phone",
            "sku",
            "desc",
        ]
    )
)

sol_customer_id = (
    df_staten_island_cat_food_orders.select(["name", "phone", "customerid"])
    .join(df_orders, on="customerid")
    .join(df_orders_items, on="orderid")
    .join(df_products, on="sku")
    .select(["customerid", "qty"])
    .group_by(["customerid"])
    .agg(pl.col("qty").sum())
    .sort(by="qty", descending=True)
    .get_column("customerid")
    .item(0)
)


print(
    "Day 5:",
    df_customers.select(["customerid", "name", "phone"])
    .filter(pl.col("customerid") == sol_customer_id)
    .get_column("phone")
    .item(),
)
