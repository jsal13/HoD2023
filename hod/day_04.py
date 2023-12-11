import polars as pl

from utils import get_data

data = get_data()
df_customers = data["customers"]
df_orders = data["orders"]
df_orders_items = data["orders_items"]
df_products = data["products"]

df_pastries = df_products.select(pl.col(["sku", "desc"])).filter(
    (pl.col("sku").str.contains("BKY"), ~pl.col("desc").str.contains("Bagel"))
)

df_orders_containing_pastries = (
    df_orders_items.join(df_pastries, on="sku", how="inner")
    .select(pl.col("orderid"))
    .unique()
)

df_pastry_order_data = df_orders.join(
    df_orders_containing_pastries, on="orderid", how="inner"
).select([pl.col(["orderid", "customerid", "ordered"])])

df_early_morning_pastry_orderers = df_pastry_order_data.filter(
    ((pl.col("ordered").dt.hour() >= 0) & (pl.col("ordered").dt.hour() < 5))
)

customerid = (
    df_early_morning_pastry_orderers.group_by(by="customerid")
    .agg(pl.col("orderid").count())
    .sort(by="orderid", descending=True)
    .item(row=0, column="customerid")
)

phone_number = df_customers.filter(pl.col("customerid") == customerid).item(
    row=0, column="phone"
)

print("Phone:", phone_number)
