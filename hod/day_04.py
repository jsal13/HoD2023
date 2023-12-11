import datetime
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

# This was a lucky guess!  We got the one who ordered the most.

# customerid = (
#     df_early_morning_pastry_orderers.group_by(by="customerid")
#     .agg(pl.col("orderid").count())
#     .sort(by="orderid", descending=True)
#     .item(row=0, column="customerid")
# )

# phone_number = df_customers.filter(pl.col("customerid") == customerid).item(
#     row=0, column="phone"
# )

# print("Phone:", phone_number)

# NOTE: A more complex, but more robust, pipeline.

MALE_NAMES = [
    "Adam",
    "Alvin",
    "Anthony",
    "Austin",
    "Bradley",
    "Brandon",
    "Brian",
    "Bryan",
    "Caleb",
    "Carl",
    "Charles",
    "Christopher",
    "Clifford",
    "Cody",
    "Cristian",
    "Daniel",
    "David",
    "George",
    "Glenn",
    "Gregory",
    "James",
    "Jared",
    "Jeremiah",
    "Jeremy",
    "John",
    "Jon",
    "Jonathan",
    "Jose",
    "Joshua",
    "Kevin",
    "Kurt",
    "Lawrence",
    "Marcus",
    "Mark",
    "Matthew",
    "Michael",
    "Mike",
    "Nick",
    "Norman",
    "Peter",
    "Richard",
    "Robert",
    "Ronald",
    "Ruben",
    "Samuel",
    "Scott",
    "Stanley",
    "Stephen",
    "Steve",
    "Timothy",
    "Todd",
    "Travis",
    "Walter",
    "Xavier",
]


def not_male_name(s: str) -> bool:
    for n in MALE_NAMES:
        if n in s:
            return False
    return True


df_suspects = (
    df_early_morning_pastry_orderers.join(df_customers, on="customerid").select(
        pl.col(["orderid", "customerid", "ordered", "name"])
    )
    # Filter out male names.
    .filter((pl.col("name").map_elements(lambda s: not_male_name(s))))
)

multiple_order_suspects = (
    df_suspects.group_by(pl.col("customerid"))
    .agg(pl.col("orderid").count().name.suffix("_count"))
    .filter(pl.col("orderid_count") > 1)
    .sort("orderid_count", descending=True)
    .get_column("customerid")
)

df_multiple_order_suspects = df_suspects.filter(
    pl.col("customerid").is_in(multiple_order_suspects)
)

df_multiple_pastry_order_suspects = (
    (
        df_multiple_order_suspects.join(df_orders_items, on="orderid")
        .join(df_products, on="sku")
        .select(
            (pl.col("orderid", "customerid", "ordered", "name", "qty", "desc", "sku"))
        )
    )
    .filter(pl.col("sku").str.contains("BKY"))
    .sort(by="ordered")
)

df_sus_shopping_list_more_than_one_cookie = (
    df_multiple_pastry_order_suspects.group_by(
        ["customerid", "orderid", "name", "ordered"]
    )
    .agg(
        [pl.col("desc").alias("shopping_list"), pl.col("qty").sum().alias("total_qty")]
    )
    .sort("customerid", descending=True)
    .filter(pl.col("total_qty") > 1)
    .select(
        (
            pl.col(["customerid", "ordered", "name"]),
            pl.col("ordered").dt.date().alias("date"),
        )
    )
)


def get_age(d: datetime.date) -> float:
    return (datetime.date.today() - d).total_seconds() / (60 * 60 * 24 * 365.25)


df_suspects_for_real = (
    df_sus_shopping_list_more_than_one_cookie.join(df_customers, on="customerid")
    .sort("date")
    .select(
        (
            pl.col(["name", "phone", "birthdate"]),
            pl.col("birthdate")
            .map_elements(lambda d: get_age(d))
            .round(decimals=0)
            .cast(pl.Int8)
            .alias("age"),
        )
    )
    .unique()
    .sort("birthdate")
)

print(df_suspects_for_real)

# NOTE: POSSIBLE TO NARROW IT DOWN HERE?
# REF: https://www.zippia.com/advice/tinder-statistics/
#
# Age Range  Share of Tinder users
# 16-24      38%
# 25-34      45%
# 35-44      13%
# 45-54      3%
# 55-64      1%

df_tinder_users = (
    df_suspects_for_real
    # There's only a ~1% chance the person on Tinder is >=55.
    .filter(pl.col("age") < 55)
    .join(df_customers, on="name")
    .select(["name", "phone", "age"])
)

print("POSSIBLE SOLUTION:")
print(df_tinder_users)
