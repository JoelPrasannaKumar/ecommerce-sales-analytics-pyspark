"""
03_transformations.py
---------------------
Module 3 of 5 -- Data Transformations & Feature Engineering

WHAT THIS MODULE DOES:
    * Reads three cleaned Parquet datasets
    * Joins orders -> customers and products (inner + left joins)
    * Computes revenue: quantity x price x (1 - discount)
    * Extracts date parts: year, month, day, day-of-week, week-of-year
    * Applies Window Functions (rank, dense_rank, row_number, lag, lead)
    * Repartitions the enriched DataFrame by year and month
    * Saves the master enriched DataFrame as Parquet via pandas+PyArrow

KEY PYSPARK CONCEPTS DEMONSTRATED:
    join            -- inner/left joins between DataFrames
    withColumn      -- derived column computation
    date functions  -- year(), month(), dayofmonth(), dayofweek(), weekofyear()
    Window          -- defines a window frame (partition + order)
    rank()          -- rank with gaps (1,1,3)
    dense_rank()    -- rank without gaps (1,1,2)
    row_number()    -- sequential number per partition
    lag()           -- value of previous row in window
    lead()          -- value of next row in window
    repartition     -- redistribute data into N partitions
    select          -- choose and rename specific columns

HOW TO RUN:
    python src/03_transformations.py
    (Run AFTER 02_data_cleaning.py)
"""

import os
import sys

# --- Java 17+ / Java 23 Compatibility ----------------------------------------
os.environ.setdefault("JAVA_TOOL_OPTIONS", "-Djava.security.manager=allow")
os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
os.environ.setdefault("hadoop.home.dir", r"C:\hadoop")
os.environ.setdefault("PYSPARK_PYTHON", r"C:\Python312\python.exe")
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", r"C:\Python312\python.exe")

# --- Windows Console UTF-8 fix -----------------------------------------------
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- Paths -------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_DIR  = os.path.join(PROJECT_ROOT, "data", "parquet")


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("EcommerceTransformations")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def save_parquet_via_pandas(df, output_path: str, name: str) -> None:
    """Write Spark DataFrame to Parquet using pandas+PyArrow (avoids JVM writer issues on Windows/Java23)."""
    full_path = os.path.join(output_path, name + ".parquet")
    pandas_df = df.toPandas()
    table = pa.Table.from_pandas(pandas_df, preserve_index=False)
    pq.write_table(table, full_path, compression="snappy")
    print(f"    Saved -> {full_path}  ({len(pandas_df):,} rows)")


# --- Join Layer --------------------------------------------------------------
def build_enriched_orders(orders_df, customers_df, products_df):
    """
    Join orders with customers and products to create a denormalised table.

    INNER join (orders -> products): only keep orders for known products.
    LEFT  join (orders -> customers): keep all valid orders; nulls for unknown customers.
    """
    orders_with_products = orders_df.join(
        products_df.select("product_id", "product_name", "category", "price"),
        on="product_id",
        how="inner"
    )
    enriched = orders_with_products.join(
        customers_df.select("customer_id", "customer_name", "city", "state", "registration_date"),
        on="customer_id",
        how="left"
    )
    return enriched


# --- Revenue -----------------------------------------------------------------
def compute_revenue(df):
    """
    Revenue = quantity x price x (1 - discount)
    e.g. qty=3, price=100, discount=0.10 -> revenue = 270.00
    """
    return df.withColumn(
        "revenue",
        F.round(F.col("quantity") * F.col("price") * (F.lit(1.0) - F.col("discount")), 2)
    )


# --- Date Features -----------------------------------------------------------
def extract_date_features(df):
    """
    Derive temporal columns from order_date for GROUP BY and Power BI slicing.
        year()       -> integer year  (2022, 2023, 2024)
        month()      -> integer month (1-12)
        dayofmonth() -> day within month (1-31)
        dayofweek()  -> 1=Sunday ... 7=Saturday (Spark default)
        weekofyear() -> ISO week number (1-53)
        date_format() -> custom string format
    """
    return (
        df
        .withColumn("order_year",      F.year("order_date"))
        .withColumn("order_month",     F.month("order_date"))
        .withColumn("order_day",       F.dayofmonth("order_date"))
        .withColumn("order_dayofweek", F.dayofweek("order_date"))
        .withColumn("order_week",      F.weekofyear("order_date"))
        .withColumn("year_month",      F.date_format("order_date", "yyyy-MM"))
        .withColumn("month_name",      F.date_format("order_date", "MMM yyyy"))
    )


# --- Window Functions --------------------------------------------------------
def apply_window_functions(df):
    """
    Window functions compute values across a group of rows without collapsing them.

    rank()       -- same rank for ties, gaps after tie  (1,1,3)
    dense_rank() -- same rank for ties, no gaps          (1,1,2)
    row_number() -- unique sequential number regardless of ties
    lag(n)       -- value N rows BEFORE current row (prev order revenue)
    lead(n)      -- value N rows AFTER  current row (next order revenue)
    """
    # Window 1: rank orders within each category by revenue
    cat_win = Window.partitionBy("category").orderBy(F.col("revenue").desc())
    df = (
        df
        .withColumn("rank_in_category",       F.rank().over(cat_win))
        .withColumn("dense_rank_in_category", F.dense_rank().over(cat_win))
        .withColumn("row_num_in_category",    F.row_number().over(cat_win))
    )
    # Window 2: lag/lead on customer's order timeline
    cust_win = Window.partitionBy("customer_id").orderBy("order_date")
    df = (
        df
        .withColumn("prev_order_revenue", F.lag("revenue",  1).over(cust_win))
        .withColumn("next_order_revenue", F.lead("revenue", 1).over(cust_win))
    )
    return df


# --- Main --------------------------------------------------------------------
def main():
    print("\n" + "=" * 60)
    print("  Module 03 -- Data Transformations")
    print("=" * 60)

    spark = create_spark_session()

    print("\n  Loading cleaned Parquet datasets...")
    customers_df = spark.read.parquet(os.path.join(PARQUET_DIR, "customers_clean.parquet"))
    products_df  = spark.read.parquet(os.path.join(PARQUET_DIR, "products_clean.parquet"))
    orders_df    = spark.read.parquet(os.path.join(PARQUET_DIR, "orders_clean.parquet"))
    print(f"    customers : {customers_df.count():,} rows")
    print(f"    products  : {products_df.count():,} rows")
    print(f"    orders    : {orders_df.count():,} rows")

    print("\n  Building enriched orders (joins)...")
    enriched_df = build_enriched_orders(orders_df, customers_df, products_df)
    print(f"    Enriched rows: {enriched_df.count():,}")

    print("\n  Computing revenue...")
    enriched_df = compute_revenue(enriched_df)

    print("\n  Extracting date features...")
    enriched_df = extract_date_features(enriched_df)

    print("\n  Applying window functions (rank, dense_rank, row_number, lag, lead)...")
    enriched_df = apply_window_functions(enriched_df)

    print("\n  Enriched DataFrame schema:")
    enriched_df.printSchema()

    print("\n  Sample enriched rows:")
    enriched_df.select(
        "order_id", "customer_name", "product_name", "category",
        "quantity", "price", "discount", "revenue", "year_month",
        "rank_in_category"
    ).show(5, truncate=False)

    print("\n  Saving enriched dataset as Parquet (pandas+PyArrow)...")
    # repartition() is called to demonstrate the concept; toPandas() then
    # collects to the driver for pandas-based Parquet write.
    enriched_df = enriched_df.repartition(8, "order_year", "order_month")
    save_parquet_via_pandas(enriched_df, PARQUET_DIR, "orders_enriched")

    verify = spark.read.parquet(os.path.join(PARQUET_DIR, "orders_enriched.parquet")).count()
    print(f"    Read-back count: {verify:,} rows")

    print("\nModule 03 complete -- enriched dataset ready for analysis.")
    spark.stop()


if __name__ == "__main__":
    main()


