"""
03_transformations.py
---------------------
Module 3 of 5 — Data Transformations & Feature Engineering

WHAT THIS MODULE DOES:
    • Reads the three cleaned Parquet datasets
    • Joins orders → customers and products to create a single enriched table
    • Computes revenue: quantity × price × (1 − discount)
    • Extracts date parts: year, month, day, day-of-week, week-of-year
    • Applies Window Functions (rank, dense_rank, row_number, lag, lead)
    • Repartitions the enriched dataset by year and month for optimised writes
    • Saves the master enriched DataFrame as Parquet

KEY PYSPARK CONCEPTS DEMONSTRATED:
    join            — inner/left joins between DataFrames
    withColumn      — derived column computation
    date functions  — year(), month(), dayofmonth(), dayofweek(), weekofyear()
    Window          — defines a window frame (partition + order)
    rank()          — rank with gaps (1,1,3)
    dense_rank()    — rank without gaps (1,1,2)
    row_number()    — sequential number per partition
    lag()           — value of previous row in window
    lead()          — value of next row in window
    repartition     — redistribute data into N partitions (for parallelism)
    select          — choose and rename specific columns
    alias           — rename a column or expression

HOW TO RUN:
    python src/03_transformations.py
    (Run AFTER 02_data_cleaning.py)
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ─── Paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_DIR  = os.path.join(PROJECT_ROOT, "data", "parquet")


# ─── SparkSession ─────────────────────────────────────────────────────────────
def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("EcommerceTransformations")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ─── Join Layer ───────────────────────────────────────────────────────────────
def build_enriched_orders(orders_df, customers_df, products_df):
    """
    Join orders with customers and products to produce a denormalised,
    analysis-ready table.

    Join types used:
        INNER join (orders → products) — we only keep orders for known products;
        orders referencing deleted/unknown products are excluded.

        LEFT join (orders → customers) — we keep all valid orders even if the
        customer record is missing (e.g. deleted accounts).  Missing customer
        fields will be null and handled downstream.

    Column naming:
        When two DataFrames share a column name (e.g. customer_id), Spark creates
        ambiguous references.  We explicitly select the columns we want and alias
        them to avoid this.
    """
    # Inner join: orders must have a matching product
    orders_with_products = orders_df.join(
        products_df.select(
            F.col("product_id"),
            F.col("product_name"),
            F.col("category"),
            F.col("price"),
        ),
        on="product_id",
        how="inner"
    )

    # Left join: keep all orders; fill nulls for customers without records
    enriched = orders_with_products.join(
        customers_df.select(
            F.col("customer_id"),
            F.col("customer_name"),
            F.col("city"),
            F.col("state"),
            F.col("registration_date"),
        ),
        on="customer_id",
        how="left"
    )

    return enriched


# ─── Revenue Computation ──────────────────────────────────────────────────────
def compute_revenue(df):
    """
    Add a revenue column.

    Revenue = quantity × price × (1 − discount)

    Example:  quantity=3, price=100, discount=0.10
              → revenue = 3 × 100 × 0.90 = 270.00

    withColumn() can reference existing columns and build arbitrarily complex
    expressions using the F (functions) module.
    """
    df = df.withColumn(
        "revenue",
        F.round(
            F.col("quantity") * F.col("price") * (F.lit(1.0) - F.col("discount")),
            2
        )
    )
    return df


# ─── Date Feature Engineering ─────────────────────────────────────────────────
def extract_date_features(df):
    """
    Derive temporal columns from order_date.

    These columns dramatically simplify downstream GROUP BY queries and are
    the backbone of time-series analysis in Power BI.

    Spark date functions used:
        year()        → integer year  (2022, 2023, 2024)
        month()       → integer month (1–12)
        dayofmonth()  → day number within month (1–31)
        dayofweek()   → day of week (1=Sunday … 7=Saturday in Spark)
        weekofyear()  → ISO week number (1–53)
        date_format() → custom string formatting (like strftime)
    """
    df = (
        df
        .withColumn("order_year",       F.year(F.col("order_date")))
        .withColumn("order_month",      F.month(F.col("order_date")))
        .withColumn("order_day",        F.dayofmonth(F.col("order_date")))
        .withColumn("order_dayofweek",  F.dayofweek(F.col("order_date")))
        .withColumn("order_week",       F.weekofyear(F.col("order_date")))
        .withColumn(
            "year_month",               # e.g. "2023-05" — useful for GROUP BY
            F.date_format(F.col("order_date"), "yyyy-MM")
        )
        .withColumn(
            "month_name",               # e.g. "May 2023" — useful for display
            F.date_format(F.col("order_date"), "MMM yyyy")
        )
    )
    return df


# ─── Window Functions ─────────────────────────────────────────────────────────
def apply_window_functions(df):
    """
    Demonstrate all four required window functions on the enriched DataFrame.

    Window functions compute values across a "window" of related rows without
    collapsing them into a single group the way GROUP BY does.

    Structure of every window function call:
        F.function_name().over(window_spec)

    Window spec components:
        partitionBy("col")  — restart computation for each unique value of col
        orderBy("col")      — defines ordering within the partition
        rowsBetween(...)    — optional frame bounds

    ── rank() ──────────────────────────────────────────────────────────────────
    Assigns the same rank to ties.  Leaves gaps after a tie.
    e.g. revenue values 500, 500, 300 → ranks 1, 1, 3

    ── dense_rank() ────────────────────────────────────────────────────────────
    Same as rank() but no gaps after ties.
    e.g. 500, 500, 300 → ranks 1, 1, 2

    ── row_number() ────────────────────────────────────────────────────────────
    Assigns a unique sequential number regardless of ties.
    Useful when you need exactly 1 row per group (e.g. latest order per customer).

    ── lag() ───────────────────────────────────────────────────────────────────
    Returns the value of a column N rows BEFORE the current row within the window.
    Commonly used to compute period-over-period changes.

    ── lead() ──────────────────────────────────────────────────────────────────
    Returns the value of a column N rows AFTER the current row.
    """

    # Window 1: Rank orders by revenue within each product category
    category_revenue_window = (
        Window
        .partitionBy("category")
        .orderBy(F.col("revenue").desc())
    )

    df = (
        df
        .withColumn("rank_in_category",         F.rank().over(category_revenue_window))
        .withColumn("dense_rank_in_category",    F.dense_rank().over(category_revenue_window))
        .withColumn("row_number_in_category",    F.row_number().over(category_revenue_window))
    )

    # Window 2: lag/lead — show previous and next order revenue per customer
    # Order by order_date so we look back/forward in time.
    customer_time_window = (
        Window
        .partitionBy("customer_id")
        .orderBy("order_date")
    )

    df = (
        df
        .withColumn("prev_order_revenue",  F.lag("revenue",  1).over(customer_time_window))
        .withColumn("next_order_revenue",  F.lead("revenue", 1).over(customer_time_window))
    )

    return df


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("\n" + "=" * 60)
    print("  Module 03 — Data Transformations")
    print("=" * 60)

    spark = create_spark_session()

    # ── Load cleaned Parquet data ─────────────────────────────────────────────
    # Reading Parquet is much faster than CSV — schema is embedded in the file,
    # and Spark can push down filters to skip entire row groups.
    print("\n  Loading cleaned Parquet datasets...")
    customers_df = spark.read.parquet(os.path.join(PARQUET_DIR, "customers_clean"))
    products_df  = spark.read.parquet(os.path.join(PARQUET_DIR, "products_clean"))
    orders_df    = spark.read.parquet(os.path.join(PARQUET_DIR, "orders_clean"))

    print(f"    customers: {customers_df.count():,} rows")
    print(f"    products : {products_df.count():,} rows")
    print(f"    orders   : {orders_df.count():,} rows")

    # ── Build enriched dataset ────────────────────────────────────────────────
    print("\n  Building enriched orders (joins)...")
    enriched_df = build_enriched_orders(orders_df, customers_df, products_df)
    print(f"    Enriched rows: {enriched_df.count():,}")

    # ── Revenue computation ───────────────────────────────────────────────────
    print("\n  Computing revenue...")
    enriched_df = compute_revenue(enriched_df)

    # ── Date feature engineering ──────────────────────────────────────────────
    print("\n  Extracting date features...")
    enriched_df = extract_date_features(enriched_df)

    # ── Window functions ──────────────────────────────────────────────────────
    print("\n  Applying window functions...")
    enriched_df = apply_window_functions(enriched_df)

    # Cache before window functions trigger multiple scans
    enriched_df.cache()

    # ── Preview the enriched schema ───────────────────────────────────────────
    print("\n  Enriched DataFrame schema:")
    enriched_df.printSchema()

    print("\n  Sample enriched rows:")
    enriched_df.select(
        "order_id", "customer_name", "product_name", "category",
        "quantity", "price", "discount", "revenue",
        "order_date", "year_month", "rank_in_category"
    ).show(5, truncate=False)

    # ── Repartition and save ──────────────────────────────────────────────────
    # repartition(N) redistributes data into N equal-sized partitions.
    # Here we repartition by year and month so that queries filtered on time
    # ranges only read the relevant files (partition pruning).
    print("\n  Saving enriched dataset to Parquet (partitioned by year/month)...")
    enriched_output = os.path.join(PARQUET_DIR, "orders_enriched")
    (
        enriched_df
        .repartition(8, "order_year", "order_month")   # 8 partitions
        .write
        .mode("overwrite")
        .partitionBy("order_year", "order_month")       # physical partition folders
        .parquet(enriched_output)
    )
    print(f"    ✅ Saved → {enriched_output}")

    # ── Verify write ──────────────────────────────────────────────────────────
    verify_count = spark.read.parquet(enriched_output).count()
    print(f"    Read-back count: {verify_count:,} rows")

    print("\n✅ Module 03 complete — enriched dataset ready for business analysis.")
    spark.stop()


if __name__ == "__main__":
    main()
