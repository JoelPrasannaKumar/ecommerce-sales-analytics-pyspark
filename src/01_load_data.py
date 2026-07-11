"""
01_load_data.py
---------------
Module 1 of 5 — Data Loading

WHAT THIS MODULE DOES:
    • Initializes a local SparkSession (standalone mode — no cluster needed)
    • Reads all three raw CSVs with automatic schema inference
    • Prints schema, row counts, and sample rows for each dataset
    • Caches DataFrames in memory for reuse by downstream modules

KEY PYSPARK CONCEPTS DEMONSTRATED:
    SparkSession    — entry point to every PySpark application
    read.csv        — loading CSV with header and schema inference
    printSchema()   — inspect column names and inferred data types
    show()          — display first N rows (like pandas head())
    count()         — total row count (triggers an action)
    cache()         — persists DataFrame in memory for faster re-reads

HOW TO RUN:
    python src/01_load_data.py
"""

import os
import sys
from pyspark.sql import SparkSession

# ─── Paths ────────────────────────────────────────────────────────────────────
# Build absolute paths so the script works from any working directory.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR      = os.path.join(PROJECT_ROOT, "data", "raw")


# ─── SparkSession ─────────────────────────────────────────────────────────────
def create_spark_session(app_name: str = "EcommerceDataLoader") -> SparkSession:
    """
    Create and return a local SparkSession.

    SparkSession is the unified entry point introduced in Spark 2.0.
    It replaces the older SparkContext + SQLContext pattern.

    'local[*]' tells Spark to run locally using all available CPU cores.
    In production this would point to a cluster master URL instead.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        # Reduce Spark log noise during development
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    # Set log level to WARN so INFO messages don't flood the console
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ─── Loaders ──────────────────────────────────────────────────────────────────
def load_csv(spark: SparkSession, file_path: str, description: str):
    """
    Read a CSV file into a Spark DataFrame.

    Parameters
    ----------
    spark       : active SparkSession
    file_path   : absolute path to the CSV file
    description : human-readable label used in console output

    Spark Options Used:
        header=True          — first row contains column names
        inferSchema=True     — Spark samples the file to detect types
                               (use explicit StructType schemas in production
                                to avoid the extra scan and ensure correctness)
        nullValue=""         — treat empty strings as null
    """
    if not os.path.exists(file_path):
        print(f"[ERROR] File not found: {file_path}")
        print("        Run generate_data.py first to create the raw datasets.")
        sys.exit(1)

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("nullValue", "")
        .csv(file_path)
    )
    return df


def display_dataframe_info(df, name: str, sample_rows: int = 5) -> None:
    """
    Print a structured summary of a DataFrame: schema, row count, and sample.
    """
    separator = "─" * 60
    print(f"\n{separator}")
    print(f"  DataFrame: {name}")
    print(separator)

    print(f"\n  Schema:")
    df.printSchema()

    row_count = df.count()   # <── ACTION: triggers a full scan of the dataset
    print(f"  Total Rows : {row_count:,}")

    print(f"\n  Sample Data (first {sample_rows} rows):")
    df.show(sample_rows, truncate=False)


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("\n" + "=" * 60)
    print("  Module 01 — Data Loading")
    print("=" * 60)

    spark = create_spark_session()

    # ── Load raw datasets ─────────────────────────────────────────────────────
    customers_df = load_csv(
        spark,
        os.path.join(RAW_DIR, "customers.csv"),
        "Customers"
    )
    products_df = load_csv(
        spark,
        os.path.join(RAW_DIR, "products.csv"),
        "Products"
    )
    orders_df = load_csv(
        spark,
        os.path.join(RAW_DIR, "orders.csv"),
        "Orders"
    )

    # ── Cache DataFrames ──────────────────────────────────────────────────────
    # cache() stores the DataFrame in JVM heap memory after the first action.
    # Subsequent actions (count, show, filter) reuse the in-memory data
    # instead of re-reading from disk — critical for iterative workloads.
    customers_df.cache()
    products_df.cache()
    orders_df.cache()

    # ── Inspect each DataFrame ────────────────────────────────────────────────
    display_dataframe_info(customers_df, "customers_df")
    display_dataframe_info(products_df,  "products_df")
    display_dataframe_info(orders_df,    "orders_df")

    # ── Quick null-count report ───────────────────────────────────────────────
    print("\n" + "─" * 60)
    print("  Null Value Counts per Column")
    print("─" * 60)

    from pyspark.sql import functions as F

    for df, name in [(customers_df, "customers"), (products_df, "products"), (orders_df, "orders")]:
        null_counts = df.select([
            F.count(F.when(F.col(c).isNull(), c)).alias(c)
            for c in df.columns
        ])
        print(f"\n  [{name}]")
        null_counts.show(truncate=False)

    print("\n✅ Module 01 complete — all raw datasets loaded and inspected.")
    spark.stop()


if __name__ == "__main__":
    main()
