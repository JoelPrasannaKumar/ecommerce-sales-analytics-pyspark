"""
02_data_cleaning.py
-------------------
Module 2 of 5 â€” Data Cleaning Pipeline

WHAT THIS MODULE DOES:
    â€¢ Removes duplicate rows across all three datasets
    â€¢ Handles missing / null values with sensible fill strategies
    â€¢ Standardises string columns (trim whitespace, title-case)
    â€¢ Normalises state names (abbreviations -> full name, consistent casing)
    â€¢ Validates numeric columns (removes negative/zero quantities and prices)
    â€¢ Filters out rows with unparseable dates
    â€¢ Saves three cleaned DataFrames as Parquet files (via pandas+pyarrow)

KEY PYSPARK CONCEPTS DEMONSTRATED:
    dropDuplicates  â€” remove exact duplicate rows (or on specific columns)
    fillna          â€” fill null values with a constant or column-specific dict
    withColumn      â€” add or overwrite a column using an expression
    drop            â€” remove a column from the DataFrame
    filter / where  â€” keep only rows satisfying a condition (synonyms)
    regexp_replace  â€” clean strings with a regex pattern
    when / otherwise â€” SQL CASE WHEN logic in the DataFrame API
    trim / initcap  â€” string normalisation functions
    to_date         â€” parse a string column into a DateType
    isNull/isNotNull â€” null predicate

HOW TO RUN:
    python src/02_data_cleaning.py
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
from pyspark.sql.types import DoubleType, IntegerType

# --- Paths -------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR      = os.path.join(PROJECT_ROOT, "data", "raw")
PARQUET_DIR  = os.path.join(PROJECT_ROOT, "data", "parquet")
os.makedirs(PARQUET_DIR, exist_ok=True)


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("EcommerceDataCleaning")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# --- State normalisation map -------------------------------------------------
STATE_ABBREV_MAP = {
    "CA": "California", "TX": "Texas", "FL": "Florida",
    "NY": "New York",   "PA": "Pennsylvania", "IL": "Illinois",
    "OH": "Ohio",       "GA": "Georgia",      "MI": "Michigan",
    "NJ": "New Jersey", "VA": "Virginia",     "WA": "Washington",
    "AZ": "Arizona",    "TN": "Tennessee",    "MA": "Massachusetts",
    "IN": "Indiana",    "MO": "Missouri",     "MD": "Maryland",
    "WI": "Wisconsin",  "CO": "Colorado",     "MN": "Minnesota",
    "SC": "South Carolina", "AL": "Alabama",  "LA": "Louisiana",
    "KY": "Kentucky",   "OR": "Oregon",       "OK": "Oklahoma",
    "CT": "Connecticut","UT": "Utah",          "NV": "Nevada",
    "IA": "Iowa",       "AR": "Arkansas",     "MS": "Mississippi",
    "KS": "Kansas",     "NM": "New Mexico",   "NE": "Nebraska",
    "ID": "Idaho",      "WV": "West Virginia","HI": "Hawaii",
    "NH": "New Hampshire","ME": "Maine",       "MT": "Montana",
    "RI": "Rhode Island","DE": "Delaware",     "SD": "South Dakota",
    "ND": "North Dakota","AK": "Alaska",       "VT": "Vermont",
    "WY": "Wyoming",
}


def build_state_correction_expr(col_name: str):
    """
    Build a chained F.when() expression that maps abbreviations to full names,
    then title-cases anything else. Avoids UDFs for JVM-side execution.
    """
    expr = F.initcap(F.trim(F.col(col_name)))
    for abbrev, full_name in STATE_ABBREV_MAP.items():
        expr = F.when(
            F.upper(F.trim(F.col(col_name))) == abbrev, full_name
        ).otherwise(expr)
    return expr


def save_parquet_via_pandas(df, output_path: str, name: str) -> None:
    """
    Convert Spark DataFrame to pandas and write Parquet using PyArrow.

    On Windows with Java 23, Spark's native Parquet writer can hit JVM
    security-manager restrictions. Writing via pandas+pyarrow (pure Python,
    no JVM file I/O) is a reliable fallback that produces identical Parquet
    files readable by any Spark/pandas/DuckDB reader.
    """
    full_path = os.path.join(output_path, name + ".parquet")
    pandas_df = df.toPandas()
    table = pa.Table.from_pandas(pandas_df, preserve_index=False)
    pq.write_table(table, full_path, compression="snappy")
    rows = len(pandas_df)
    print(f"    Saved -> {full_path}  ({rows:,} rows)")


# --- Cleaning functions ------------------------------------------------------

def clean_customers(df):
    """
    1. Drop exact duplicates and duplicates by customer_id
    2. Trim string columns
    3. Normalise state (abbreviation -> full name, title case)
    4. Fill missing city with 'Unknown'
    5. Drop rows with null customer_id or customer_name
    6. Parse registration_date; drop unparseable dates
    """
    print("\n  [Customers] Cleaning...")
    before = df.count()

    df = df.dropDuplicates()
    df = df.dropDuplicates(subset=["customer_id"])

    for col_name in ["customer_name", "city", "state"]:
        df = df.withColumn(col_name, F.trim(F.col(col_name)))

    df = df.withColumn("state", build_state_correction_expr("state"))
    df = df.fillna({"city": "Unknown", "state": "Unknown"})
    df = df.filter(F.col("customer_id").isNotNull())
    df = df.where(F.col("customer_name").isNotNull())
    df = df.withColumn(
        "registration_date",
        F.to_date(F.col("registration_date"), "yyyy-MM-dd")
    )
    df = df.filter(F.col("registration_date").isNotNull())
    df = df.withColumn("customer_name", F.initcap(F.col("customer_name")))

    after = df.count()
    print(f"    Rows before: {before:,}  |  After: {after:,}  |  Removed: {before - after:,}")
    return df


def clean_products(df):
    """
    1. Drop exact duplicates and by product_id
    2. Title-case category
    3. Fill missing product names
    4. Cast and validate price (> 0)
    5. Drop null product IDs
    """
    print("\n  [Products] Cleaning...")
    before = df.count()

    df = df.dropDuplicates()
    df = df.dropDuplicates(subset=["product_id"])
    df = df.withColumn(
        "category",
        F.initcap(F.trim(F.regexp_replace(F.col("category"), r"\s+", " ")))
    )
    df = df.fillna({"product_name": "Unknown Product"})
    df = df.withColumn("product_name", F.initcap(F.trim(F.col("product_name"))))
    df = df.withColumn("price", F.col("price").cast(DoubleType()))
    df = df.filter(F.col("price").isNotNull() & (F.col("price") > 0))
    df = df.filter(F.col("product_id").isNotNull())

    after = df.count()
    print(f"    Rows before: {before:,}  |  After: {after:,}  |  Removed: {before - after:,}")
    return df


def clean_orders(df):
    """
    1. Drop exact duplicates and by order_id
    2. Cast quantity (Integer) and discount (Double)
    3. Fill null discount with 0.0
    4. Filter invalid quantity (<= 0) and discount (outside 0-1)
    5. Parse order_date; drop unparseable dates
    6. Drop null key columns
    """
    print("\n  [Orders] Cleaning...")
    before = df.count()

    df = df.dropDuplicates()
    df = df.dropDuplicates(subset=["order_id"])
    df = df.withColumn("quantity", F.col("quantity").cast(IntegerType()))
    df = df.withColumn("discount", F.col("discount").cast(DoubleType()))
    df = df.fillna({"discount": 0.0})
    df = df.filter(F.col("quantity").isNotNull() & (F.col("quantity") > 0))
    df = df.where((F.col("discount") >= 0.0) & (F.col("discount") <= 1.0))
    df = df.withColumn(
        "order_date",
        F.to_date(F.col("order_date"), "yyyy-MM-dd")
    )
    df = df.filter(F.col("order_date").isNotNull())
    df = df.filter(
        F.col("order_id").isNotNull() &
        F.col("customer_id").isNotNull() &
        F.col("product_id").isNotNull()
    )

    after = df.count()
    print(f"    Rows before: {before:,}  |  After: {after:,}  |  Removed: {before - after:,}")
    return df


# --- Main --------------------------------------------------------------------
def main():
    print("\n" + "=" * 60)
    print("  Module 02 -- Data Cleaning Pipeline")
    print("=" * 60)

    spark = create_spark_session()

    read_opts = {"header": "true", "inferSchema": "true", "nullValue": ""}
    customers_raw = spark.read.options(**read_opts).csv(os.path.join(RAW_DIR, "customers.csv"))
    products_raw  = spark.read.options(**read_opts).csv(os.path.join(RAW_DIR, "products.csv"))
    orders_raw    = spark.read.options(**read_opts).csv(os.path.join(RAW_DIR, "orders.csv"))

    customers_clean = clean_customers(customers_raw)
    products_clean  = clean_products(products_raw)
    orders_clean    = clean_orders(orders_raw)

    print("\n  Saving cleaned datasets as Parquet (pandas+PyArrow)...")
    save_parquet_via_pandas(customers_clean, PARQUET_DIR, "customers_clean")
    save_parquet_via_pandas(products_clean,  PARQUET_DIR, "products_clean")
    save_parquet_via_pandas(orders_clean,    PARQUET_DIR, "orders_clean")

    print("\n  Read-back verification (Spark reading PyArrow-written Parquet):")
    for name in ["customers_clean", "products_clean", "orders_clean"]:
        path = os.path.join(PARQUET_DIR, name + ".parquet")
        count = spark.read.parquet(path).count()
        print(f"    {name}: {count:,} rows")

    print("\nModule 02 complete -- cleaned data saved to data/parquet/")
    spark.stop()


if __name__ == "__main__":
    main()

