"""
02_data_cleaning.py
-------------------
Module 2 of 5 — Data Cleaning Pipeline

WHAT THIS MODULE DOES:
    • Removes duplicate rows across all three datasets
    • Handles missing / null values with sensible fill strategies
    • Standardises string columns (trim whitespace, title-case)
    • Normalises state names (abbreviations → full name, consistent casing)
    • Validates numeric columns (removes negative/zero quantities and prices)
    • Filters out rows with unparseable dates
    • Drops rows that are completely unusable
    • Saves three cleaned DataFrames as Parquet files

KEY PYSPARK CONCEPTS DEMONSTRATED:
    dropDuplicates  — remove exact duplicate rows (or on specific columns)
    fillna          — fill null values with a constant or column-specific dict
    withColumn      — add or overwrite a column using an expression
    drop            — remove a column from the DataFrame
    filter / where  — keep only rows satisfying a condition (synonyms)
    regexp_replace  — clean strings with a regex pattern
    when / otherwise — SQL CASE WHEN logic in the DataFrame API
    trim / initcap  — string normalisation functions
    to_date         — parse a string column into a DateType
    isNull/isNotNull — null predicate
    write.parquet   — save a DataFrame as columnar Parquet files

HOW TO RUN:
    python src/02_data_cleaning.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

# ─── Paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR      = os.path.join(PROJECT_ROOT, "data", "raw")
PARQUET_DIR  = os.path.join(PROJECT_ROOT, "data", "parquet")
os.makedirs(PARQUET_DIR, exist_ok=True)


# ─── SparkSession ─────────────────────────────────────────────────────────────
def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("EcommerceDataCleaning")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ─── State normalisation map ───────────────────────────────────────────────────
# Maps abbreviations and all-caps/all-lower variants back to Title Case names.
# In production this would live in a config file or reference table.
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
    Build a chained F.when() expression that maps known abbreviations to full
    state names, then title-cases anything else.

    This replaces a UDF (User Defined Function) — plain Spark expressions are
    always preferred because they run on the JVM without Python serialisation
    overhead.
    """
    expr = F.initcap(F.trim(F.col(col_name)))   # default: title-case the cleaned string
    for abbrev, full_name in STATE_ABBREV_MAP.items():
        expr = F.when(
            F.upper(F.trim(F.col(col_name))) == abbrev, full_name
        ).otherwise(expr)
    return expr


# ─── Cleaning functions ────────────────────────────────────────────────────────

def clean_customers(df):
    """
    Cleaning steps for the customers DataFrame:
    1. Drop exact duplicate rows
    2. Trim whitespace from all string columns
    3. Normalise state column (abbrev → full name, title case)
    4. Fill missing city with "Unknown"
    5. Drop rows where customer_id OR customer_name is null
    6. Parse registration_date; drop rows with unparseable dates
    """
    print("\n  [Customers] Starting cleaning...")
    before = df.count()

    # Step 1 — Remove exact duplicate rows
    # dropDuplicates() with no args removes rows where EVERY column is equal.
    # Passing a subset list removes rows duplicated on those specific columns.
    df = df.dropDuplicates()
    df = df.dropDuplicates(subset=["customer_id"])   # keep first occurrence per ID

    # Step 2 — Trim whitespace from string columns
    for col_name in ["customer_name", "city", "state"]:
        df = df.withColumn(col_name, F.trim(F.col(col_name)))

    # Step 3 — Normalise state values
    df = df.withColumn("state", build_state_correction_expr("state"))

    # Step 4 — Fill missing city
    # fillna() can accept a single value (applied to all string columns) or a
    # dict mapping column names to their fill values.
    df = df.fillna({"city": "Unknown", "state": "Unknown"})

    # Step 5 — Drop rows where critical identifiers are null
    # filter() and where() are exact synonyms; use whichever reads more clearly.
    df = df.filter(F.col("customer_id").isNotNull())
    df = df.where(F.col("customer_name").isNotNull())

    # Step 6 — Parse and validate registration_date
    # to_date() returns null for strings that don't match the format.
    df = df.withColumn(
        "registration_date",
        F.to_date(F.col("registration_date"), "yyyy-MM-dd")
    )
    # Drop rows where date parsing failed (null after conversion)
    df = df.filter(F.col("registration_date").isNotNull())

    # Step 7 — Title-case customer names for consistency
    df = df.withColumn("customer_name", F.initcap(F.col("customer_name")))

    after = df.count()
    print(f"    Rows before: {before:,}  |  After: {after:,}  |  Removed: {before - after:,}")
    return df


def clean_products(df):
    """
    Cleaning steps for the products DataFrame:
    1. Drop duplicate rows (exact and by product_id)
    2. Standardise category casing to Title Case
    3. Fill missing product names with 'Unknown Product'
    4. Cast price to DoubleType; drop null/negative/zero prices
    5. Drop rows with null product_id
    """
    print("\n  [Products] Starting cleaning...")
    before = df.count()

    # Step 1 — Deduplicate
    df = df.dropDuplicates()
    df = df.dropDuplicates(subset=["product_id"])

    # Step 2 — Standardise category: trim + title case
    # regexp_replace removes any extra whitespace inside the string
    df = df.withColumn(
        "category",
        F.initcap(F.trim(F.regexp_replace(F.col("category"), r"\s+", " ")))
    )

    # Step 3 — Fill missing product names
    df = df.fillna({"product_name": "Unknown Product"})
    df = df.withColumn("product_name", F.initcap(F.trim(F.col("product_name"))))

    # Step 4 — Validate price
    df = df.withColumn("price", F.col("price").cast(DoubleType()))
    # filter keeps only rows where price is a valid positive number
    df = df.filter(F.col("price").isNotNull() & (F.col("price") > 0))

    # Step 5 — Drop null product IDs
    df = df.filter(F.col("product_id").isNotNull())

    after = df.count()
    print(f"    Rows before: {before:,}  |  After: {after:,}  |  Removed: {before - after:,}")
    return df


def clean_orders(df):
    """
    Cleaning steps for the orders DataFrame:
    1. Drop exact duplicate rows (and by order_id)
    2. Cast quantity to Integer and discount to Double
    3. Fill missing discount with 0.0 (assume no discount)
    4. Filter out negative/zero quantities
    5. Filter out invalid discount values (must be 0 <= discount <= 1)
    6. Parse order_date; drop rows with unparseable dates
    7. Drop rows where order_id, customer_id, or product_id is null
    """
    print("\n  [Orders] Starting cleaning...")
    before = df.count()

    # Step 1 — Deduplicate
    df = df.dropDuplicates()
    df = df.dropDuplicates(subset=["order_id"])

    # Step 2 — Type casting
    df = df.withColumn("quantity", F.col("quantity").cast(IntegerType()))
    df = df.withColumn("discount", F.col("discount").cast(DoubleType()))

    # Step 3 — Fill null discounts with 0 (business rule: no discount if missing)
    df = df.fillna({"discount": 0.0})

    # Step 4 — Remove invalid quantities (must be a positive integer)
    df = df.filter(F.col("quantity").isNotNull() & (F.col("quantity") > 0))

    # Step 5 — Discount must be between 0 and 1 (inclusive)
    # using where() here to demonstrate it's identical to filter()
    df = df.where((F.col("discount") >= 0.0) & (F.col("discount") <= 1.0))

    # Step 6 — Parse order_date
    df = df.withColumn(
        "order_date",
        F.to_date(F.col("order_date"), "yyyy-MM-dd")
    )
    df = df.filter(F.col("order_date").isNotNull())

    # Step 7 — Drop rows missing any primary/foreign key
    df = df.filter(
        F.col("order_id").isNotNull() &
        F.col("customer_id").isNotNull() &
        F.col("product_id").isNotNull()
    )

    after = df.count()
    print(f"    Rows before: {before:,}  |  After: {after:,}  |  Removed: {before - after:,}")
    return df


def save_parquet(df, path: str, name: str) -> None:
    """
    Write a DataFrame to disk as Parquet.

    Parquet is a columnar storage format that:
      • Compresses data significantly vs CSV
      • Stores schema information in the file footer
      • Enables predicate pushdown (Spark can skip entire row groups)
      • Is the industry standard format for analytics workloads

    mode="overwrite" replaces any existing data at that path.
    """
    output_path = os.path.join(path, name)
    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )
    print(f"    ✅ Saved → {output_path}")


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("\n" + "=" * 60)
    print("  Module 02 — Data Cleaning Pipeline")
    print("=" * 60)

    spark = create_spark_session()

    # Load raw CSVs
    read_opts = {"header": "true", "inferSchema": "true", "nullValue": ""}
    customers_raw = spark.read.options(**read_opts).csv(os.path.join(RAW_DIR, "customers.csv"))
    products_raw  = spark.read.options(**read_opts).csv(os.path.join(RAW_DIR, "products.csv"))
    orders_raw    = spark.read.options(**read_opts).csv(os.path.join(RAW_DIR, "orders.csv"))

    # Clean each dataset
    customers_clean = clean_customers(customers_raw)
    products_clean  = clean_products(products_raw)
    orders_clean    = clean_orders(orders_raw)

    # Persist cleaned data as Parquet
    print("\n  Saving cleaned datasets to Parquet...")
    save_parquet(customers_clean, PARQUET_DIR, "customers_clean")
    save_parquet(products_clean,  PARQUET_DIR, "products_clean")
    save_parquet(orders_clean,    PARQUET_DIR, "orders_clean")

    # Quick validation — re-read from Parquet and confirm row counts
    print("\n  Parquet read-back validation:")
    for name in ["customers_clean", "products_clean", "orders_clean"]:
        count = spark.read.parquet(os.path.join(PARQUET_DIR, name)).count()
        print(f"    {name}: {count:,} rows")

    print("\n✅ Module 02 complete — cleaned data saved to data/parquet/")
    spark.stop()


if __name__ == "__main__":
    main()
