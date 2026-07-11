"""
01_load_data.py
---------------
Module 1 of 5 â€” Data Loading

WHAT THIS MODULE DOES:
    â€¢ Initializes a local SparkSession (standalone mode â€” no cluster needed)
    â€¢ Reads all three raw CSVs with automatic schema inference
    â€¢ Prints schema, row counts, and sample rows for each dataset
    â€¢ Caches DataFrames in memory for reuse by downstream modules

KEY PYSPARK CONCEPTS DEMONSTRATED:
    SparkSession    â€” entry point to every PySpark application
    read.csv        â€” loading CSV with header and schema inference
    printSchema()   â€” inspect column names and inferred data types
    show()          â€” display first N rows (like pandas head())
    count()         â€” total row count (triggers an action)
    cache()         â€” persists DataFrame in memory for faster re-reads

HOW TO RUN:
    python src/01_load_data.py
"""

import os
import sys

# â”€â”€â”€ Java 17+ Compatibility â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# PySpark 3.5 uses Hadoop's UserGroupInformation which calls
# Subject.getSubject() â€” a method restricted in Java 17+ by default.
# Setting this flag re-enables it so Spark runs on Java 17, 19, 21, 23.
os.environ.setdefault("JAVA_TOOL_OPTIONS", "-Djava.security.manager=allow")
os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
os.environ.setdefault("hadoop.home.dir", r"C:\hadoop")
os.environ.setdefault("PYSPARK_PYTHON", r"C:\Python312\python.exe")
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", r"C:\Python312\python.exe")

# --- Windows Console UTF-8 fix -----------------------------------------------
# Spark's printSchema() and show() output Unicode box-drawing characters.
# This ensures they render correctly on Windows without UnicodeEncodeError.
import sys, io
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

from pyspark.sql import SparkSession

# â”€â”€â”€ Paths â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Build absolute paths so the script works from any working directory.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR      = os.path.join(PROJECT_ROOT, "data", "raw")


# â”€â”€â”€ SparkSession â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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


# â”€â”€â”€ Loaders â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def load_csv(spark: SparkSession, file_path: str, description: str):
    """
    Read a CSV file into a Spark DataFrame.

    Parameters
    ----------
    spark       : active SparkSession
    file_path   : absolute path to the CSV file
    description : human-readable label used in console output

    Spark Options Used:
        header=True          â€” first row contains column names
        inferSchema=True     â€” Spark samples the file to detect types
                               (use explicit StructType schemas in production
                                to avoid the extra scan and ensure correctness)
        nullValue=""         â€” treat empty strings as null
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
    separator = "â”€" * 60
    print(f"\n{separator}")
    print(f"  DataFrame: {name}")
    print(separator)

    print(f"\n  Schema:")
    df.printSchema()

    row_count = df.count()   # <â”€â”€ ACTION: triggers a full scan of the dataset
    print(f"  Total Rows : {row_count:,}")

    print(f"\n  Sample Data (first {sample_rows} rows):")
    df.show(sample_rows, truncate=False)


# â”€â”€â”€ Main â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def main():
    print("\n" + "=" * 60)
    print("  Module 01 â€” Data Loading")
    print("=" * 60)

    spark = create_spark_session()

    # â”€â”€ Load raw datasets â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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

    # â”€â”€ Cache DataFrames â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # cache() stores the DataFrame in JVM heap memory after the first action.
    # Subsequent actions (count, show, filter) reuse the in-memory data
    # instead of re-reading from disk â€” critical for iterative workloads.
    customers_df.cache()
    products_df.cache()
    orders_df.cache()

    # â”€â”€ Inspect each DataFrame â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    display_dataframe_info(customers_df, "customers_df")
    display_dataframe_info(products_df,  "products_df")
    display_dataframe_info(orders_df,    "orders_df")

    # â”€â”€ Quick null-count report â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("\n" + "â”€" * 60)
    print("  Null Value Counts per Column")
    print("â”€" * 60)

    from pyspark.sql import functions as F

    for df, name in [(customers_df, "customers"), (products_df, "products"), (orders_df, "orders")]:
        null_counts = df.select([
            F.count(F.when(F.col(c).isNull(), c)).alias(c)
            for c in df.columns
        ])
        print(f"\n  [{name}]")
        null_counts.show(truncate=False)

    print("\nâœ… Module 01 complete â€” all raw datasets loaded and inspected.")
    spark.stop()


if __name__ == "__main__":
    main()



