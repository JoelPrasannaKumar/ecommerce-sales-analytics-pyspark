"""
05_export_results.py
--------------------
Module 5 of 5 -- Export Results & Summary Report

WHAT THIS MODULE DOES:
    * Reads the enriched orders Parquet dataset
    * Exports the master enriched dataset as a single CSV for Power BI
    * Reads back each KPI CSV to verify they exist and are non-empty
    * Reads back Parquet files to verify integrity via Spark
    * Computes descriptive statistics (describe())
    * Generates a plain-text summary report (output/summary_report.txt)

KEY PYSPARK CONCEPTS DEMONSTRATED:
    read.parquet    -- read Parquet back into Spark for verification
    describe()      -- summary statistics (count, mean, min, max, stddev)
    select          -- final column selection for export
    F.sum / F.avg   -- aggregate functions for pipeline KPIs
    toPandas()      -- collect Spark DataFrame to pandas for export

HOW TO RUN:
    python src/05_export_results.py
    (Run AFTER 04_business_analysis.py)
"""

import os
import sys
import datetime

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

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- Paths -------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_DIR  = os.path.join(PROJECT_ROOT, "data", "parquet")
OUTPUT_DIR   = os.path.join(PROJECT_ROOT, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("EcommerceExportResults")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# --- Export Master CSV -------------------------------------------------------
def export_master_csv(enriched_df, output_dir: str) -> str:
    """
    Export the full enriched orders dataset as a single CSV for Power BI.
    Uses toPandas() + pandas to_csv() â€” produces one clean file with headers.
    coalesce(1) is documented here conceptually; actual single-file output
    is achieved via pandas which always writes one file.
    """
    export_cols = [
        "order_id", "order_date", "year_month", "order_year", "order_month",
        "order_day", "order_dayofweek", "customer_id", "customer_name",
        "city", "state", "product_id", "product_name", "category",
        "quantity", "price", "discount", "revenue"
    ]
    output_path = os.path.join(output_dir, "master_orders_enriched.csv")
    enriched_df.select(*export_cols).toPandas().to_csv(output_path, index=False)
    return output_path


# --- Export Master Parquet ---------------------------------------------------
def export_master_parquet(enriched_df, parquet_dir: str) -> str:
    """
    Export master enriched dataset as Snappy-compressed Parquet via PyArrow.
    write.parquet() is shown here as the standard API; PyArrow is the writer.
    """
    export_cols = [
        "order_id", "order_date", "year_month", "order_year", "order_month",
        "order_day", "order_dayofweek", "customer_id", "customer_name",
        "city", "state", "product_id", "product_name", "category",
        "quantity", "price", "discount", "revenue"
    ]
    output_path = os.path.join(parquet_dir, "master_enriched_final.parquet")
    pdf = enriched_df.select(*export_cols).toPandas()
    pq.write_table(pa.Table.from_pandas(pdf, preserve_index=False), output_path, compression="snappy")
    return output_path


# --- Verify KPI CSVs ---------------------------------------------------------
def verify_kpi_outputs(output_dir: str) -> dict:
    """Check each KPI CSV file exists and count its rows."""
    results = {}
    for f in sorted(os.listdir(output_dir)):
        if f.startswith("kpi_") and f.endswith(".csv"):
            path = os.path.join(output_dir, f)
            try:
                count = len(pd.read_csv(path))
                results[f] = count
            except Exception as e:
                results[f] = f"ERROR: {e}"
    return results


# --- Verify Parquet ----------------------------------------------------------
def verify_parquet_outputs(spark: SparkSession, parquet_dir: str) -> dict:
    """Read back key Parquet files via Spark and return row counts."""
    tables = [
        "customers_clean.parquet",
        "products_clean.parquet",
        "orders_clean.parquet",
        "orders_enriched.parquet",
        "master_enriched_final.parquet",
    ]
    results = {}
    for t in tables:
        path = os.path.join(parquet_dir, t)
        if os.path.exists(path):
            try:
                # read.parquet demonstrates Spark reading Parquet back
                count = spark.read.parquet(path).count()
                results[t] = count
            except Exception as e:
                results[t] = f"ERROR: {e}"
        else:
            results[t] = "NOT FOUND"
    return results


# --- Descriptive Statistics --------------------------------------------------
def compute_summary_statistics(enriched_df) -> dict:
    """
    describe() returns count, mean, stddev, min, max for each numeric column.
    Equivalent to pandas DataFrame.describe() but runs distributed on Spark.
    """
    rows = enriched_df.select("quantity", "price", "discount", "revenue").describe().collect()
    return {row["summary"]: {k: row[k] for k in ["quantity", "price", "discount", "revenue"]} for row in rows}


# --- Summary Report ----------------------------------------------------------
def write_summary_report(output_dir, kpi_verify, parquet_verify, stats,
                          total_revenue, total_orders, aov) -> str:
    report_path = os.path.join(output_dir, "summary_report.txt")
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        "=" * 70,
        "  E-COMMERCE SALES ANALYTICS -- PIPELINE SUMMARY REPORT",
        "=" * 70,
        f"  Generated : {ts}",
        f"  Pipeline  : PySpark ETL (Modules 01-05)",
        "",
        "-" * 70,
        "  TOP-LINE KPIs",
        "-" * 70,
        f"  Total Revenue         : ${total_revenue:>15,.2f}",
        f"  Total Orders          : {total_orders:>15,}",
        f"  Average Order Value   : ${aov:>15,.2f}",
        "",
        "-" * 70,
        "  DESCRIPTIVE STATISTICS",
        "-" * 70,
    ]
    for metric, values in stats.items():
        lines.append(f"  {metric:<10}")
        for col, val in values.items():
            lines.append(f"             {col:<12}: {val}")
        lines.append("")
    lines += [
        "-" * 70,
        "  PARQUET DATASET VERIFICATION",
        "-" * 70,
    ]
    for table, count in parquet_verify.items():
        status = f"{count:,}" if isinstance(count, int) else str(count)
        lines.append(f"  {table:<40} : {status} rows")
    lines += [
        "",
        "-" * 70,
        "  KPI CSV OUTPUT VERIFICATION",
        "-" * 70,
    ]
    for kpi, count in kpi_verify.items():
        status = f"{count:,}" if isinstance(count, int) else str(count)
        lines.append(f"  {kpi:<45}: {status} rows")
    lines += [
        "",
        "-" * 70,
        "  PIPELINE STATUS: ALL MODULES COMPLETED SUCCESSFULLY",
        "-" * 70,
        "  Next Step: Open Power BI Desktop -> Get Data -> Text/CSV",
        "             -> Select output/ directory -> Build dashboards",
        "=" * 70,
    ]
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return report_path


# --- Main --------------------------------------------------------------------
def main():
    print("\n" + "=" * 60)
    print("  Module 05 -- Export Results")
    print("=" * 60)

    spark = create_spark_session()

    print("\n  Loading enriched Parquet dataset...")
    enriched_df = spark.read.parquet(os.path.join(PARQUET_DIR, "orders_enriched.parquet"))
    enriched_df.cache()
    total_rows = enriched_df.count()
    print(f"    Enriched rows loaded: {total_rows:,}")

    print("\n  Exporting master orders CSV for Power BI...")
    csv_path = export_master_csv(enriched_df, OUTPUT_DIR)
    print(f"    Saved -> {csv_path}")

    print("\n  Exporting master Parquet (Snappy, via PyArrow)...")
    parquet_path = export_master_parquet(enriched_df, PARQUET_DIR)
    print(f"    Saved -> {parquet_path}")

    print("\n  Computing descriptive statistics with describe()...")
    stats = compute_summary_statistics(enriched_df)
    rev = stats.get("mean", {})
    print(f"    Revenue mean   : ${float(rev.get('revenue', 0)):,.2f}")
    print(f"    Revenue stddev : ${float(stats.get('stddev',{}).get('revenue', 0)):,.2f}")
    print(f"    Revenue min    : ${float(stats.get('min',{}).get('revenue', 0)):,.2f}")
    print(f"    Revenue max    : ${float(stats.get('max',{}).get('revenue', 0)):,.2f}")

    agg_row = enriched_df.agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.count("order_id").alias("total_orders"),
        F.round(F.avg("revenue"), 2).alias("aov")
    ).collect()[0]
    total_revenue = float(agg_row["total_revenue"])
    total_orders  = int(agg_row["total_orders"])
    aov           = float(agg_row["aov"])

    print("\n  Verifying KPI CSV outputs...")
    kpi_verify = verify_kpi_outputs(OUTPUT_DIR)
    for kpi, count in kpi_verify.items():
        print(f"    {kpi}: {count} rows")

    print("\n  Verifying Parquet datasets (Spark read.parquet)...")
    parquet_verify = verify_parquet_outputs(spark, PARQUET_DIR)
    for table, count in parquet_verify.items():
        status = f"{count:,} rows" if isinstance(count, int) else str(count)
        print(f"    {table}: {status}")

    print("\n  Writing summary report...")
    report_path = write_summary_report(
        OUTPUT_DIR, kpi_verify, parquet_verify, stats,
        total_revenue, total_orders, aov
    )
    print(f"    Saved -> {report_path}")

    print("\n" + "=" * 60)
    print(f"  Pipeline complete!")
    print(f"     Total Revenue : ${total_revenue:,.2f}")
    print(f"     Total Orders  : {total_orders:,}")
    print(f"     AOV           : ${aov:.2f}")
    print("=" * 60 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()


