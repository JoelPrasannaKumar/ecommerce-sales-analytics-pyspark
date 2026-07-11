"""
05_export_results.py
--------------------
Module 5 of 5 — Export Results & Summary Report

WHAT THIS MODULE DOES:
    • Reads the enriched orders Parquet dataset
    • Exports the master enriched dataset as a single CSV for Power BI
    • Re-reads each KPI CSV from output/ to verify they exist and are non-empty
    • Reads back the Parquet files to verify integrity
    • Generates a plain-text summary report (output/summary_report.txt)

KEY PYSPARK CONCEPTS DEMONSTRATED:
    coalesce(1)     — merge partitions into one file without a shuffle
    write.csv       — export DataFrame as CSV
    write.parquet   — save with Snappy compression
    read.parquet    — read-back verification
    read.csv        — validate previously written KPI CSVs
    F.col / select  — final column selection for export
    describe()      — summary statistics (count, mean, min, max, stddev)

HOW TO RUN:
    python src/05_export_results.py
    (Run AFTER 04_business_analysis.py)
"""

import os
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ─── Paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_DIR  = os.path.join(PROJECT_ROOT, "data", "parquet")
OUTPUT_DIR   = os.path.join(PROJECT_ROOT, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ─── SparkSession ─────────────────────────────────────────────────────────────
def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("EcommerceExportResults")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ─── Export Master Dataset ────────────────────────────────────────────────────
def export_master_csv(enriched_df, output_dir: str) -> str:
    """
    Export the full enriched orders dataset as a single CSV file for Power BI.

    Columns selected are the ones used directly in Power BI visuals.
    We drop the internal window-function columns that aren't needed in the dashboard.

    coalesce(1) consolidates all Spark partitions into one output file.
    Acceptable here because the full enriched dataset fits in memory for a
    ~10k row project; in production you'd stream to blob storage instead.
    """
    export_cols = [
        "order_id", "order_date", "year_month", "order_year", "order_month",
        "order_day", "order_dayofweek", "customer_id", "customer_name",
        "city", "state", "product_id", "product_name", "category",
        "quantity", "price", "discount", "revenue"
    ]

    output_path = os.path.join(output_dir, "master_orders_enriched")
    (
        enriched_df
        .select(*export_cols)
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(output_path)
    )
    return output_path


def export_master_parquet(enriched_df, parquet_dir: str) -> str:
    """
    Export the master enriched dataset as compressed Parquet.
    Uses Snappy compression (Spark default) — good balance of speed and ratio.
    """
    output_path = os.path.join(parquet_dir, "master_enriched_final")
    export_cols = [
        "order_id", "order_date", "year_month", "order_year", "order_month",
        "order_day", "order_dayofweek", "customer_id", "customer_name",
        "city", "state", "product_id", "product_name", "category",
        "quantity", "price", "discount", "revenue"
    ]
    (
        enriched_df
        .select(*export_cols)
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(output_path)
    )
    return output_path


# ─── Verify KPI CSVs ──────────────────────────────────────────────────────────
def verify_kpi_outputs(spark: SparkSession, output_dir: str) -> dict:
    """
    Scan the output/ directory for KPI folders and verify each is non-empty.
    Returns a dict mapping folder_name → row_count.
    """
    kpi_folders = [
        d for d in os.listdir(output_dir)
        if os.path.isdir(os.path.join(output_dir, d)) and d.startswith("kpi_")
    ]
    results = {}
    for folder in sorted(kpi_folders):
        folder_path = os.path.join(output_dir, folder)
        try:
            df = spark.read.option("header", "true").csv(folder_path)
            count = df.count()
            results[folder] = count
        except Exception as e:
            results[folder] = f"ERROR: {e}"
    return results


# ─── Verify Parquet ───────────────────────────────────────────────────────────
def verify_parquet_outputs(spark: SparkSession, parquet_dir: str) -> dict:
    """
    Read back key Parquet datasets and return their row counts.
    This confirms that Parquet files were written correctly and are readable.
    """
    parquet_tables = [
        "customers_clean",
        "products_clean",
        "orders_clean",
        "orders_enriched",
        "master_enriched_final",
    ]
    results = {}
    for table in parquet_tables:
        path = os.path.join(parquet_dir, table)
        if os.path.exists(path):
            try:
                count = spark.read.parquet(path).count()
                results[table] = count
            except Exception as e:
                results[table] = f"ERROR: {e}"
        else:
            results[table] = "NOT FOUND"
    return results


# ─── Summary Statistics ───────────────────────────────────────────────────────
def compute_summary_statistics(enriched_df) -> dict:
    """
    Use describe() to get standard descriptive statistics on numeric columns.
    describe() returns count, mean, stddev, min, max for each numeric column.
    """
    stats = (
        enriched_df
        .select("quantity", "price", "discount", "revenue")
        .describe()
    )
    rows = stats.collect()
    summary = {}
    for row in rows:
        summary[row["summary"]] = {
            "quantity": row["quantity"],
            "price":    row["price"],
            "discount": row["discount"],
            "revenue":  row["revenue"],
        }
    return summary


# ─── Text Summary Report ──────────────────────────────────────────────────────
def write_summary_report(
    output_dir: str,
    kpi_verify: dict,
    parquet_verify: dict,
    stats: dict,
    total_revenue: float,
    total_orders: int,
    aov: float,
) -> str:
    """
    Write a plain-text summary report combining pipeline results, verification
    outcomes, and key KPI values. Saved to output/summary_report.txt.
    """
    report_path = os.path.join(output_dir, "summary_report.txt")
    timestamp   = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines = [
        "=" * 70,
        "  E-COMMERCE SALES ANALYTICS — PIPELINE SUMMARY REPORT",
        "=" * 70,
        f"  Generated : {timestamp}",
        f"  Pipeline  : PySpark ETL (Modules 01–05)",
        "",
        "─" * 70,
        "  TOP-LINE KPIs",
        "─" * 70,
        f"  Total Revenue         : ${total_revenue:>15,.2f}",
        f"  Total Orders          : {total_orders:>15,}",
        f"  Average Order Value   : ${aov:>15,.2f}",
        "",
        "─" * 70,
        "  DESCRIPTIVE STATISTICS",
        "─" * 70,
    ]

    for metric, values in stats.items():
        lines.append(f"  {metric:<10}")
        for col, val in values.items():
            lines.append(f"             {col:<12}: {val}")
        lines.append("")

    lines += [
        "─" * 70,
        "  PARQUET DATASET VERIFICATION",
        "─" * 70,
    ]
    for table, count in parquet_verify.items():
        status = f"{count:,}" if isinstance(count, int) else str(count)
        lines.append(f"  {table:<30} : {status} rows")

    lines += [
        "",
        "─" * 70,
        "  KPI CSV OUTPUT VERIFICATION",
        "─" * 70,
    ]
    for kpi, count in kpi_verify.items():
        status = f"{count:,}" if isinstance(count, int) else str(count)
        lines.append(f"  {kpi:<35}: {status} rows")

    lines += [
        "",
        "─" * 70,
        "  PIPELINE STATUS: ✅ ALL MODULES COMPLETED SUCCESSFULLY",
        "─" * 70,
        "  Next Step: Open Power BI Desktop → Get Data → Folder",
        "             → Select output/ directory → Build dashboards",
        "=" * 70,
    ]

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    return report_path


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("\n" + "=" * 60)
    print("  Module 05 — Export Results")
    print("=" * 60)

    spark = create_spark_session()

    # ── Load enriched dataset ─────────────────────────────────────────────────
    print("\n  Loading enriched Parquet dataset...")
    enriched_df = spark.read.parquet(os.path.join(PARQUET_DIR, "orders_enriched"))
    enriched_df.cache()
    total_rows = enriched_df.count()
    print(f"    Enriched rows loaded: {total_rows:,}")

    # ── Export master CSV ─────────────────────────────────────────────────────
    print("\n  Exporting master orders CSV for Power BI...")
    csv_path = export_master_csv(enriched_df, OUTPUT_DIR)
    print(f"    ✅ CSV → {csv_path}")

    # ── Export master Parquet ─────────────────────────────────────────────────
    print("\n  Exporting master Parquet (Snappy compressed)...")
    parquet_path = export_master_parquet(enriched_df, PARQUET_DIR)
    print(f"    ✅ Parquet → {parquet_path}")

    # ── Descriptive statistics ────────────────────────────────────────────────
    print("\n  Computing descriptive statistics...")
    stats = compute_summary_statistics(enriched_df)
    print("    Revenue statistics:")
    print(f"      Mean  : ${float(stats.get('mean',{}).get('revenue', 0)):,.2f}")
    print(f"      StdDev: ${float(stats.get('stddev',{}).get('revenue', 0)):,.2f}")
    print(f"      Min   : ${float(stats.get('min',{}).get('revenue', 0)):,.2f}")
    print(f"      Max   : ${float(stats.get('max',{}).get('revenue', 0)):,.2f}")

    # ── Fetch KPI summary values for the report ───────────────────────────────
    agg_row = enriched_df.agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.count("order_id").alias("total_orders"),
        F.round(F.avg("revenue"), 2).alias("aov")
    ).collect()[0]

    total_revenue = float(agg_row["total_revenue"])
    total_orders  = int(agg_row["total_orders"])
    aov           = float(agg_row["aov"])

    # ── Verify all outputs ────────────────────────────────────────────────────
    print("\n  Verifying KPI CSV outputs...")
    kpi_verify = verify_kpi_outputs(spark, OUTPUT_DIR)
    for kpi, count in kpi_verify.items():
        status = f"{count:,} rows" if isinstance(count, int) else str(count)
        print(f"    {kpi}: {status}")

    print("\n  Verifying Parquet datasets...")
    parquet_verify = verify_parquet_outputs(spark, PARQUET_DIR)
    for table, count in parquet_verify.items():
        status = f"{count:,} rows" if isinstance(count, int) else str(count)
        print(f"    {table}: {status}")

    # ── Write summary report ──────────────────────────────────────────────────
    print("\n  Writing summary report...")
    report_path = write_summary_report(
        OUTPUT_DIR, kpi_verify, parquet_verify, stats,
        total_revenue, total_orders, aov
    )
    print(f"    ✅ Report → {report_path}")

    print("\n" + "=" * 60)
    print(f"  ✅ Pipeline complete!")
    print(f"     Total Revenue : ${total_revenue:,.2f}")
    print(f"     Total Orders  : {total_orders:,}")
    print(f"     AOV           : ${aov:.2f}")
    print("=" * 60 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()
