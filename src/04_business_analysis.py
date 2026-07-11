"""
04_business_analysis.py
------------------------
Module 4 of 5 -- Business KPI Analysis

WHAT THIS MODULE DOES:
    Computes all 13 business KPIs using both:
      * Spark DataFrame API  (functional, type-safe)
      * Spark SQL            (familiar SQL syntax)

    KPIs:
     1.  Total Revenue
     2.  Monthly Revenue
     3.  Revenue by State
     4.  Revenue by Category
     5.  Top 10 Customers
     6.  Top 10 Products
     7.  Highest Selling Category (units)
     8.  Average Order Value (AOV)
     9.  Average Customer Spend
    10.  Customers with No Orders
    11.  Products Never Sold
    12.  Monthly Growth Rate
    13.  Daily Sales Trend

KEY PYSPARK CONCEPTS DEMONSTRATED:
    createOrReplaceTempView  -- register DataFrame as SQL view
    spark.sql()              -- execute SQL against registered views
    groupBy + agg            -- aggregate grouped data
    sort / orderBy           -- sort results ascending/descending
    F.sum / F.avg / F.count  -- aggregate functions
    F.round                  -- round numeric results
    Window + lag()           -- period-over-period growth
    left anti join           -- find unmatched records

HOW TO RUN:
    python src/04_business_analysis.py
    (Run AFTER 03_transformations.py)
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

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- Paths -------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_DIR  = os.path.join(PROJECT_ROOT, "data", "parquet")
OUTPUT_DIR   = os.path.join(PROJECT_ROOT, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("EcommerceBusinessAnalysis")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def save_kpi_csv(df, output_dir: str, filename: str) -> None:
    """
    Collect Spark DataFrame to pandas, then write a single clean CSV.
    coalesce(1) on Spark + collect -> toPandas avoids Spark worker CSV writer
    while producing the same single-file output Power BI expects.
    """
    path = os.path.join(output_dir, filename + ".csv")
    df.toPandas().to_csv(path, index=False)
    print(f"    Saved -> {path}")


# ---------------------------------------------------------------------------
# KPI FUNCTIONS
# ---------------------------------------------------------------------------

def kpi_01_total_revenue(enriched_df, spark):
    """KPI 1 -- Total Revenue: SUM(revenue) across all orders."""
    result = enriched_df.agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.count("order_id").alias("total_orders")
    )
    val = result.collect()[0]["total_revenue"]
    print(f"\n  KPI 01 -- Total Revenue      : ${val:,.2f}")
    return result


def kpi_02_monthly_revenue(enriched_df, spark):
    """KPI 2 -- Monthly Revenue grouped by year_month, sorted chronologically."""
    result = spark.sql("""
        SELECT
            year_month,
            month_name,
            ROUND(SUM(revenue), 2)  AS monthly_revenue,
            COUNT(order_id)          AS order_count
        FROM orders_enriched
        GROUP BY year_month, month_name
        ORDER BY year_month ASC
    """)
    print(f"  KPI 02 -- Monthly Revenue    : {result.count()} months")
    return result


def kpi_03_revenue_by_state(enriched_df, spark):
    """KPI 3 -- Revenue by State for regional performance analysis."""
    result = (
        enriched_df
        .filter(F.col("state").isNotNull())
        .groupBy("state")
        .agg(
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.count("order_id").alias("order_count"),
            F.countDistinct("customer_id").alias("unique_customers")
        )
        .sort(F.col("total_revenue").desc())
    )
    print(f"  KPI 03 -- Revenue by State   : {result.count()} states")
    return result


def kpi_04_revenue_by_category(enriched_df, spark):
    """KPI 4 -- Revenue by Category to guide product mix decisions."""
    result = spark.sql("""
        SELECT
            category,
            ROUND(SUM(revenue), 2)   AS total_revenue,
            COUNT(order_id)           AS order_count,
            ROUND(AVG(revenue), 2)   AS avg_order_revenue
        FROM orders_enriched
        GROUP BY category
        ORDER BY total_revenue DESC
    """)
    print(f"  KPI 04 -- Revenue by Category: {result.count()} categories")
    return result


def kpi_05_top10_customers(enriched_df, spark):
    """KPI 5 -- Top 10 Customers for CRM and retention targeting."""
    result = (
        enriched_df
        .filter(F.col("customer_name").isNotNull())
        .groupBy("customer_id", "customer_name", "state")
        .agg(
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.count("order_id").alias("order_count"),
            F.round(F.avg("revenue"), 2).alias("avg_order_value")
        )
        .orderBy(F.col("total_revenue").desc())
        .limit(10)
    )
    print(f"  KPI 05 -- Top 10 Customers")
    return result


def kpi_06_top10_products(enriched_df, spark):
    """KPI 6 -- Top 10 Products for inventory and pricing decisions."""
    result = spark.sql("""
        SELECT
            product_id, product_name, category,
            ROUND(SUM(revenue), 2)      AS total_revenue,
            SUM(quantity)                AS units_sold,
            ROUND(AVG(revenue), 2)      AS avg_revenue_per_order
        FROM orders_enriched
        GROUP BY product_id, product_name, category
        ORDER BY total_revenue DESC
        LIMIT 10
    """)
    print(f"  KPI 06 -- Top 10 Products")
    return result


def kpi_07_highest_selling_category(enriched_df, spark):
    """KPI 7 -- Highest Selling Category by units sold (not just revenue)."""
    result = spark.sql("""
        SELECT
            category,
            SUM(quantity)                AS total_units_sold,
            ROUND(SUM(revenue), 2)      AS total_revenue
        FROM orders_enriched
        GROUP BY category
        ORDER BY total_units_sold DESC
    """)
    top = result.collect()[0]["category"]
    print(f"  KPI 07 -- Highest Selling Category: {top}")
    return result


def kpi_08_average_order_value(enriched_df, spark):
    """
    KPI 8 -- Average Order Value (AOV).
    AOV = Total Revenue / Total Orders.
    A core e-commerce health metric.
    """
    result = enriched_df.agg(
        F.round(F.avg("revenue"), 2).alias("average_order_value"),
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.count("order_id").alias("total_orders")
    )
    val = result.collect()[0]["average_order_value"]
    print(f"  KPI 08 -- Average Order Value: ${val:,.2f}")
    return result


def kpi_09_avg_customer_spend(enriched_df, spark):
    """KPI 9 -- Average Customer Spend (Lifetime Value proxy)."""
    customer_spend = (
        enriched_df
        .groupBy("customer_id")
        .agg(F.round(F.sum("revenue"), 2).alias("customer_total_spend"))
    )
    result = customer_spend.agg(
        F.round(F.avg("customer_total_spend"), 2).alias("avg_customer_spend"),
        F.round(F.min("customer_total_spend"), 2).alias("min_customer_spend"),
        F.round(F.max("customer_total_spend"), 2).alias("max_customer_spend")
    )
    val = result.collect()[0]["avg_customer_spend"]
    print(f"  KPI 09 -- Avg Customer Spend : ${val:,.2f}")
    return result


def kpi_10_customers_no_orders(customers_df, enriched_df, spark):
    """
    KPI 10 -- Customers with No Orders.
    Uses LEFT ANTI JOIN: returns rows from customers with NO match in orders.
    """
    ordered = enriched_df.select("customer_id").distinct()
    result = (
        customers_df
        .join(ordered, on="customer_id", how="left_anti")
        .select("customer_id", "customer_name", "state", "registration_date")
        .orderBy("registration_date")
    )
    count = result.count()
    print(f"  KPI 10 -- Customers No Orders: {count:,} customers")
    return result


def kpi_11_products_never_sold(products_df, enriched_df, spark):
    """
    KPI 11 -- Products Never Sold.
    LEFT ANTI JOIN between products and orders.
    Candidates for discontinuation or promotion.
    """
    sold = enriched_df.select("product_id").distinct()
    result = (
        products_df
        .join(sold, on="product_id", how="left_anti")
        .select("product_id", "product_name", "category", "price")
        .orderBy("category", "product_name")
    )
    count = result.count()
    print(f"  KPI 11 -- Products Never Sold: {count:,} products")
    return result


def kpi_12_monthly_growth_rate(monthly_revenue_df, spark):
    """
    KPI 12 -- Monthly Revenue Growth Rate.
    Growth = (Current - Previous) / Previous * 100.
    Uses lag() window function over the time-ordered monthly revenue.
    """
    time_window = Window.orderBy("year_month")
    result = (
        monthly_revenue_df
        .withColumn("prev_month_revenue", F.lag("monthly_revenue", 1).over(time_window))
        .withColumn(
            "growth_rate_pct",
            F.when(
                F.col("prev_month_revenue").isNotNull() & (F.col("prev_month_revenue") != 0),
                F.round(
                    (F.col("monthly_revenue") - F.col("prev_month_revenue"))
                    / F.col("prev_month_revenue") * 100, 2
                )
            ).otherwise(None)
        )
        .select("year_month", "month_name", "monthly_revenue", "prev_month_revenue", "growth_rate_pct")
        .orderBy("year_month")
    )
    print(f"  KPI 12 -- Monthly Growth Rate: computed")
    return result


def kpi_13_daily_sales_trend(enriched_df, spark):
    """KPI 13 -- Daily Sales Trend for operational planning and time-series charts."""
    result = spark.sql("""
        SELECT
            order_date, order_year, order_month, order_day,
            ROUND(SUM(revenue), 2)  AS daily_revenue,
            COUNT(order_id)          AS daily_orders,
            ROUND(AVG(revenue), 2)  AS avg_daily_order_value
        FROM orders_enriched
        GROUP BY order_date, order_year, order_month, order_day
        ORDER BY order_date ASC
    """)
    print(f"  KPI 13 -- Daily Sales Trend  : {result.count():,} days")
    return result


# --- Main --------------------------------------------------------------------
def main():
    print("\n" + "=" * 60)
    print("  Module 04 -- Business Analysis (13 KPIs)")
    print("=" * 60)

    spark = create_spark_session()

    print("\n  Loading enriched Parquet dataset...")
    enriched_df  = spark.read.parquet(os.path.join(PARQUET_DIR, "orders_enriched.parquet"))
    customers_df = spark.read.parquet(os.path.join(PARQUET_DIR, "customers_clean.parquet"))
    products_df  = spark.read.parquet(os.path.join(PARQUET_DIR, "products_clean.parquet"))

    enriched_df.cache()
    customers_df.cache()
    products_df.cache()

    # Register Spark SQL temp views
    # createOrReplaceTempView() makes a DataFrame queryable via spark.sql().
    enriched_df.createOrReplaceTempView("orders_enriched")
    customers_df.createOrReplaceTempView("customers")
    products_df.createOrReplaceTempView("products")

    print("\n  Computing KPIs...\n")
    total_revenue_df       = kpi_01_total_revenue(enriched_df, spark)
    monthly_revenue_df     = kpi_02_monthly_revenue(enriched_df, spark)
    revenue_by_state_df    = kpi_03_revenue_by_state(enriched_df, spark)
    revenue_by_category_df = kpi_04_revenue_by_category(enriched_df, spark)
    top10_customers_df     = kpi_05_top10_customers(enriched_df, spark)
    top10_products_df      = kpi_06_top10_products(enriched_df, spark)
    highest_category_df    = kpi_07_highest_selling_category(enriched_df, spark)
    avg_order_value_df     = kpi_08_average_order_value(enriched_df, spark)
    avg_customer_spend_df  = kpi_09_avg_customer_spend(enriched_df, spark)
    no_orders_df           = kpi_10_customers_no_orders(customers_df, enriched_df, spark)
    never_sold_df          = kpi_11_products_never_sold(products_df, enriched_df, spark)
    growth_rate_df         = kpi_12_monthly_growth_rate(monthly_revenue_df, spark)
    daily_trend_df         = kpi_13_daily_sales_trend(enriched_df, spark)

    print("\n  Saving KPI results to output/ as CSV...")
    kpi_map = {
        "kpi_01_total_revenue":       total_revenue_df,
        "kpi_02_monthly_revenue":     monthly_revenue_df,
        "kpi_03_revenue_by_state":    revenue_by_state_df,
        "kpi_04_revenue_by_category": revenue_by_category_df,
        "kpi_05_top10_customers":     top10_customers_df,
        "kpi_06_top10_products":      top10_products_df,
        "kpi_07_highest_category":    highest_category_df,
        "kpi_08_avg_order_value":     avg_order_value_df,
        "kpi_09_avg_customer_spend":  avg_customer_spend_df,
        "kpi_10_customers_no_orders": no_orders_df,
        "kpi_11_products_never_sold": never_sold_df,
        "kpi_12_monthly_growth_rate": growth_rate_df,
        "kpi_13_daily_sales_trend":   daily_trend_df,
    }

    for filename, df in kpi_map.items():
        save_kpi_csv(df, OUTPUT_DIR, filename)

    print("\nModule 04 complete -- all 13 KPIs saved to output/")
    spark.stop()


if __name__ == "__main__":
    main()


