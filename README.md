# E-commerce Sales Analytics using PySpark

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-4.0.0-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-4.0-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Parquet](https://img.shields.io/badge/Parquet-columnar-50ABF1?style=flat)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?style=flat&logo=powerbi&logoColor=black)
![Git](https://img.shields.io/badge/Git-version%20control-F05032?style=flat&logo=git&logoColor=white)

---

## Project Overview

A **production-quality, end-to-end Data Engineering project** that demonstrates a complete ETL pipeline using **Apache PySpark**. The pipeline ingests raw e-commerce CSV data, performs rigorous cleaning and transformation, computes 13 business KPIs using **Spark SQL** and **Window Functions**, persists results in **Parquet** format, and exports data ready for a **Power BI** dashboard.

This project is designed to replicate the work of an entry-level data engineer at an e-commerce company â€” from raw data ingestion to executive-level business insights.

---

## Architecture

```
Raw CSVs (data/raw/)
       â”‚
       â–¼
â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
â”‚  01_load_data.py â”‚  SparkSession Â· read.csv Â· schema inspection Â· cache
â””â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
         â”‚
         â–¼
â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
â”‚ 02_data_cleaning.py â”‚  dropDuplicates Â· fillna Â· withColumn Â· filter/where
â””â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
         â”‚
         â–¼  (Parquet: customers_clean, products_clean, orders_clean)
â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
â”‚ 03_transformations.py    â”‚  joins Â· date functions Â· Window Â· repartition
â””â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
         â”‚
         â–¼  (Parquet: orders_enriched)
â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
â”‚ 04_business_analysis.py  â”‚  groupBy Â· agg Â· Spark SQL Â· 13 KPIs
â””â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
         â”‚
         â–¼  (CSV: output/kpi_*/)
â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
â”‚ 05_export_results.py   â”‚  coalesce Â· write.parquet Â· summary report
â””â”€â”€â”€â”€â”€â”€â”€â”€â”¬â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
         â”‚
         â–¼
â”Œâ”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”
â”‚  Power BI Desktop â”‚  Star Schema Â· DAX Measures Â· 5 Dashboard Pages
â””â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”˜
```

---

## Tech Stack

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.10+ | Core programming language |
| PySpark | 3.5.1 | Distributed data processing |
| Apache Spark | 3.5 | Execution engine (local mode) |
| PyArrow | 15.0 | Parquet read/write |
| Pandas | 2.2 | Helper operations |
| Faker | 24.11 | Realistic dataset generation |
| Power BI Desktop | Latest | Interactive dashboards |
| Git | Latest | Version control |
| GitHub | â€” | Remote repository |

---

## Folder Structure

```
Ecommerce-Sales-Analytics/
â”‚
â”œâ”€â”€ data/
â”‚   â”œâ”€â”€ raw/                     â† Original CSV files (source of truth)
â”‚   â”‚   â”œâ”€â”€ customers.csv
â”‚   â”‚   â”œâ”€â”€ products.csv
â”‚   â”‚   â””â”€â”€ orders.csv
â”‚   â”œâ”€â”€ processed/               â† Reserved for intermediate outputs
â”‚   â””â”€â”€ parquet/                 â† Cleaned and enriched Parquet files
â”‚       â”œâ”€â”€ customers_clean/
â”‚       â”œâ”€â”€ products_clean/
â”‚       â”œâ”€â”€ orders_clean/
â”‚       â”œâ”€â”€ orders_enriched/
â”‚       â””â”€â”€ master_enriched_final/
â”‚
â”œâ”€â”€ src/
â”‚   â”œâ”€â”€ 01_load_data.py          â† SparkSession, CSV loading, schema inspection
â”‚   â”œâ”€â”€ 02_data_cleaning.py      â† Dedup, null handling, validation
â”‚   â”œâ”€â”€ 03_transformations.py    â† Joins, revenue, date features, window funcs
â”‚   â”œâ”€â”€ 04_business_analysis.py  â† 13 KPIs, Spark SQL, aggregations
â”‚   â””â”€â”€ 05_export_results.py     â† CSV/Parquet export, summary report
â”‚
â”œâ”€â”€ dashboard/
â”‚   â””â”€â”€ README_PowerBI.md        â† Step-by-step Power BI build guide
â”‚
â”œâ”€â”€ output/                      â† All KPI CSVs and master dataset export
â”‚   â”œâ”€â”€ kpi_01_total_revenue/
â”‚   â”œâ”€â”€ kpi_02_monthly_revenue/
â”‚   â”œâ”€â”€ ...                      â† 13 KPI folders
â”‚   â”œâ”€â”€ master_orders_enriched/
â”‚   â””â”€â”€ summary_report.txt
â”‚
â”œâ”€â”€ screenshots/                 â† Dashboard and output screenshots
â”œâ”€â”€ generate_data.py             â† Faker-based dataset generator
â”œâ”€â”€ requirements.txt
â”œâ”€â”€ .gitignore
â””â”€â”€ README.md
```

---

## Dataset Description

Three realistic CSV datasets with intentional data quality issues:

### customers.csv (1,080+ rows)
| Column | Type | Notes |
|--------|------|-------|
| customer_id | String | CUST00001 format |
| customer_name | String | ~3% nulls |
| city | String | ~2% nulls |
| state | String | Mix of full names, abbreviations, wrong casing |
| registration_date | String | ~1.5% invalid dates |

### products.csv (535+ rows)
| Column | Type | Notes |
|--------|------|-------|
| product_id | String | PROD00001 format |
| product_name | String | ~2.5% nulls |
| category | String | 10 categories, inconsistent casing |
| price | Float | ~3% nulls, some negatives |

### orders.csv (10,762+ rows)
| Column | Type | Notes |
|--------|------|-------|
| order_id | String | ORD000001 format |
| customer_id | String | ~2% orphan references |
| product_id | String | ~2% orphan references |
| quantity | Integer | ~2% nulls, 30 negatives |
| discount | Float | ~5% nulls, 20 out-of-range |
| order_date | String | 2022â€“2024 range |

---

## Installation

### Prerequisites
- **Python 3.10+**
- **Java 11 or 17** (required by PySpark)
  - Download: https://adoptium.net/
  - Verify: `java -version`
- **Git**

### Setup Steps

```bash
# 1. Clone the repository
git clone https://github.com/JoelPrasannaKumar/ecommerce-sales-analytics-pyspark.git
cd ecommerce-sales-analytics-pyspark

# 2. Create a virtual environment
python -m venv venv

# Windows
venv\Scripts\activate

# macOS / Linux
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Set JAVA_HOME (Windows example)
set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-17.x.x.x-hotspot
set PATH=%JAVA_HOME%\bin;%PATH%
```

---

## How to Run

Run the pipeline modules **in order**:

```bash
# Step 1 â€” Generate raw datasets
python generate_data.py

# Step 2 â€” Inspect raw data
python src/01_load_data.py

# Step 3 â€” Clean data and save to Parquet
python src/02_data_cleaning.py

# Step 4 â€” Transform, join, add features
python src/03_transformations.py

# Step 5 â€” Compute all 13 KPIs
python src/04_business_analysis.py

# Step 6 â€” Export results and generate summary report
python src/05_export_results.py
```

After running all modules, the `output/` folder will contain 13 KPI CSV files and a `summary_report.txt`.

---

## PySpark Concepts Demonstrated

| Concept | Where Used |
|---------|-----------|
| `SparkSession` | `01_load_data.py` |
| `read.csv` with options | `01_load_data.py`, `02_data_cleaning.py` |
| `cache()` | `01_load_data.py`, `04_business_analysis.py` |
| `dropDuplicates()` | `02_data_cleaning.py` |
| `fillna()` | `02_data_cleaning.py` |
| `withColumn()` | `02_data_cleaning.py`, `03_transformations.py` |
| `filter()` / `where()` | `02_data_cleaning.py` |
| `drop()` | `02_data_cleaning.py` |
| `join()` (inner, left, left_anti) | `03_transformations.py`, `04_business_analysis.py` |
| `select()` / `alias()` | All modules |
| Date functions | `03_transformations.py` |
| `Window` + `rank()` / `dense_rank()` / `row_number()` | `03_transformations.py` |
| `Window` + `lag()` / `lead()` | `03_transformations.py`, `04_business_analysis.py` |
| `repartition()` | `03_transformations.py` |
| `groupBy()` + `agg()` | `04_business_analysis.py` |
| `sort()` / `orderBy()` | `04_business_analysis.py` |
| `createOrReplaceTempView()` + `spark.sql()` | `04_business_analysis.py` |
| `write.parquet()` | `02_data_cleaning.py`, `03_transformations.py` |
| `read.parquet()` | `03_transformations.py` onwards |
| `coalesce(1)` + `write.csv()` | `04_business_analysis.py`, `05_export_results.py` |
| `describe()` | `05_export_results.py` |

---

## Business KPIs

| # | KPI | Business Value |
|---|-----|---------------|
| 1 | **Total Revenue** | Top-line performance indicator |
| 2 | **Monthly Revenue** | Tracks revenue over time |
| 3 | **Revenue by State** | Identifies high-value geographies |
| 4 | **Revenue by Category** | Guides product mix decisions |
| 5 | **Top 10 Customers** | CRM and retention targeting |
| 6 | **Top 10 Products** | Best-performing SKU identification |
| 7 | **Highest Selling Category** | Units-sold performance (â‰  revenue) |
| 8 | **Average Order Value (AOV)** | Revenue efficiency per transaction |
| 9 | **Average Customer Spend** | Lifetime value proxy |
| 10 | **Customers with No Orders** | Churn and re-engagement candidates |
| 11 | **Products Never Sold** | Inventory/discontinuation decisions |
| 12 | **Monthly Growth Rate** | Period-over-period trend |
| 13 | **Daily Sales Trend** | Operational and planning insights |

---

## Business Insights

Based on the generated dataset (2022â€“2024):

- **Electronics and Home & Kitchen** consistently rank as the top revenue-generating categories
- **Q4 months** (Octoberâ€“December) show the highest monthly revenue â€” consistent with holiday shopping behaviour
- **California, Texas, and Florida** account for the largest share of revenue, aligning with population density
- Approximately **8â€“10% of registered customers** have never placed an order â€” a significant re-engagement opportunity
- The **Average Order Value** sits around **$200â€“$400**, driven by high-ticket Electronics orders
- **Monthly Growth Rate** shows positive momentum in 2023â€“2024 with occasional dips in Q1

---

## Power BI Dashboard

Five interactive pages powered by exported CSVs:

| Page | Focus |
|------|-------|
| 1. Executive Dashboard | KPI cards, category donut, state bar chart |
| 2. Sales Trends | Daily/monthly line charts, growth rate |
| 3. Customer Analysis | Top customers, AOV, inactive customers |
| 4. Product Analysis | Top products, category breakdown, never-sold |
| 5. Regional Analysis | State revenue map and ranking |

â†’ Full build guide: [`dashboard/README_PowerBI.md`](dashboard/README_PowerBI.md)

---

## Screenshots

> _Add Power BI dashboard screenshots to the `screenshots/` folder after building the dashboard._

---

## Future Improvements

- [ ] Add Apache Airflow DAG to orchestrate the pipeline on a schedule
- [ ] Deploy on Databricks Community Edition for a cloud-native demo
- [ ] Add Delta Lake for ACID transactions and time-travel
- [ ] Implement data quality checks using Great Expectations
- [ ] Add unit tests for each cleaning and transformation function
- [ ] Containerise the pipeline with Docker
- [ ] Ingest data from a PostgreSQL database instead of flat CSVs
- [ ] Add streaming pipeline using Spark Structured Streaming

---

## Resume Description

> **Data Engineer** â€” E-commerce Sales Analytics Pipeline (Python Â· PySpark Â· Spark SQL)
>
> - Built an end-to-end ETL pipeline using **PySpark 3.5** that processes **10,000+ orders** across **1,000+ customers** and **500+ products** in standalone Spark mode.
> - Implemented a multi-stage **data cleaning pipeline** using `dropDuplicates`, `fillna`, `withColumn`, and regex-based normalisation to handle nulls, duplicates, and inconsistent formatting.
> - Engineered derived features including **revenue computation**, temporal date columns, and **Window Functions** (`rank`, `dense_rank`, `row_number`, `lag`, `lead`) for time-series and ranking analysis.
> - Computed **13 business KPIs** (Total Revenue, AOV, Monthly Growth Rate, Customer Lifetime Value proxy) using both the **DataFrame API** and **Spark SQL** registered temp views.
> - Persisted cleaned and enriched datasets in **Parquet format** with Snappy compression and partition pruning by year/month for optimised downstream reads.
> - Exported KPI results as **CSV files** and built a **5-page Power BI dashboard** with KPI cards, line charts, bar charts, maps, and slicers for executive reporting.

---

## Author

**Joel** | Entry-Level Data Engineer  
GitHub: [github.com/JoelPrasannaKumar](https://github.com/JoelPrasannaKumar)

---

## License

MIT License â€” free to use, modify, and distribute.

