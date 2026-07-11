# Power BI Dashboard — Setup & Build Guide

## Overview

This guide walks you through connecting Power BI Desktop to the project's exported CSV files and building all five dashboard pages step by step.

> **Prerequisites**
> - Power BI Desktop installed (free from [microsoft.com/powerbi](https://powerbi.microsoft.com/desktop))
> - PySpark pipeline has been run and `output/` folder is populated
> - The `output/master_orders_enriched/` folder contains a `.csv` file

---

## Step 1 — Connect Power BI to the Data

1. Open **Power BI Desktop**
2. Click **Home → Get Data → Folder**
3. Navigate to the `output/` directory of this project
4. Click **Combine & Transform Data**
5. Power BI will auto-detect all CSV files inside subfolders
6. In the Power Query Editor, keep only rows where the `Source.Name` starts with `part-` (the actual Spark part files)
7. Promote headers and set correct data types

### Alternatively (recommended for beginners):

Load each KPI CSV individually:

| Table Name | Source Folder |
|------------|--------------|
| `MasterOrders` | `output/master_orders_enriched/` |
| `MonthlyRevenue` | `output/kpi_02_monthly_revenue/` |
| `RevenueByState` | `output/kpi_03_revenue_by_state/` |
| `RevenueByCategory` | `output/kpi_04_revenue_by_category/` |
| `Top10Customers` | `output/kpi_05_top10_customers/` |
| `Top10Products` | `output/kpi_06_top10_products/` |
| `GrowthRate` | `output/kpi_12_monthly_growth_rate/` |
| `DailyTrend` | `output/kpi_13_daily_sales_trend/` |

---

## Step 2 — Data Model (Star Schema)

```
                     ┌─────────────────┐
                     │   DimCustomers  │
                     │─────────────────│
                     │ customer_id (PK)│
                     │ customer_name   │
                     │ city            │
                     │ state           │
                     └────────┬────────┘
                              │ 1:Many
                     ┌────────▼────────────────┐
  ┌───────────────┐  │      FactOrders          │  ┌───────────────┐
  │  DimProducts  │  │──────────────────────────│  │  DimDate      │
  │───────────────│  │ order_id (PK)            │  │───────────────│
  │ product_id(PK)│◄─┤ customer_id (FK)         ├─►│ order_date(PK)│
  │ product_name  │  │ product_id (FK)          │  │ year_month    │
  │ category      │  │ quantity                 │  │ order_year    │
  │ price         │  │ price                    │  │ order_month   │
  └───────────────┘  │ discount                 │  │ order_day     │
                     │ revenue                  │  └───────────────┘
                     │ order_date (FK)          │
                     └──────────────────────────┘
```

In Power BI's **Model view**:
- Set `MasterOrders[order_date]` → relationship with a Date table
- All relationships should be **Many-to-One** from `MasterOrders` to dimension tables

---

## Step 3 — Create Measures (DAX)

Create the following measures in the `MasterOrders` table:

```dax
Total Revenue = SUM(MasterOrders[revenue])

Total Orders = COUNT(MasterOrders[order_id])

Average Order Value = DIVIDE([Total Revenue], [Total Orders], 0)

Total Units Sold = SUM(MasterOrders[quantity])

Revenue MoM Growth =
VAR CurrentRevenue = [Total Revenue]
VAR PrevRevenue =
    CALCULATE(
        [Total Revenue],
        DATEADD(MasterOrders[order_date], -1, MONTH)
    )
RETURN
    DIVIDE(CurrentRevenue - PrevRevenue, PrevRevenue, 0) * 100
```

---

## Step 4 — Dashboard Pages

### Page 1: Executive Dashboard

**Visuals:**
| Visual Type | Data |
|------------|------|
| KPI Card | Total Revenue |
| KPI Card | Total Orders |
| KPI Card | Average Order Value |
| KPI Card | Total Units Sold |
| Donut Chart | Revenue by Category |
| Bar Chart | Top 10 States by Revenue |
| Line Chart | Monthly Revenue (last 12 months) |

**Slicers:** Year, State, Category

---

### Page 2: Sales Trends

**Visuals:**
| Visual Type | Data |
|------------|------|
| Line Chart | Daily Revenue Trend |
| Line Chart | Monthly Revenue + Growth Rate |
| Clustered Bar | Revenue by Day of Week |
| Waterfall Chart | Month-over-Month Revenue Change |
| Table | Monthly Revenue + Growth Rate % |

**Slicers:** Year, Month, Category

---

### Page 3: Customer Analysis

**Visuals:**
| Visual Type | Data |
|------------|------|
| Table | Top 10 Customers (revenue, orders, AOV) |
| KPI Card | Average Customer Spend |
| KPI Card | Customers with No Orders (count) |
| Bar Chart | Orders per Customer (binned) |
| Map | Customers by State (bubble size = revenue) |

**Slicers:** State, Year

---

### Page 4: Product Analysis

**Visuals:**
| Visual Type | Data |
|------------|------|
| Bar Chart | Top 10 Products by Revenue |
| Pie Chart | Revenue Share by Category |
| Table | Top 10 Products (units, revenue, avg price) |
| KPI Card | Products Never Sold (count) |
| Bar Chart | Units Sold by Category |

**Slicers:** Category, Year

---

### Page 5: Regional Analysis

**Visuals:**
| Visual Type | Data |
|------------|------|
| Map / Filled Map | Revenue by State (colour = revenue) |
| Bar Chart | Top 10 States by Revenue |
| Bar Chart | Orders by State |
| Table | State Revenue Breakdown |
| KPI Card | Top State (by revenue) |

**Slicers:** Year, Category

---

## Step 5 — Formatting Tips

- **Theme**: Use a dark or professional theme (View → Themes → Executive or Modern)
- **Colors**: Use a consistent color palette per category
- **Fonts**: Use Segoe UI (Power BI default) at 12pt for body, 18pt for KPI values
- **KPI Cards**: Show actual value + comparison to previous period
- **Page Navigation**: Add a navigation bar with page buttons on the left side

---

## Step 6 — Publishing (Optional)

1. Sign in to **Power BI Service** (app.powerbi.com)
2. Click **Publish** in Power BI Desktop
3. Select your workspace
4. Share the dashboard link with stakeholders

---

## File Checklist

Before opening Power BI, confirm these files exist:

```
output/
├── master_orders_enriched/     ← part-*.csv file inside
├── kpi_02_monthly_revenue/
├── kpi_03_revenue_by_state/
├── kpi_04_revenue_by_category/
├── kpi_05_top10_customers/
├── kpi_06_top10_products/
├── kpi_12_monthly_growth_rate/
├── kpi_13_daily_sales_trend/
└── summary_report.txt
```
