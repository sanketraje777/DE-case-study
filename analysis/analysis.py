# %% [markdown]
# # Data Analysis Pipeline
#
# **Table:** `dm.sales_order_item_flat`  
# **Database URL:** `postgresql+psycopg2://postgres:pg123@db-postgres:5432/data_warehouse`
#
# **Description:**  
# This notebook performs a comprehensive data analysis pipeline on the `dm.sales_order_item_flat` table. It covers:
# 1. Configuration and environment setup  
# 2. Database connection  
# 3. Data ingestion (with chunking for memory efficiency)  
# 4. Data validation & quality checks  
# 5. Exploratory Data Analysis (EDA)  
# 6. Feature engineering (including date features, RFM metrics)  
# 7. Advanced analyses (RFM segmentation, customer lifetime value)  
# 8. Visualizations (sales trends, product & customer insights)  
# 9. Saving cleaned data & summary tables (CSV + writing back to Postgres)  
# 10. Logging, error handling, and best practices for a production environment  
#
# **How to use:**  
# - Ensure you have Python 3.7+ installed in a virtual environment.  
# - Activate your venv and install required packages:
#   ```
#   pip install pandas sqlalchemy psycopg2-binary matplotlib seaborn scikit-learn ydata-profiling ipywidgets
#   ```  
#   or 
#   ```
#   pip install -r requirements.txt
#   ```  
# - Execute cells sequentially in JupyterLab or VS Code (with the “Run Cell” feature).  
# - Adjust parameters (e.g., date range filters) as needed in the “Parameters” cell below.
#

# %% [markdown]
# ## 1. Parameters and Configuration
#
# Define global parameters to control filtering, chunk size, and file paths.
# These can later be overridden if you use `papermill` for parametrization in production.

# %%
# Parameters (can be overridden via papermill or direct editing)
DB_URL           = "postgresql+psycopg2://postgres:pg123@localhost:5432/data_warehouse"
CHUNK_SIZE       = 100_000                   # Number of rows per chunk when reading
DATE_FILTER_FROM = "2016-11-23"              # Only load orders from this date onward
OUTPUT_DIR       = "./outputs"               # Directory to save CSV/Parquet outputs
SUMMARY_TABLE    = "monthly_sales_summary"   # Name of summary table to write back to Postgres
SCHEMA           = "analysis"
DM_TABLE         = "sales_order_item_flat"
DM_SCHEMA        = "dm"

# Create OUTPUT_DIR if it doesn’t exist
import os
os.makedirs(OUTPUT_DIR, exist_ok=True)

# %% [markdown]
# ## 2. Imports & Environment Setup
#
# Import essential libraries and configure plotting styles, logging, and warnings.

# %%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import matplotlib.dates as mdates
import seaborn as sns
from textwrap import fill 
import logging
import sys
from sqlalchemy import create_engine, text
from sklearn.preprocessing import LabelEncoder
from scipy import stats
import statsmodels.api as sm
from statsmodels.tsa.arima.model import ARIMA

# Plot configuration
sns.set_theme(style="whitegrid", palette="muted")
plt.rcParams["figure.figsize"] = (12, 6)

# Suppress overly verbose warnings
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

# %% [markdown]
# ## 3. Logging Configuration
#
# Set up a logger so that any errors, warnings, or info messages are captured in a log file.
# In production, you can redirect these to a centralized logging service.

# %%
# Configure a root logger
logger = logging.getLogger("sales_analysis")
logger.setLevel(logging.INFO)

# Formatter for console and file handlers
formatter = logging.Formatter(
    fmt="%(asctime)s — %(name)s — %(levelname)s — \n%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# File handler (rotating daily logs can be added if needed)
log_file = os.path.join(OUTPUT_DIR, "sales_analysis.log")
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

logger.info("Logger initialized. Starting sales analysis pipeline.")

# %% [markdown]
# ## 4. Database Connection
#
# Create a SQLAlchemy engine and test the connection. We’ll wrap this in a function to handle retries/failures gracefully.

# %%
def get_engine(db_url: str):
    """
    Create and return a SQLAlchemy engine, with minimal retry logic.
    """
    try:
        engine = create_engine(db_url, pool_pre_ping=True)
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Successfully connected to the database.")
        return engine
    except Exception as e:
        logger.exception("Failed to connect to the database.")
        raise

engine = get_engine(DB_URL)

# %% [markdown]
# ## 5. Data Ingestion (Chunked Read)
#
# To avoid memory issues, we read in chunks from Postgres, filter by `order_created_at >= DATE_FILTER_FROM`, and concatenate.
# We also perform type enforcement and basic validation as we load.


# %%
def load_data(engine, chunk_size: int = 100_000, date_from: str = None):
    """
    Load data from dm.sales_order_item_flat in chunked fashion.
    Returns a concatenated DataFrame.
    """
    sql_base = f"""
        SELECT
            item_id,
            order_id,
            order_number,
            order_created_at,
            order_total,
            total_qty_ordered,
            customer_id,
            customer_name,
            customer_gender,
            customer_email,
            product_id,
            product_sku,
            product_name,
            item_price,
            item_qty_order,
            item_unit_total
        FROM {DM_SCHEMA}.{DM_TABLE}
    """
    if date_from:
        sql_base += f" WHERE order_created_at >= {date_from}"
    sql_base += ";"

    # ─── Use engine.raw_connection() + pd.read_sql_query(plain_string, conn) ────────
    #     This ensures pandas sees a true DBAPI connection (psycopg2), so .cursor() exists.
    raw_conn = engine.raw_connection()
    try:
        # pd.read_sql_query expects a SQL string when you're passing a raw DBAPI connection.
        chunks = pd.read_sql_query(
            sql_base, con=raw_conn, chunksize=chunk_size,
            parse_dates=["order_created_at"])
        df = pd.concat(chunks, ignore_index=True)
    except Exception as e:
        logger.exception("Error during data loading.")
        raise
    finally:
        raw_conn.close()
    return df

# Load the data
df = load_data(engine, chunk_size=CHUNK_SIZE)

# ─── 5. Inspect the Result ────────────────────────────────────────────────────────
print("Loaded rows:", len(df))
print(df.dtypes)
print(df.head())
print(df.info())


# %% [markdown]
# ## 6. Data Validation & Quality Checks
#
# Perform checks on column data types, missing values, and business rules (e.g., non-negative totals).

# %%
def validate_data(df: pd.DataFrame):
    """
    Run basic data quality checks on df:
    1. Check for nulls in NOT NULL columns
    2. Check business rules (e.g., order_total >= 0)
    3. Report invalid rows if any
    """
    errors = []

    # 1. Null checks
    not_null_cols = [
        "item_id","order_id","order_number","order_created_at","order_total",
        "total_qty_ordered","customer_id","customer_name","customer_email",
        "product_id","product_sku","product_name","item_price",
        "item_qty_order","item_unit_total"
    ]
    null_counts = df[not_null_cols].isnull().sum()
    for col, cnt in null_counts.items():
        if cnt > 0:
            errors.append(f"Column `{col}` has {cnt} null values but is defined NOT NULL.")

    # 2. Business-rule checks
    invalid_totals = df[df["order_total"] < 0]
    if len(invalid_totals) > 0:
        errors.append(f"Found {len(invalid_totals)} rows with order_total < 0.")

    invalid_qty = df[df["total_qty_ordered"] < 0]
    if len(invalid_qty) > 0:
        errors.append(f"Found {len(invalid_qty)} rows with total_qty_ordered < 0.")

    invalid_item_price = df[df["item_price"] < 0]
    if len(invalid_item_price) > 0:
        errors.append(f"Found {len(invalid_item_price)} rows with item_price < 0.")

    invalid_unit_total = df[df["item_unit_total"] < 0]
    if len(invalid_unit_total) > 0:
        errors.append(f"Found {len(invalid_unit_total)} rows with item_unit_total < 0.")

    # 3. customer_gender constraints
    invalid_gender = df[~df["customer_gender"].isin(["Female", "Male"]) & df["customer_gender"].notna()]
    if len(invalid_gender) > 0:
        errors.append(f"Found {len(invalid_gender)} rows with invalid customer_gender values.")

    # 4. email validation via regex
    email_pattern = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    invalid_emails = df[~df["customer_email"].str.match(email_pattern, na=False)]
    if len(invalid_emails) > 0:
        errors.append(f"Found {len(invalid_emails)} rows with invalid customer_email values.")

    # Summarize
    if errors:
        for err in errors:
            logger.error(err)
        raise ValueError("Data validation failed. Check log for details.")
    else:
        logger.info("Data validation passed. No issues found.")

# Run validation
validate_data(df)

# %% [markdown]
# ## 7. Exploratory Data Analysis (EDA)
#
# Get a high-level understanding of the data: distributions, summary statistics, and unique counts.

# %%
# Basic shape & structure
logger.info(f"DataFrame shape: {df.shape}")
logger.info(f"DataFrame columns:\n{df.dtypes}")

# Display first 5 rows in the notebook
df.head()

# %%
# Summary statistics for numeric columns
numeric_stats = df[[
    "order_total","total_qty_ordered","item_price","item_qty_order","item_unit_total"
]].describe().T

logger.info("Numeric summary statistics:")
numeric_stats

# %%
cols = ["order_total", "total_qty_ordered", "item_price", "item_qty_order", "item_unit_total"]

# Keep rows where all values in the specified columns are non-zero
non_zero_df = df[df[cols].ne(0).all(axis=1)]

numeric_stats = non_zero_df[cols].describe().T

logger.info("Numeric summary statistics (excluding rows with any zero):")
numeric_stats

# %%
# Count of unique orders, customers, and products
n_orders    = df["order_id"].nunique()
n_customers = df["customer_id"].nunique()
n_products  = df["product_id"].nunique()

logger.info(f"Unique orders: {n_orders}")
logger.info(f"Unique customers: {n_customers}")
logger.info(f"Unique products: {n_products}")

# %%
# Distribution of customer_gender
gender_counts = df["customer_gender"].value_counts()
logger.info(f"Customer gender distribution:\n{gender_counts}")

# %%
# Distribution of top 100 item_unit_total
n = 100
plt.figure()
sns.histplot(df["item_unit_total"].sort_values(ascending=False).head(n), bins=50, kde=True)
plt.title(f"Distribution of Top {n} Item Unit Total")
plt.xlabel("Item Unit Total ($)")
plt.ylabel("Frequency")
plt.tight_layout()
plt.show()

# %% [markdown]
# ## 8. Feature Engineering
#
# 1. **Date features:** order_date, order_month, day_of_week, hour_of_day  
# 2. **RFM features:** Recency, Frequency, Monetary for customer segmentation  
# 3. **High-value flags** and other boolean features

# %%
# 8.1 Date Features
df["order_date"]        = df["order_created_at"].dt.date
df["order_month"]       = df["order_created_at"].dt.to_period("M").astype(str)
df["order_day_of_week"] = df["order_created_at"].dt.day_name()
df["order_hour"]        = df["order_created_at"].dt.hour

# %%
# 8.2 RFM (Customer-Level) — Recency, Frequency, Monetary
# Recency: days since last order (as of max date in dataset)
max_date = df["order_created_at"].max().normalize()
logger.info(f"Max order date in data: {max_date}")

# Compute last_order_date, total_orders, total_spent per customer
rfm = (
    df.groupby(["customer_id", "customer_name"])
      .agg(
          last_order_date=("order_created_at", "max"),
          frequency=("order_id", "nunique"),
          monetary=("item_unit_total", "sum")
      )
      .reset_index()
)
rfm["recency_days"] = (max_date - rfm["last_order_date"].dt.normalize()).dt.days

# Display top 5 RFM entries
rfm.head()

# %%
# Top 10 customers by number of orders (preliminary)
n = 10
logger.info(f"Top {n} Customers by Number of Orders (Frequency):")
rfm[["customer_id","customer_name", "monetary", "frequency"]].sort_values("frequency", ascending=False).head(n)

# %%
# Top 10 customers by total purchase (preliminary)
n = 10
logger.info(f"Top {n} Customers by Total Purchase (Monetary):")
rfm[["customer_id","customer_name","frequency", "monetary"]].sort_values("monetary", ascending=False).head(n)

# %%
# 2. Define the number of quantiles (e.g., 5 for quintiles)
n_quantiles = 5

# 3. Compute R, F, M scores using pd.qcut across the entire series

# Recency: lower recency_days -> better score, so we compute qcut then invert
rfm["R_score"] = pd.qcut(
    rfm["recency_days"],
    q=n_quantiles,
    labels=False,
    duplicates="drop"
).astype(int)

# Invert recency scores: 0 (worst recency) becomes (n_quantiles) (best), etc.
rfm["R_score"] = n_quantiles - rfm["R_score"]

# Frequency: higher frequency -> higher score
rfm["F_score"] = pd.qcut(
    rfm["frequency"],
    q=n_quantiles,
    labels=False,
    duplicates="drop"
).astype(int) + 1

# Monetary: higher monetary -> higher score
rfm["M_score"] = pd.qcut(
    rfm["monetary"],
    q=n_quantiles,
    labels=False,
    duplicates="drop"
).astype(int) + 1

# 4. Combine into a single RFM score (e.g., R*100 + F*10 + M)
rfm["RFM_score"] = (
    rfm["R_score"] * 100 +
    rfm["F_score"] * 10 +
    rfm["M_score"]
)

rfm_edges = [0, 100, 200, 400, 500, max(rfm["RFM_score"])]
rfm_labels = ["At Risk","Needs Attention","Potential Loyalist","Loyal Customers","Champions"]
# 2. Use pd.cut with those edges (note: need len(labels) == len(edges)-1)
rfm["segment"] = pd.cut(
    rfm["RFM_score"],
    bins=rfm_edges,
    labels=rfm_labels,
    include_lowest=True
)

segment_counts = rfm["segment"].value_counts().reset_index()
segment_counts["segment"] = pd.Categorical(segment_counts["segment"], 
                                categories=rfm_labels[::-1], ordered=True)
segment_counts = segment_counts.sort_values("segment")

# 3. Optionally inspect edges
logger.info(f"Bins used: {rfm_edges}")
logger.info(segment_counts)

# 6. View the resulting RFM table
rfm[[
    "customer_id", "customer_name",
    "recency_days", "frequency", "monetary",
    "R_score", "F_score", "M_score", "RFM_score", "segment"
]].head()


# %%
# 8.4 High-Value Item Flag (order-level)
df["high_value_item"] = df["item_unit_total"] > df["item_unit_total"].quantile(0.80)

# 8.5 Email Domain Extraction (customer-level)
df["email_domain"] = df["customer_email"].str.split("@").str[-1].str.lower()

# 8.6 Label Encoding (if needed for modeling)
le_gender = LabelEncoder()
df["gender_encoded"] = le_gender.fit_transform(df["customer_gender"].fillna("Unknown"))

# %% [markdown]
# ## 9. Advanced Analysis & Segmentation
#
# ### 9.1 RFM Segmentation Groups
# We can bucket RFM scores into named segments (e.g., “Champions”, “Loyal Customers”, etc.).  
# For brevity, we’ll define a simple mapping based on RFM_score ranges.


# %%
# ### 9.2 Customer Lifetime Value (CLTV) Approximation
# A simple proxy: monetary value per customer * average order frequency
rfm["avg_order_value"] = rfm["monetary"] / rfm["frequency"]
rfm["cltv_score"] = rfm["avg_order_value"] * rfm["frequency"]

# Top 5 customers by CLTV
n = 5
logger.info(f"Top {n} Customers by Customer Lifetime Value (CLTV) score:")
rfm.sort_values("cltv_score", ascending=False).head(n)

# %% [markdown]
# ## 10. Sales Trend Analysis
#
# Aggregate sales metrics at different time granularities.

# %%
# 10.1 Monthly Sales (time series)
sales_monthly = (
    df.groupby("order_month")
      .agg(
          total_sales=("item_unit_total", "sum"),
          total_orders=("order_id", "nunique"),
          total_qty=("item_qty_order", "sum")
      )
      .reset_index()
)
# Convert order_month back to Timestamp for plotting
sales_monthly["order_month_ts"] = pd.to_datetime(sales_monthly["order_month"] + "-01")
sales_monthly.head()

# %%
# 10.2 Weekly Sales
df["order_week"] = df["order_created_at"].dt.to_period("W").astype(str)
sales_weekly = (
    df.groupby("order_week")
      .agg(
          total_sales=("item_unit_total", "sum"),
          total_orders=("order_id", "nunique"),
          total_qty=("item_qty_order", "sum")
      )
      .reset_index()
)
sales_weekly["order_week_start"] = pd.to_datetime(sales_weekly["order_week"].str.split("/").str[0])
sales_weekly.sort_values("order_week_start", inplace=True)
sales_weekly.head()

# %% [markdown]
# ## 11. Product-Level Analysis
#
# Determine top products by quantity sold and revenue, and examine price distributions.

# %%
top_products = (
    df.groupby(["product_id", "product_name"])
        .agg(total_qty_sold=("item_qty_order", "sum"), 
             total_revenue=("item_unit_total", "sum"))
        .reset_index()
)

# %%
# 11.1 Top 10 Products by Quantity Sold
n = 10
top_products_qty = (top_products
      .sort_values("total_qty_sold", ascending=False))
logger.info(f"Top {n} Products by Quantity Sold:")
top_products_qty.head(n)

# %%
# 11.2 Top 10 Products by Revenue
n = 10
top_products_rev = (top_products
      .sort_values("total_revenue", ascending=False))
logger.info(f"Top {n} Products by Revenue:")
top_products_rev.head(n)

# %%
# 11.3 Price Distribution Across Top n Items
n = 100
plt.figure()
sns.boxplot(x=df["item_price"].sort_values(ascending=False).head(n), showmeans=True)
plt.title(f"Item Price Distribution across Top {n} Items")
plt.xlabel("Item Price ($)")
plt.xticks(rotation=45)
plt.gca().xaxis.set_major_formatter(mtick.StrMethodFormatter('${x:,.2f}'))
plt.tight_layout()
plt.show()

# %% [markdown]
# ## 12. Customer Demographics & Behavior
#
# Explore gender distribution, email domain popularity, and average order totals by gender.

# %%
# 12.1 Gender Distribution Pie Chart
gender_counts = gender_counts.reset_index()
gender_counts.columns = ["gender", "count"]

plt.figure()
plt.pie(
    gender_counts["count"],
    labels=gender_counts["gender"],
    autopct="%1.1f%%",
    startangle=140
)
plt.title("Customer Gender Distribution")
plt.tight_layout()
plt.show()

# %%
# 12.2 Top 10 Email Domains
n = 10
top_domains = (
    df["email_domain"]
      .value_counts()
      .reset_index()
)
print(top_domains.head(n))

# Bar plot of top email domains
plt.figure()
sns.barplot(x="count", y="email_domain", data=top_domains.head(n), 
            hue="email_domain", legend=False, palette="cubehelix")
plt.title(f"Top {n} Customer Email Domains")
plt.xlabel("Number of Customers")
plt.ylabel("Email Domain")
plt.tight_layout()
plt.show()

# %%
# 12.3 Average Order Total by Gender
avg_order_by_gender = (
    df.groupby("customer_gender")
      .agg(avg_order_total=("order_total", "mean"), count_orders=("order_id", "nunique"))
      .reset_index()
)
print(avg_order_by_gender)

plt.figure()
ax = sns.barplot(x="customer_gender", y="avg_order_total", data=avg_order_by_gender, 
            hue="customer_gender", legend=False, palette="pastel")
plt.title("Average Order Total by Gender")
plt.ylabel("Average Order Total ($)")
plt.xlabel("Gender")
ax.yaxis.set_major_formatter(mtick.StrMethodFormatter('${x:,.2f}'))
ax.tick_params(axis='y', rotation=45)
plt.tight_layout()
plt.show()

# %% [markdown]
# ## 13. Save Intermediate & Final Outputs
#
# 1. Save cleaned raw data (if needed) to Parquet/CSV  
# 2. Save RFM and segmentation data  
# 3. Save time-series summaries  
# 4. Write summary tables back to Postgres for dashboarding or further reporting  

# %%
# 13.1 Save cleaned DataFrame to Parquet
cleaned_path = os.path.join(OUTPUT_DIR, "sales_order_item_flat_cleaned.parquet")
df.to_parquet(cleaned_path, index=False)
logger.info(f"Cleaned data saved to Parquet: {cleaned_path}")

# %%
# 13.2 Save RFM table to CSV
rfm_path = os.path.join(OUTPUT_DIR, "customer_rfm.csv")
rfm.to_csv(rfm_path, index=False)
logger.info(f"RFM data saved to CSV: {rfm_path}")

# %%
# 13.3 Save sales_monthly and sales_weekly to CSV
sales_monthly_path = os.path.join(OUTPUT_DIR, "sales_monthly.csv")
sales_monthly.to_csv(sales_monthly_path, index=False)
logger.info(f"Monthly sales summary saved to CSV: {sales_monthly_path}")

sales_weekly_path = os.path.join(OUTPUT_DIR, "sales_weekly.csv")
sales_weekly.to_csv(sales_weekly_path, index=False)
logger.info(f"Weekly sales summary saved to CSV: {sales_weekly_path}")

# %%
# 13.4 Write sales_monthly summary back to Postgres (overwrite if exists)
from sqlalchemy import Table, Column, String, Integer, Float, MetaData
import math

sales_monthly_db = sales_monthly[["order_month", "total_sales", "total_orders", "total_qty"]].copy()
sales_monthly_db.rename(columns={"order_month": "month"}, inplace=True)

with engine.begin() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))
metadata = MetaData(schema=SCHEMA)
monthly_sales_tbl = Table(
    SUMMARY_TABLE, metadata,
    Column("month", String, primary_key=True),
    Column("total_sales", Float),
    Column("total_orders",   Integer),
    Column("total_qty",      Integer),
)
metadata.drop_all(engine, tables=[monthly_sales_tbl], checkfirst=True)
metadata.create_all(engine, tables=[monthly_sales_tbl])

def stream_insert(df, table, engine, batch_size=5000):
    """
    Manually insert rows in batches of `batch_size` using SQLAlchemy Core.
    """
    total_rows = len(df)
    n_batches  = math.ceil(total_rows / batch_size)
    
    with engine.begin() as conn:
        for i in range(n_batches):
            start = i * batch_size
            end   = min(start + batch_size, total_rows)
            # Slice a chunk of the DataFrame
            chunk_df = df.iloc[start:end]
            # Convert that slice to list of dicts (only this chunk in memory)
            chunk_records = chunk_df.to_dict(orient="records")
            
            conn.execute(table.insert(), chunk_records)
            
            print(f"Inserted batch {i+1}/{n_batches} ({end-start} rows into {table.schema}.{table.name})")

# Usage:
stream_insert(sales_monthly_db, monthly_sales_tbl, engine, batch_size=5000)


# %% [markdown]
# ## 14. Comprehensive Visual Dashboard (Optional)
#
# If you want to present multiple plots in a single canvas, you can create a grid. Below is an example of combining three sets of key charts:
# 1. Monthly/Weekly sales/orders/items trend
# 2. Top 10 products by revenue  
# 3. RFM segment distribution  


# %%
# 14.3 Plot grid
fig, axes = plt.subplots(3, 1, figsize=(24, 24))

# A. Monthly Sales Trend
sns.lineplot(
    x="order_month_ts",
    y="total_sales",
    data=sales_monthly,
    marker="o",
    ax=axes[0],
    label="Monthly",
    color="steelblue"
)

# A. Weekly Sales Trend
sns.lineplot(
    x="order_week_start",
    y="total_sales",
    data=sales_weekly,
    marker="o",
    ax=axes[0],
    label="Weekly",
    color="orange"
)

axes[0].set_title("Sales Trend")
axes[0].set_xlabel("Timeline")
axes[0].set_ylabel("Total Sales ($)")
axes[0].tick_params(axis="both", rotation=45)
axes[0].yaxis.set_major_formatter(mtick.StrMethodFormatter('{x:,.2f}'))
axes[0].legend()

# A. Monthly Orders Trend
sns.lineplot(
    x="order_month_ts",
    y="total_orders",
    data=sales_monthly,
    marker="o",
    ax=axes[1],
    label="Monthly",
    color="steelblue"
)

# D. Weekly Orders Trend
sns.lineplot(
    x="order_week_start",
    y="total_orders",
    data=sales_weekly,
    marker="o",
    ax=axes[1],
    label="Weekly",
    color="orange"
)

axes[1].set_title("Orders Trend")
axes[1].set_xlabel("Timeline")
axes[1].set_ylabel("Total Orders")
axes[1].tick_params(axis="x", rotation=45)

# A. Monthly Items Trend
sns.lineplot(
    x="order_month_ts",
    y="total_qty",
    data=sales_monthly,
    marker="o",
    ax=axes[2],
    label="Monthly",
    color="steelblue"
)

# D. Weekly Items Trend
sns.lineplot(
    x="order_week_start",
    y="total_qty",
    data=sales_weekly,
    marker="o",
    ax=axes[2],
    label="Weekly",
    color="orange"
)

axes[2].set_title("Items Trend")
axes[2].set_xlabel("Timeline")
axes[2].set_ylabel("Total Items")
axes[2].tick_params(axis="x", rotation=45)

for ax in axes:
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%B %Y'))
    ax.grid(which="major", axis="x", linestyle="--", alpha=0.3)

plt.tight_layout()
plt.show()

# %%
n = 10
# 14.1a Top 10 Products by Revenue (for plotting)
topn_products_rev = top_products_rev.head(n).copy()
topn_products_rev["product_label"] = (
    topn_products_rev["product_name"].apply(lambda x: fill(x, width=25)) + 
    "\n(ID: " + topn_products_rev["product_id"].astype(str) + ")"
)

# 14.1b Top 10 Products by Qty sold (for plotting)
topn_products_qty = top_products_qty.head(n).copy()
topn_products_qty["product_label"] = (
    topn_products_qty["product_name"].apply(lambda x: fill(x, width=25)) + 
    "\n(ID: " + topn_products_qty["product_id"].astype(str) + ")"
)

fig, axes = plt.subplots(1, 2, figsize=(24, 8))
# B. Top 10 Products by Revenue
sns.barplot(
    x="total_revenue",
    y="product_label",
    data=topn_products_rev.sort_values("total_revenue", ascending=True),
    ax=axes[0],
    hue="product_label",
    legend=False,
    palette="crest"
)
axes[0].set_title(f"Top {n} Products by Revenue")
axes[0].set_xlabel("Revenue ($)")
axes[0].set_ylabel("Product Name")
axes[0].xaxis.set_major_formatter(mtick.StrMethodFormatter('${x:,.2f}'))
axes[0].tick_params(axis='x', rotation=45)

# B. Top 10 Products by Quantity sold
sns.barplot(
    x="total_qty_sold",
    y="product_label",
    data=topn_products_qty.sort_values("total_qty_sold", ascending=True),
    ax=axes[1],
    hue="product_label",
    legend=False,
    palette="crest"
)
axes[1].set_title(f"Top {n} Products by Quantity")
axes[1].set_xlabel("Quantity")
axes[1].set_ylabel("Product Name")

plt.tight_layout()
plt.show()

# %%
# 14.2 RFM segment counts
# C. RFM Segment Distribution
sns.barplot(
    x="count",
    y="segment",
    data=segment_counts,
    hue="segment",
    legend=False,
    palette="Spectral",
    order=rfm_labels[::-1]
)
plt.title("Customer RFM Segment Distribution")
plt.xlabel("Number of Customers")
plt.ylabel("Segment")
plt.yticks(rotation=45)
plt.show()

# %%
# ## Visualizations
# Total Sales by Day of Week
sales_by_day = df.groupby("order_day_of_week").agg(total_sales=("item_unit_total", "sum"), 
    total_orders=("order_id", "nunique"), total_items=("item_qty_order", "sum")).reset_index()
order_days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
sales_by_day["order_day_of_week"] = pd.Categorical(sales_by_day["order_day_of_week"], categories=order_days, ordered=True)
sales_by_day.sort_values("order_day_of_week", inplace=True)

# ## Create 3-row, 1-column subplots
fig, axes = plt.subplots(3, 1, figsize=(12, 18))

# 1) Total Sales by Day of Week
sns.barplot(
    x="order_day_of_week",
    y="total_sales",
    data=sales_by_day,
    hue="order_day_of_week",
    legend=False,
    palette="Set2",
    ax=axes[0]
)
axes[0].set_title("Total Sales by Day of Week")
axes[0].set_xlabel("Day of Week")
axes[0].set_ylabel("Total Sales ($)")
axes[0].yaxis.set_major_formatter(mtick.StrMethodFormatter('${x:,.2f}'))
axes[0].tick_params(axis='both', rotation=45)

# 2) Total Orders by Day of Week
sns.barplot(
    x="order_day_of_week",
    y="total_orders",
    data=sales_by_day,
    hue="order_day_of_week",
    legend=False,
    palette="Set2",
    ax=axes[1]
)
axes[1].set_title("Total Orders by Day of Week")
axes[1].set_xlabel("Day of Week")
axes[1].set_ylabel("Total Orders")
axes[1].yaxis.set_major_formatter(mtick.StrMethodFormatter('{x:,.0f}'))
axes[1].tick_params(axis='x', rotation=45)

# 3) Total Items by Day of Week
sns.barplot(
    x="order_day_of_week",
    y="total_items",
    data=sales_by_day,
    hue="order_day_of_week",
    legend=False,
    palette="Set2",
    ax=axes[2]
)
axes[2].set_title("Total Items by Day of Week")
axes[2].set_xlabel("Day of Week")
axes[2].set_ylabel("Total Items")
axes[2].yaxis.set_major_formatter(mtick.StrMethodFormatter('{x:,.0f}'))
axes[2].tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.show()

# %% [markdown]
# ## 15. Additional Statistical & Exploratory Analyses
#
# In this section, we’ll:
# 1. Create a Seaborn pairplot of key numeric columns and compute a correlation matrix  
# 2. Use `resample()` + `rolling()` to analyze trends in daily and weekly sales  
# 3. Compute Pearson correlation (and p-value) between `item_price` and `item_unit_total` via `scipy.stats`  
# 4. Fit a simple linear regression of `item_unit_total` on `item_price` using `statsmodels`  
# 5. Fit a basic ARIMA time series model on the weekly total sales via `statsmodels.tsa`
#
# These steps build on the DataFrames we already prepared (`df`, `sales_weekly`, etc.).

# %%
# Make sure plots appear inline
# (In JupyterLab this is usually automatic; if running as a script, ensure plt.show() is called.)

# ─── 15.1 Pairplot & Correlation Matrix ────────────────────────────────────────

# Select a subset of numeric columns for pairwise exploration
numeric_cols = [
    "order_total",
    "total_qty_ordered",
    "item_price",
    "item_qty_order",
    "item_unit_total"
]

# 15.1.1 Pairplot
sns.pairplot(df[numeric_cols], diag_kind="kde", corner=True, plot_kws={"marker": "."})
plt.suptitle("Pairplot of Key Numeric Columns", y=1.02)
plt.tight_layout()
plt.show()

# 15.1.2 Correlation Matrix
corr_matrix = df[numeric_cols].corr()
plt.figure(figsize=(8, 6))
sns.heatmap(
    corr_matrix,
    annot=True,
    fmt=".2f",
    cmap="coolwarm",
    cbar_kws={"shrink": 0.8}
)
plt.title("Correlation Matrix of Numeric Features")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# %%
# ─── 15.2 Resample & Rolling Analysis ─────────────────────────────────────────

# 15.2.1 Convert df to a daily time series of total sales
df.set_index("order_created_at", inplace=True)  # temporarily set index to datetime
daily_sales = df["item_unit_total"].resample("D").sum().rename("daily_sales")

# 15.2.2 7-day rolling average of daily sales
daily_sales_df = daily_sales.to_frame()
daily_sales_df["7d_roll_avg"] = daily_sales_df["daily_sales"].rolling(window=7, min_periods=1).mean()

# Plot daily sales + 7-day rolling average
fig, axes = plt.subplots(2, 1, figsize=(24, 16))
axes[0].plot(daily_sales_df.index, daily_sales_df["daily_sales"], label="Daily Sales", alpha=0.6)
axes[0].plot(daily_sales_df.index, daily_sales_df["7d_roll_avg"], label="7-day Rolling Avg", linestyle="--", linewidth=2)
axes[0].set_title("Daily Sales & 7-day Rolling Average")
axes[0].set_xlabel("Date")
axes[0].set_ylabel("Sales ($)")
axes[0].legend()

# Restore df index to keep later code consistent
df.reset_index(inplace=True)

# 15.2.3 Weekly sales (already computed as sales_weekly), plot with rolling mean of 4 weeks
sales_weekly.sort_values("order_week_start", inplace=True)
sales_weekly_ts = sales_weekly.set_index("order_week_start")["total_sales"]
sales_weekly_df = sales_weekly_ts.to_frame()
sales_weekly_df["4w_roll_avg"] = sales_weekly_df["total_sales"].rolling(window=4, min_periods=1).mean()

axes[1].plot(sales_weekly_df.index, sales_weekly_df["total_sales"], label="Weekly Sales", alpha=0.6)
axes[1].plot(sales_weekly_df.index, sales_weekly_df["4w_roll_avg"], linestyle="--", label="4-week Rolling Avg")
axes[1].set_title("Weekly Sales & 4-week Rolling Average")
axes[1].set_xlabel("Week Starting")
axes[1].set_ylabel("Sales ($)")
axes[1].legend()

for ax in axes:
    ax.tick_params(axis="both", rotation=45)
    ax.yaxis.set_major_formatter(mtick.StrMethodFormatter('${x:,.2f}'))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%B %Y'))
    ax.grid(which="major", axis="x", linestyle="--", alpha=0.3)

plt.tight_layout()
plt.show()

# %% [markdown]
# ## 17. Rolling Average of Order Size Over Different Windows
#
# Compute daily average order size (number of items per order) and then plot the
# 1-day, 7-day, and 30-day rolling averages on a single timeline.  
# The y-axis ticks will be integers (0,1,2,3,…) at every increment, and the x-axis
# will use monthly labels in the format “January 2021”, “February 2021”, etc.

# %%
# ─── 17.1 Compute one row per order with order_date and order_size ───────────

# Aggregate `df` to get one row per order_id, with order_date and total item count
order_items = (
    df.groupby("order_id")
      .agg(
          order_date=("order_created_at", "min"),      # use earliest timestamp as order date
          order_size=("item_qty_order", "sum"),        # total items in order
          customer_gender=("customer_gender", "first"),
          order_day_of_week=("order_day_of_week", "first")
      )
      .reset_index()
)

# Convert to DataFrame indexed by order_date (drop time component)
order_items["order_date"] = order_items["order_date"].dt.normalize()
order_items.set_index("order_date", inplace=True)

# ─── 17.2 Resample to get daily average order size ────────────────────────────

# For each calendar day, compute "average order_size" among all orders placed that day
daily_avg = order_items["order_size"].resample("D").mean().fillna(0)

# ─── 17.3 Compute rolling averages for 1-day, 7-day, and 30-day windows ───────

rolling_7d  = daily_avg.rolling(window=7,  min_periods=1).mean()
rolling_30d = daily_avg.rolling(window=30, min_periods=1).mean()

# ─── 17.4 Plot all three rolling series on one figure ────────────────────────

plt.figure(figsize=(24, 8))
ax = plt.gca()

# Plot the raw daily average
sns.lineplot(
    x=daily_avg.index,
    y=daily_avg.values,
    ax=ax,
    label="Daily Avg",
    color="gray",
    linewidth=1,
    alpha=0.5
)

# Plot 7-day rolling
sns.lineplot(
    x=rolling_7d.index,
    y=rolling_7d.values,
    ax=ax,
    label="7-Day Rolling Avg",
    color="teal",
    linewidth=2
)

# Plot 30-day rolling
sns.lineplot(
    x=rolling_30d.index,
    y=rolling_30d.values,
    ax=ax,
    label="30-Day Rolling Avg",
    color="darkorange",
    linewidth=2
)

# ─── 17.5 Format y-axis to show every integer tick ────────────────────────────

ax.yaxis.set_major_locator(mtick.MultipleLocator(1))
ax.set_ylim(bottom=0)

# ─── 17.6 Format x-axis to show monthly ticks with “Month Year” labels ────────

ax.xaxis.set_major_locator(mdates.MonthLocator())
ax.xaxis.set_major_formatter(mdates.DateFormatter("%B %Y"))
ax.grid(which="major", axis="both", linestyle="--", alpha=0.3)
plt.xticks(rotation=45)

# ─── 17.7 Labels, Title, Legend, Layout ─────────────────────────────────────

ax.set_title("Rolling Average Order Size (1-Day, 7-Day, 30-Day Windows)", fontsize=14)
ax.set_xlabel("Date", fontsize=12)
ax.set_ylabel("Average Order Size (items)", fontsize=12)
ax.legend(title="")
plt.tight_layout()
plt.show()

# %% [markdown]
# ### 18. Average Order Size by Customer Gender and Day of Week
#
# Compute average order size grouped by `customer_gender` and by `order_day_of_week`.

# %%
# ─── 18.1 Average Order Size by Customer Gender ─────────────────────────────
avg_by_gender = (
    order_items
    .groupby("customer_gender")["order_size"]
    .mean()
    .reset_index()
)

plt.figure(figsize=(6, 4))
ax = sns.barplot(
    x="customer_gender",
    y="order_size",
    data=avg_by_gender,
    hue="customer_gender",
    legend=False,
    palette="pastel",
    edgecolor=".2"
)
plt.title("Average Order Size by Gender")
plt.xlabel("Customer Gender")
plt.ylabel("Average Order Size (items)")
ax.yaxis.set_major_locator(mtick.MultipleLocator(1))
ax.set_ylim(bottom=0)
plt.tight_layout()
plt.show()

# ─── 18.2 Average Order Size by Day of Week ─────────────────────────────────
# Define order of days
days_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

avg_by_dow = (
    order_items
    .groupby("order_day_of_week")["order_size"]
    .mean()
    .reindex(days_order)
    .reset_index()
)

plt.figure(figsize=(8, 4))
ax = sns.barplot(
    x="order_day_of_week",
    y="order_size",
    data=avg_by_dow,
    order=days_order,
    hue="order_day_of_week",
    legend=False,
    palette="muted",
    edgecolor=".2"
)
plt.title("Average Order Size by Day of Week")
plt.xlabel("Day of Week")
plt.ylabel("Average Order Size (items)")
ax.yaxis.set_major_locator(mtick.MultipleLocator(1))
ax.set_ylim(bottom=0)
plt.tight_layout()
plt.show()

# %%
# ─── 15.3 Pearson Correlation (scipy.stats) ─────────────────────────────────

# Compute Pearson r & p-value between item_price and item_unit_total
item_price = df["item_price"]
item_total = df["item_unit_total"]
pearson_r, pearson_p = stats.pearsonr(item_price, item_total)

print(f"Pearson correlation between item_price and item_unit_total: r = {pearson_r:.4f}, p-value = {pearson_p:.2e}")

# %%
# ─── 15.4 Linear Regression (statsmodels) ────────────────────────────────────

# Prepare predictors (add constant) and response
X = sm.add_constant(df["item_price"])  # intercept + item_price
y = df["item_unit_total"]

# Fit Ordinary Least Squares (OLS)
ols_model = sm.OLS(y, X).fit()

# Print summary
print(ols_model.summary())

# Optionally, plot scatter + regression line
plt.figure(figsize=(8, 5))
sns.scatterplot(x="item_price", y="item_unit_total", data=df.sample(1000), alpha=0.3, label="sampled original data")
# Regression line
price_vals = np.linspace(df["item_price"].min(), df["item_price"].max(), 100)
preds = ols_model.params["const"] + ols_model.params["item_price"] * price_vals
plt.plot(price_vals, preds, color="red", linewidth=2, label="Linear Regression Fit")
plt.title("Item Unit Total vs. Item Price with Regression Line")
plt.xlabel("Item Price ($)")
plt.ylabel("Item Unit Total ($)")
plt.legend()
plt.tick_params(axis="both", rotation=45)
plt.gca().xaxis.set_major_formatter(mtick.StrMethodFormatter('${x:,.2f}'))
plt.gca().yaxis.set_major_formatter(mtick.StrMethodFormatter('${x:,.2f}'))
plt.tight_layout()
plt.show()

# %%
# ─── 15.5 Time Series Forecasting (ARIMA on Weekly Sales) ─────────────────

# Ensure sales_weekly_df has a proper datetime index (already set above)
# Fit a simple ARIMA(1,0,1) model (as an example)
arima_order = (1, 0, 1)
arima_model = ARIMA(sales_weekly_df["total_sales"], order=arima_order).fit()
print(arima_model.summary())

# Forecast next 8 weeks
forecast_steps = 8
forecast_index = pd.date_range(
    start=sales_weekly_df.index.max() + pd.Timedelta(weeks=1),
    periods=forecast_steps,
    freq="W-MON"
)
forecast = arima_model.forecast(steps=forecast_steps)

# Plot actual vs forecast
plt.figure(figsize=(24, 8))
plt.plot(sales_weekly_df.index, sales_weekly_df["total_sales"], marker=".", label="Actual Weekly Sales")
plt.plot(forecast_index, forecast, marker=".", linestyle="--", label="ARIMA Forecast")
plt.title(f"Weekly Sales ARIMA({arima_order[0]},{arima_order[1]},{arima_order[2]}) Forecast")
plt.xlabel("Week Starting")
plt.ylabel("Sales ($)")
plt.legend()
plt.tick_params(axis="both", rotation=45)
plt.gca().xaxis.set_major_locator(mdates.MonthLocator())
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%B %Y'))
plt.gca().yaxis.set_major_formatter(mtick.StrMethodFormatter('${x:,.2f}'))
plt.grid(which="major", axis="x", linestyle="--", alpha=0.3)
plt.tight_layout()
plt.show()

# %%
from ydata_profiling import ProfileReport

# Generate the report
profile = ProfileReport(df, title="Pandas Profiling Report", explorative=True)

# Display in Jupyter Notebook
profile.to_notebook_iframe()

profile.to_file("ydata_profiling_report.html")


# %% [markdown]
# ## 15. Next Steps & Best Practices
#
# 1. **Automate** this notebook using **Papermill** to parameterize dates and run on a schedule (e.g., daily/weekly).  
# 2. **Integrate Great Expectations** (or similar) for a formal data validation suite, producing data quality reports.  
# 3. **Build an interactive dashboard** using Plotly Dash or Streamlit, sourcing from the Postgres summary tables.  
# 4. **Add unit tests** (e.g., using `pytest`, `nbval`) to ensure each function behaves as expected.  
# 5. **Version control** the notebook (or convert to a `.py` with Jupytext) to enable diffable changes.  
# 6. **Monitor performance**: If the data grows, consider reading only deltas (e.g., `order_created_at >= last_run_date`) rather than the full table each time.  
#
# **End of Analysis Pipeline Notebook**  
