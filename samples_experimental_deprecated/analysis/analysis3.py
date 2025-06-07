# ---
# # Production-Grade Data Analysis of `dm.sales_order_item_flat` Table
# ---
# ## 1️⃣ Setup & Imports

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from sqlalchemy import create_engine, text

# Configure plots
sns.set(style="whitegrid")
plt.rcParams["figure.figsize"] = (12, 6)

# ---
# ## 2️⃣ Connect to PostgreSQL Database

# SQLAlchemy database URL
db_url = "postgresql+psycopg2://postgres:pg123@db-postgres:5432/data_warehouse"

# Create SQLAlchemy engine
engine = create_engine(db_url)

# ---
# ## 3️⃣ Load Data into Pandas

# Use SQL to read the entire table
query = """
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
FROM dm.sales_order_item_flat;
"""

df = pd.read_sql_query(text(query), engine)

# Display first few rows
df.head()

# ---
# ## 4️⃣ Basic EDA: Data Overview

# Shape and columns
print(f"Shape: {df.shape}")
print(f"Columns: {df.columns.tolist()}")

# Data types and null counts
print(df.info())
print(df.isnull().sum())

# Basic statistics
df.describe(include="all").T

# ---
# ## 5️⃣ Data Cleaning & Feature Engineering

# Convert order_created_at to datetime (should be already, but ensure it)
df["order_created_at"] = pd.to_datetime(df["order_created_at"])

# Create additional features
df["order_date"] = df["order_created_at"].dt.date
df["order_month"] = df["order_created_at"].dt.to_period("M")
df["order_day_of_week"] = df["order_created_at"].dt.day_name()

# Example: flag high-value items
df["high_value_item"] = df["item_unit_total"] > 100

# Validate customer_gender values
print(df["customer_gender"].value_counts())

# ---
# ## 6️⃣ Analysis: Key Metrics

# 🟢 Top 10 customers by total purchase
top_customers = (
    df.groupby(["customer_id", "customer_name"])
    .agg(total_spent=("item_unit_total", "sum"), total_orders=("order_id", "nunique"))
    .sort_values("total_spent", ascending=False)
    .head(10)
    .reset_index()
)
print(top_customers)

# 🟢 Sales over time
sales_by_month = df.groupby("order_month").agg(total_sales=("item_unit_total", "sum")).reset_index()
print(sales_by_month)

# 🟢 Most popular products
popular_products = (
    df.groupby(["product_id", "product_name"])
    .agg(total_qty_sold=("item_qty_order", "sum"))
    .sort_values("total_qty_sold", ascending=False)
    .head(10)
    .reset_index()
)
print(popular_products)

# ---
# ## 7️⃣ Visualizations

# 🟢 Monthly Sales Trend
plt.figure(figsize=(12, 6))
sns.lineplot(x="order_month", y="total_sales", data=sales_by_month, marker="o", color="dodgerblue")
plt.title("Monthly Sales Trend")
plt.xlabel("Month")
plt.ylabel("Total Sales ($)")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# 🟢 Top 10 Products by Quantity Sold
plt.figure(figsize=(12, 6))
sns.barplot(
    x="total_qty_sold",
    y="product_name",
    data=popular_products.sort_values("total_qty_sold", ascending=True),
    palette="viridis"
)
plt.title("Top 10 Products by Quantity Sold")
plt.xlabel("Total Quantity Sold")
plt.ylabel("Product Name")
plt.tight_layout()
plt.show()

# 🟢 Sales by Day of Week
sales_by_day = df.groupby("order_day_of_week").agg(total_sales=("item_unit_total", "sum")).reset_index()
order_days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
sales_by_day["order_day_of_week"] = pd.Categorical(sales_by_day["order_day_of_week"], categories=order_days, ordered=True)
sales_by_day = sales_by_day.sort_values("order_day_of_week")

plt.figure(figsize=(10, 5))
sns.barplot(x="order_day_of_week", y="total_sales", data=sales_by_day, palette="Set2")
plt.title("Total Sales by Day of Week")
plt.ylabel("Total Sales ($)")
plt.xlabel("Day of Week")
plt.tight_layout()
plt.show()

# ---
# ## 8️⃣ Save Results

# Save to CSV for further reporting
top_customers.to_csv("top_customers.csv", index=False)
popular_products.to_csv("top_products.csv", index=False)
sales_by_month.to_csv("sales_by_month.csv", index=False)

# Example: Write a summary table back to Postgres (optional)
# Create a DataFrame with summaries
summary_df = sales_by_month.copy()
summary_df.columns = ["month", "total_sales"]

# Save to a new table in Postgres (replace if exists)
summary_df.to_sql("monthly_sales_summary", engine, schema="dm", if_exists="replace", index=False)

# ---
# ## 9️⃣ Next Steps
# - Explore advanced analyses (e.g., customer segmentation, cohort analysis)
# - Build interactive dashboards (Plotly, Dash, or Streamlit)
# - Automate insights into Airflow / DAGs for scheduled runs
# - Document findings in Confluence or internal wikis for knowledge sharing

# ---
# 🎉 Done! This notebook provides a **production-grade analysis pipeline** for your sales data.
