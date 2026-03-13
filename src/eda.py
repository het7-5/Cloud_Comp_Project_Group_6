"""
Exploratory Data Analysis (EDA) Module
=======================================
Performs comprehensive EDA on the E-Commerce dataset using Spark DataFrames
and Spark SQL. Generates summary statistics, sample queries, and visualizations.

Spark Components Used: Structured APIs + Spark SQL
"""

import os
import sys
import matplotlib
matplotlib.use("Agg")  # Non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.utils import get_spark_session, DATA_SAMPLE_DIR, EDA_OUTPUT_DIR, ensure_dirs
from src.ingestion import load_customers, load_products, load_transactions, load_clickstream

from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    month, year, dayofweek, hour, date_format, round as spark_round,
    when, lit, explode, desc, asc, countDistinct, datediff, current_date,
    concat, row_number, dense_rank
)
from pyspark.sql.window import Window


# ─────────────────────────────────────────────────────────────
# Plot Styling
# ─────────────────────────────────────────────────────────────
COLORS = {
    "primary": "#6366F1",
    "secondary": "#EC4899",
    "accent": "#10B981",
    "warning": "#F59E0B",
    "danger": "#EF4444",
    "dark": "#1E293B",
    "light": "#F8FAFC",
}
PALETTE = ["#6366F1", "#EC4899", "#10B981", "#F59E0B", "#EF4444",
           "#8B5CF6", "#14B8A6", "#F97316", "#3B82F6", "#84CC16"]

plt.rcParams.update({
    "figure.facecolor": "#0F172A",
    "axes.facecolor": "#1E293B",
    "axes.edgecolor": "#334155",
    "axes.labelcolor": "#E2E8F0",
    "text.color": "#E2E8F0",
    "xtick.color": "#94A3B8",
    "ytick.color": "#94A3B8",
    "grid.color": "#334155",
    "grid.alpha": 0.4,
    "font.size": 11,
    "axes.titlesize": 14,
    "axes.labelsize": 12,
    "figure.dpi": 150,
})


def save_plot(fig, filename):
    """Save figure to EDA output directory."""
    path = os.path.join(EDA_OUTPUT_DIR, filename)
    fig.savefig(path, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"   📊 Saved plot: {filename}")
    return path


# ─────────────────────────────────────────────────────────────
# 1. Dataset Overview & Summary Statistics
# ─────────────────────────────────────────────────────────────

def dataset_overview(customers_df, products_df, transactions_df, clickstream_df, spark):
    """Print comprehensive dataset summary and register SQL views."""
    print("\n" + "=" * 60)
    print("📊 DATASET OVERVIEW")
    print("=" * 60)

    # Register as SQL temp views for Spark SQL queries
    customers_df.createOrReplaceTempView("customers")
    products_df.createOrReplaceTempView("products")
    transactions_df.createOrReplaceTempView("transactions")
    clickstream_df.createOrReplaceTempView("clickstream")

    # Table row counts
    tables = {
        "Customer": customers_df,
        "Product": products_df,
        "Transaction": transactions_df,
        "Click Stream": clickstream_df,
    }
    print("\n📋 Table Sizes:")
    summary_data = []
    for name, df in tables.items():
        row_count = df.count()
        col_count = len(df.columns)
        summary_data.append((name, row_count, col_count))
        print(f"   {name:15s} → {row_count:>6,} rows × {col_count:>2} columns")

    # Null analysis
    print("\n🔍 Null Value Analysis:")
    for name, df in tables.items():
        null_counts = {}
        for c in df.columns:
            n = df.filter(col(c).isNull()).count()
            if n > 0:
                null_counts[c] = n
        if null_counts:
            print(f"   {name}: {null_counts}")
        else:
            print(f"   {name}: No nulls ✓")

    return summary_data


# ─────────────────────────────────────────────────────────────
# 2. Customer Analysis
# ─────────────────────────────────────────────────────────────

def analyze_customers(customers_df, spark):
    """Analyze customer demographics and generate plots."""
    print("\n" + "-" * 40)
    print("👥 CUSTOMER ANALYSIS")
    print("-" * 40)

    # Gender distribution
    gender_dist = customers_df.groupBy("gender").agg(count("*").alias("count")).toPandas()
    print(f"\n   Gender Distribution:\n{gender_dist.to_string(index=False)}")

    # Device type distribution
    device_dist = customers_df.groupBy("device_type").agg(count("*").alias("count")).orderBy(desc("count")).toPandas()
    print(f"\n   Device Type Distribution:\n{device_dist.to_string(index=False)}")

    # Top countries
    country_dist = customers_df.groupBy("home_country").agg(count("*").alias("count")).orderBy(desc("count")).toPandas()
    print(f"\n   Top Countries:\n{country_dist.to_string(index=False)}")

    # Join date distribution (Spark SQL)
    join_trend = spark.sql("""
        SELECT date_format(first_join_date, 'yyyy-MM') AS join_month,
               COUNT(*) AS new_customers
        FROM customers
        WHERE first_join_date IS NOT NULL
        GROUP BY join_month
        ORDER BY join_month
    """).toPandas()

    # ── Plot 1: Customer Demographics Dashboard ──
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("Customer Demographics Overview", fontsize=18, fontweight="bold", color="#E2E8F0")

    # Gender pie
    ax = axes[0, 0]
    gender_labels = gender_dist["gender"].map({"M": "Male", "F": "Female"}).values
    wedges, texts, autotexts = ax.pie(
        gender_dist["count"], labels=gender_labels,
        colors=[COLORS["primary"], COLORS["secondary"]],
        autopct="%1.1f%%", startangle=90, textprops={"color": "#E2E8F0", "fontsize": 12}
    )
    ax.set_title("Gender Distribution", fontsize=13, color="#E2E8F0")

    # Device type bar
    ax = axes[0, 1]
    bars = ax.barh(device_dist["device_type"], device_dist["count"],
                   color=PALETTE[:len(device_dist)], edgecolor="none", height=0.6)
    ax.set_title("Device Type Distribution", fontsize=13)
    ax.set_xlabel("Count")
    for bar, val in zip(bars, device_dist["count"]):
        ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height() / 2,
                f"{val}", va="center", fontsize=10, color="#CBD5E1")
    ax.invert_yaxis()

    # Top 8 countries
    ax = axes[1, 0]
    top_countries = country_dist.head(8)
    bars = ax.bar(range(len(top_countries)), top_countries["count"],
                  color=PALETTE[:len(top_countries)], edgecolor="none")
    ax.set_xticks(range(len(top_countries)))
    ax.set_xticklabels(top_countries["home_country"], rotation=45, ha="right", fontsize=9)
    ax.set_title("Customers by Country (Top 8)", fontsize=13)
    ax.set_ylabel("Count")

    # Join trend line
    ax = axes[1, 1]
    if len(join_trend) > 0:
        ax.fill_between(range(len(join_trend)), join_trend["new_customers"],
                        alpha=0.3, color=COLORS["accent"])
        ax.plot(range(len(join_trend)), join_trend["new_customers"],
                color=COLORS["accent"], linewidth=2, marker="o", markersize=3)
        step = max(1, len(join_trend) // 8)
        ax.set_xticks(range(0, len(join_trend), step))
        ax.set_xticklabels(join_trend["join_month"].iloc[::step], rotation=45, ha="right", fontsize=8)
    ax.set_title("New Customer Registrations Over Time", fontsize=13)
    ax.set_ylabel("New Customers")

    plt.tight_layout()
    save_plot(fig, "01_customer_demographics.png")

    return gender_dist, device_dist, country_dist


# ─────────────────────────────────────────────────────────────
# 3. Product Analysis
# ─────────────────────────────────────────────────────────────

def analyze_products(products_df, spark):
    """Analyze product catalog and generate plots."""
    print("\n" + "-" * 40)
    print("🛍️ PRODUCT ANALYSIS")
    print("-" * 40)

    # Category distribution
    cat_dist = products_df.groupBy("masterCategory").agg(count("*").alias("count")).orderBy(desc("count")).toPandas()
    print(f"\n   Master Category Distribution:\n{cat_dist.to_string(index=False)}")

    # Season distribution (Spark SQL)
    season_dist = spark.sql("""
        SELECT season, COUNT(*) as count
        FROM products
        GROUP BY season
        ORDER BY count DESC
    """).toPandas()

    # Colour distribution
    colour_dist = spark.sql("""
        SELECT baseColour, COUNT(*) as count
        FROM products
        GROUP BY baseColour
        ORDER BY count DESC
        LIMIT 10
    """).toPandas()

    # Gender targeting distribution
    gender_target = products_df.groupBy("gender").agg(count("*").alias("count")).orderBy(desc("count")).toPandas()

    # Sub-category breakdown
    subcat_dist = spark.sql("""
        SELECT masterCategory, subCategory, COUNT(*) as count
        FROM products
        GROUP BY masterCategory, subCategory
        ORDER BY masterCategory, count DESC
    """).toPandas()
    print(f"\n   Sub-Category Breakdown:\n{subcat_dist.to_string(index=False)}")

    # ── Plot 2: Product Catalog Analysis ──
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("Product Catalog Analysis", fontsize=18, fontweight="bold", color="#E2E8F0")

    # Master category
    ax = axes[0, 0]
    bars = ax.bar(range(len(cat_dist)), cat_dist["count"],
                  color=PALETTE[:len(cat_dist)], edgecolor="none")
    ax.set_xticks(range(len(cat_dist)))
    ax.set_xticklabels(cat_dist["masterCategory"], rotation=30, ha="right", fontsize=9)
    ax.set_title("Products by Master Category", fontsize=13)
    ax.set_ylabel("Count")
    for bar, val in zip(bars, cat_dist["count"]):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                str(val), ha="center", fontsize=9, color="#CBD5E1")

    # Season
    ax = axes[0, 1]
    wedges, texts, autotexts = ax.pie(
        season_dist["count"], labels=season_dist["season"],
        colors=PALETTE[:len(season_dist)],
        autopct="%1.1f%%", startangle=90, textprops={"color": "#E2E8F0", "fontsize": 11}
    )
    ax.set_title("Products by Season", fontsize=13)

    # Top colours
    ax = axes[1, 0]
    bars = ax.barh(colour_dist["baseColour"], colour_dist["count"],
                   color=PALETTE[:len(colour_dist)], edgecolor="none", height=0.6)
    ax.set_title("Top 10 Product Colours", fontsize=13)
    ax.set_xlabel("Count")
    ax.invert_yaxis()

    # Gender target
    ax = axes[1, 1]
    bars = ax.bar(range(len(gender_target)), gender_target["count"],
                  color=[COLORS["primary"], COLORS["secondary"], COLORS["accent"]][:len(gender_target)],
                  edgecolor="none")
    ax.set_xticks(range(len(gender_target)))
    ax.set_xticklabels(gender_target["gender"], fontsize=11)
    ax.set_title("Products by Target Gender", fontsize=13)
    ax.set_ylabel("Count")

    plt.tight_layout()
    save_plot(fig, "02_product_analysis.png")

    return cat_dist, season_dist


# ─────────────────────────────────────────────────────────────
# 4. Transaction Analysis
# ─────────────────────────────────────────────────────────────

def analyze_transactions(transactions_df, spark):
    """Analyze transaction patterns and generate plots."""
    print("\n" + "-" * 40)
    print("💰 TRANSACTION ANALYSIS")
    print("-" * 40)

    # Basic stats
    stats = spark.sql("""
        SELECT 
            COUNT(*) as total_transactions,
            COUNT(DISTINCT customer_id) as unique_customers,
            ROUND(AVG(total_amount), 2) as avg_order_value,
            ROUND(MIN(total_amount), 2) as min_order_value,
            ROUND(MAX(total_amount), 2) as max_order_value,
            ROUND(SUM(total_amount), 2) as total_revenue,
            ROUND(AVG(shipment_fee), 2) as avg_shipment_fee,
            ROUND(AVG(promo_amount), 2) as avg_promo_amount
        FROM transactions
    """).toPandas()
    print(f"\n   Transaction Summary:")
    for col_name in stats.columns:
        print(f"     {col_name}: {stats[col_name].values[0]:,.2f}" if isinstance(stats[col_name].values[0], (int, float)) else f"     {col_name}: {stats[col_name].values[0]}")

    # Payment status distribution
    payment_status = spark.sql("""
        SELECT payment_status,
               COUNT(*) as count,
               ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM transactions), 1) as pct
        FROM transactions
        GROUP BY payment_status
        ORDER BY count DESC
    """).toPandas()
    print(f"\n   Payment Status:\n{payment_status.to_string(index=False)}")

    # Payment method distribution
    payment_method = spark.sql("""
        SELECT payment_method, COUNT(*) as count
        FROM transactions
        GROUP BY payment_method
        ORDER BY count DESC
    """).toPandas()

    # Monthly revenue trend
    monthly_revenue = spark.sql("""
        SELECT date_format(created_at, 'yyyy-MM') AS month,
               COUNT(*) as num_transactions,
               ROUND(SUM(total_amount), 2) as revenue
        FROM transactions
        WHERE created_at IS NOT NULL
        GROUP BY month
        ORDER BY month
    """).toPandas()

    # Hourly distribution
    hourly_dist = spark.sql("""
        SELECT hour(created_at) as hour_of_day,
               COUNT(*) as num_transactions
        FROM transactions
        WHERE created_at IS NOT NULL
        GROUP BY hour_of_day
        ORDER BY hour_of_day
    """).toPandas()

    # Day of week distribution
    dow_dist = spark.sql("""
        SELECT dayofweek(created_at) as day_of_week,
               COUNT(*) as num_transactions
        FROM transactions
        WHERE created_at IS NOT NULL
        GROUP BY day_of_week
        ORDER BY day_of_week
    """).toPandas()
    day_names = {1: "Sun", 2: "Mon", 3: "Tue", 4: "Wed", 5: "Thu", 6: "Fri", 7: "Sat"}
    dow_dist["day_name"] = dow_dist["day_of_week"].map(day_names)

    # Order value distribution
    order_values = transactions_df.select("total_amount").toPandas()

    # ── Plot 3: Transaction Analysis ──
    fig, axes = plt.subplots(2, 3, figsize=(18, 10))
    fig.suptitle("Transaction Analysis Dashboard", fontsize=18, fontweight="bold", color="#E2E8F0")

    # Payment status
    ax = axes[0, 0]
    colors_status = [COLORS["accent"] if s == "Success" else COLORS["danger"]
                     for s in payment_status["payment_status"]]
    wedges, texts, autotexts = ax.pie(
        payment_status["count"], labels=payment_status["payment_status"],
        colors=colors_status, autopct="%1.1f%%", startangle=90,
        textprops={"color": "#E2E8F0", "fontsize": 12}
    )
    ax.set_title("Payment Status", fontsize=13)

    # Payment methods
    ax = axes[0, 1]
    bars = ax.barh(payment_method["payment_method"], payment_method["count"],
                   color=PALETTE[:len(payment_method)], edgecolor="none", height=0.6)
    ax.set_title("Payment Methods", fontsize=13)
    ax.set_xlabel("Count")
    ax.invert_yaxis()

    # Monthly revenue trend
    ax = axes[0, 2]
    if len(monthly_revenue) > 0:
        ax.fill_between(range(len(monthly_revenue)), monthly_revenue["revenue"],
                        alpha=0.3, color=COLORS["accent"])
        ax.plot(range(len(monthly_revenue)), monthly_revenue["revenue"],
                color=COLORS["accent"], linewidth=2, marker="o", markersize=4)
        step = max(1, len(monthly_revenue) // 6)
        ax.set_xticks(range(0, len(monthly_revenue), step))
        ax.set_xticklabels(monthly_revenue["month"].iloc[::step], rotation=45, ha="right", fontsize=8)
    ax.set_title("Monthly Revenue Trend", fontsize=13)
    ax.set_ylabel("Revenue ($)")
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))

    # Order value histogram
    ax = axes[1, 0]
    ax.hist(order_values["total_amount"].dropna(), bins=40, color=COLORS["primary"],
            edgecolor="#334155", alpha=0.85)
    ax.set_title("Order Value Distribution", fontsize=13)
    ax.set_xlabel("Total Amount ($)")
    ax.set_ylabel("Frequency")
    ax.axvline(order_values["total_amount"].mean(), color=COLORS["warning"],
               linestyle="--", linewidth=2, label=f"Mean: ${order_values['total_amount'].mean():.0f}")
    ax.legend(fontsize=9)

    # Hourly distribution
    ax = axes[1, 1]
    if len(hourly_dist) > 0:
        ax.bar(hourly_dist["hour_of_day"], hourly_dist["num_transactions"],
               color=COLORS["primary"], edgecolor="none", alpha=0.85)
    ax.set_title("Transactions by Hour of Day", fontsize=13)
    ax.set_xlabel("Hour")
    ax.set_ylabel("Count")

    # Day of week
    ax = axes[1, 2]
    if len(dow_dist) > 0:
        bars = ax.bar(range(len(dow_dist)), dow_dist["num_transactions"],
                      color=PALETTE[:len(dow_dist)], edgecolor="none")
        ax.set_xticks(range(len(dow_dist)))
        ax.set_xticklabels(dow_dist["day_name"], fontsize=10)
    ax.set_title("Transactions by Day of Week", fontsize=13)
    ax.set_ylabel("Count")

    plt.tight_layout()
    save_plot(fig, "03_transaction_analysis.png")

    return stats, payment_status, monthly_revenue


# ─────────────────────────────────────────────────────────────
# 5. Click Stream Analysis
# ─────────────────────────────────────────────────────────────

def analyze_clickstream(clickstream_df, spark):
    """Analyze user click stream behavior."""
    print("\n" + "-" * 40)
    print("🖱️ CLICK STREAM ANALYSIS")
    print("-" * 40)

    # Event distribution
    event_dist = spark.sql("""
        SELECT event_name, COUNT(*) as count
        FROM clickstream
        GROUP BY event_name
        ORDER BY count DESC
    """).toPandas()
    print(f"\n   Event Distribution:\n{event_dist.to_string(index=False)}")

    # Traffic source
    traffic_dist = spark.sql("""
        SELECT traffic_source, COUNT(*) as count
        FROM clickstream
        GROUP BY traffic_source
        ORDER BY count DESC
    """).toPandas()

    # Sessions stats
    session_stats = spark.sql("""
        SELECT COUNT(DISTINCT session_id) as unique_sessions,
               ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT session_id), 1) as avg_events_per_session
        FROM clickstream
    """).toPandas()
    print(f"\n   Unique Sessions: {session_stats['unique_sessions'].values[0]}")
    print(f"   Avg Events/Session: {session_stats['avg_events_per_session'].values[0]}")

    # Hourly event distribution
    hourly_events = spark.sql("""
        SELECT hour(event_time) as hour_of_day,
               COUNT(*) as event_count
        FROM clickstream
        WHERE event_time IS NOT NULL
        GROUP BY hour_of_day
        ORDER BY hour_of_day
    """).toPandas()

    # ── Plot 4: Click Stream Analysis ──
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("Click Stream Behavior Analysis", fontsize=18, fontweight="bold", color="#E2E8F0")

    # Event types
    ax = axes[0, 0]
    bars = ax.barh(event_dist["event_name"], event_dist["count"],
                   color=PALETTE[:len(event_dist)], edgecolor="none", height=0.6)
    ax.set_title("Event Type Distribution", fontsize=13)
    ax.set_xlabel("Count")
    ax.invert_yaxis()

    # Traffic source
    ax = axes[0, 1]
    wedges, texts, autotexts = ax.pie(
        traffic_dist["count"], labels=traffic_dist["traffic_source"],
        colors=PALETTE[:len(traffic_dist)], autopct="%1.1f%%",
        startangle=90, textprops={"color": "#E2E8F0", "fontsize": 12}
    )
    ax.set_title("Traffic Source Distribution", fontsize=13)

    # Hourly events
    ax = axes[1, 0]
    if len(hourly_events) > 0:
        ax.bar(hourly_events["hour_of_day"], hourly_events["event_count"],
               color=COLORS["secondary"], edgecolor="none", alpha=0.85)
    ax.set_title("Click Events by Hour", fontsize=13)
    ax.set_xlabel("Hour of Day")
    ax.set_ylabel("Event Count")

    # Funnel-like view: key events
    ax = axes[1, 1]
    funnel_events = ["view_product", "add_to_cart", "checkout"]
    funnel_data = event_dist[event_dist["event_name"].isin(funnel_events)].copy()
    funnel_data["event_name"] = pd.Categorical(funnel_data["event_name"],
                                               categories=funnel_events, ordered=True)
    funnel_data = funnel_data.sort_values("event_name")
    if len(funnel_data) > 0:
        bars = ax.bar(range(len(funnel_data)), funnel_data["count"],
                      color=[COLORS["primary"], COLORS["warning"], COLORS["accent"]],
                      edgecolor="none")
        ax.set_xticks(range(len(funnel_data)))
        ax.set_xticklabels(["View Product", "Add to Cart", "Checkout"], fontsize=10)
        for bar, val in zip(bars, funnel_data["count"]):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 5,
                    str(val), ha="center", fontsize=11, color="#CBD5E1", fontweight="bold")
    ax.set_title("Conversion Funnel", fontsize=13)
    ax.set_ylabel("Event Count")

    plt.tight_layout()
    save_plot(fig, "04_clickstream_analysis.png")

    return event_dist, traffic_dist


# ─────────────────────────────────────────────────────────────
# 6. Cross-Table Analysis (Joins)
# ─────────────────────────────────────────────────────────────

def cross_table_analysis(spark):
    """Perform cross-table joins and analysis using Spark SQL."""
    print("\n" + "-" * 40)
    print("🔗 CROSS-TABLE ANALYSIS (Joins)")
    print("-" * 40)

    # Revenue by customer country (join customer + transaction)
    revenue_by_country = spark.sql("""
        SELECT c.home_country,
               COUNT(t.booking_id) as num_orders,
               ROUND(SUM(t.total_amount), 2) as total_revenue,
               ROUND(AVG(t.total_amount), 2) as avg_order_value
        FROM transactions t
        JOIN customers c ON t.customer_id = c.customer_id
        WHERE t.payment_status = 'Success'
        GROUP BY c.home_country
        ORDER BY total_revenue DESC
    """).toPandas()
    print(f"\n   Revenue by Country:\n{revenue_by_country.to_string(index=False)}")

    # Revenue by device type
    revenue_by_device = spark.sql("""
        SELECT c.device_type,
               COUNT(t.booking_id) as num_orders,
               ROUND(SUM(t.total_amount), 2) as total_revenue,
               ROUND(AVG(t.total_amount), 2) as avg_order_value
        FROM transactions t
        JOIN customers c ON t.customer_id = c.customer_id
        WHERE t.payment_status = 'Success'
        GROUP BY c.device_type
        ORDER BY total_revenue DESC
    """).toPandas()
    print(f"\n   Revenue by Device Type:\n{revenue_by_device.to_string(index=False)}")

    # Payment success rate by payment method
    success_rate = spark.sql("""
        SELECT payment_method,
               COUNT(*) as total,
               SUM(CASE WHEN payment_status = 'Success' THEN 1 ELSE 0 END) as success_count,
               ROUND(SUM(CASE WHEN payment_status = 'Success' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as success_rate
        FROM transactions
        GROUP BY payment_method
        ORDER BY success_rate DESC
    """).toPandas()
    print(f"\n   Payment Success Rate by Method:\n{success_rate.to_string(index=False)}")

    # ── Plot 5: Cross-Table Insights ──
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    fig.suptitle("Cross-Table Insights", fontsize=18, fontweight="bold", color="#E2E8F0")

    # Revenue by country
    ax = axes[0]
    top8 = revenue_by_country.head(8)
    bars = ax.bar(range(len(top8)), top8["total_revenue"],
                  color=PALETTE[:len(top8)], edgecolor="none")
    ax.set_xticks(range(len(top8)))
    ax.set_xticklabels(top8["home_country"], rotation=45, ha="right", fontsize=9)
    ax.set_title("Revenue by Country (Top 8)", fontsize=13)
    ax.set_ylabel("Total Revenue ($)")
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))

    # Revenue by device
    ax = axes[1]
    bars = ax.bar(range(len(revenue_by_device)), revenue_by_device["total_revenue"],
                  color=[COLORS["primary"], COLORS["secondary"], COLORS["accent"]][:len(revenue_by_device)],
                  edgecolor="none")
    ax.set_xticks(range(len(revenue_by_device)))
    ax.set_xticklabels(revenue_by_device["device_type"], fontsize=11)
    ax.set_title("Revenue by Device Type", fontsize=13)
    ax.set_ylabel("Total Revenue ($)")
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))

    # Success rate by payment method
    ax = axes[2]
    bars = ax.barh(success_rate["payment_method"], success_rate["success_rate"],
                   color=PALETTE[:len(success_rate)], edgecolor="none", height=0.6)
    ax.set_title("Payment Success Rate by Method (%)", fontsize=13)
    ax.set_xlabel("Success Rate (%)")
    ax.set_xlim(0, 105)
    for bar, val in zip(bars, success_rate["success_rate"]):
        ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
                f"{val}%", va="center", fontsize=10, color="#CBD5E1")
    ax.invert_yaxis()

    plt.tight_layout()
    save_plot(fig, "05_cross_table_insights.png")

    return revenue_by_country, revenue_by_device


# ─────────────────────────────────────────────────────────────
# 7. Key Findings Summary
# ─────────────────────────────────────────────────────────────

def generate_summary_report(summary_data, stats, payment_status, spark):
    """Generate a text summary of key EDA findings."""
    print("\n" + "=" * 60)
    print("📝 KEY FINDINGS SUMMARY")
    print("=" * 60)

    total_revenue = stats["total_revenue"].values[0]
    avg_order = stats["avg_order_value"].values[0]
    success_rate = payment_status[payment_status["payment_status"] == "Success"]["pct"].values
    success_pct = success_rate[0] if len(success_rate) > 0 else "N/A"

    findings = f"""
KEY FINDINGS FROM EDA
=====================

1. DATASET OVERVIEW
   - 4 interconnected tables: Customer, Product, Transaction, ClickStream
   - Tables: {', '.join([f'{name} ({rows:,} rows)' for name, rows, _ in summary_data])}

2. TRANSACTION INSIGHTS
   - Total Revenue: ${total_revenue:,.2f}
   - Average Order Value: ${avg_order:,.2f}
   - Payment Success Rate: {success_pct}%
   - Most data spans 2023-2024

3. CUSTOMER DEMOGRAPHICS
   - Customers across 10 countries
   - Mix of Android, iOS, and Web users
   - Registration trend shows adoption across 2020-2023

4. PRODUCT CATALOG
   - Fashion-focused e-commerce platform
   - Products span Apparel, Accessories, Footwear, Personal Care
   - Seasonal and gender-targeted product segmentation

5. CLICK STREAM BEHAVIOR
   - Multiple event types captured (view, cart, checkout, search, etc.)
   - Conversion funnel from view_product → add_to_cart → checkout
   - Traffic from mobile, web, and tablet sources

6. NEXT STEPS
   - Build complex Spark SQL queries (cohort analysis, CLV, funnel metrics)
   - Implement streaming component for real-time click stream processing
   - Train MLlib models for payment status prediction
   - Customer segmentation via KMeans clustering
"""
    print(findings)

    # Save summary to file
    summary_path = os.path.join(EDA_OUTPUT_DIR, "eda_summary.txt")
    with open(summary_path, "w") as f:
        f.write(findings)
    print(f"   📄 Summary saved: {summary_path}")

    return findings


# ─────────────────────────────────────────────────────────────
# Main EDA Runner
# ─────────────────────────────────────────────────────────────

def run_eda():
    """Execute the complete EDA pipeline."""
    ensure_dirs()
    spark = get_spark_session("ECommerce-EDA")

    print("\n" + "=" * 60)
    print("🔬 STARTING EXPLORATORY DATA ANALYSIS")
    print("=" * 60)

    # Load data
    customers_df = load_customers(spark)
    products_df = load_products(spark)
    transactions_df = load_transactions(spark)
    clickstream_df = load_clickstream(spark)

    # Run analyses
    summary_data = dataset_overview(customers_df, products_df, transactions_df, clickstream_df, spark)
    analyze_customers(customers_df, spark)
    analyze_products(products_df, spark)
    stats, payment_status, monthly_revenue = analyze_transactions(transactions_df, spark)
    analyze_clickstream(clickstream_df, spark)
    cross_table_analysis(spark)

    # Generate summary
    generate_summary_report(summary_data, stats, payment_status, spark)

    print("\n" + "=" * 60)
    print("✅ EDA COMPLETE — All plots saved to outputs/eda/")
    print("=" * 60 + "\n")

    spark.stop()


if __name__ == "__main__":
    run_eda()
