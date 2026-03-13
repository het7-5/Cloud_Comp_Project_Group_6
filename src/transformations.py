"""
Data Transformations & Spark SQL Queries Module
================================================
Demonstrates complex Spark SQL queries including:
- Multi-level aggregations with window functions
- Customer Lifetime Value (CLV) computation
- Conversion funnel analysis
- Revenue trend analysis with rolling averages
- Promo code effectiveness analysis

Spark Components Used: Structured APIs + Spark SQL
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.utils import get_spark_session, DATA_SAMPLE_DIR, SQL_OUTPUT_DIR, ensure_dirs
from src.ingestion import load_customers, load_products, load_transactions, load_clickstream

from pyspark.sql.functions import col, desc


def register_views(spark, customers_df, products_df, transactions_df, clickstream_df):
    """Register DataFrames as Spark SQL temp views."""
    customers_df.createOrReplaceTempView("customers")
    products_df.createOrReplaceTempView("products")
    transactions_df.createOrReplaceTempView("transactions")
    clickstream_df.createOrReplaceTempView("clickstream")


# ─────────────────────────────────────────────────────────────
# Query 1: Revenue by Category & Season (window functions)
# ─────────────────────────────────────────────────────────────

def query_revenue_by_category_season(spark):
    """Multi-level aggregation: revenue by payment method with ranking."""
    print("\n📊 Query 1: Revenue by Payment Method with Ranking")
    result = spark.sql("""
        SELECT 
            payment_method,
            payment_status,
            COUNT(*) AS num_transactions,
            ROUND(SUM(total_amount), 2) AS total_revenue,
            ROUND(AVG(total_amount), 2) AS avg_order_value,
            DENSE_RANK() OVER (
                PARTITION BY payment_status 
                ORDER BY SUM(total_amount) DESC
            ) AS revenue_rank
        FROM transactions
        GROUP BY payment_method, payment_status
        ORDER BY payment_status, revenue_rank
    """)
    result.show(20, truncate=False)
    return result


# ─────────────────────────────────────────────────────────────
# Query 2: Customer Lifetime Value (CLV)
# ─────────────────────────────────────────────────────────────

def query_customer_ltv(spark):
    """Compute Customer Lifetime Value with ranking."""
    print("\n📊 Query 2: Customer Lifetime Value (Top 15)")
    result = spark.sql("""
        WITH customer_metrics AS (
            SELECT 
                t.customer_id,
                c.first_name,
                c.last_name,
                c.home_country,
                c.device_type,
                COUNT(DISTINCT t.booking_id) AS total_orders,
                ROUND(SUM(t.total_amount), 2) AS lifetime_value,
                ROUND(AVG(t.total_amount), 2) AS avg_order_value,
                MIN(t.created_at) AS first_order,
                MAX(t.created_at) AS last_order,
                ROUND(SUM(t.promo_amount), 2) AS total_promo_used
            FROM transactions t
            JOIN customers c ON t.customer_id = c.customer_id
            WHERE t.payment_status = 'Success'
            GROUP BY t.customer_id, c.first_name, c.last_name, c.home_country, c.device_type
        )
        SELECT *,
               DENSE_RANK() OVER (ORDER BY lifetime_value DESC) AS ltv_rank
        FROM customer_metrics
        ORDER BY ltv_rank
        LIMIT 15
    """)
    result.show(truncate=False)
    return result


# ─────────────────────────────────────────────────────────────
# Query 3: Promo Code Effectiveness
# ─────────────────────────────────────────────────────────────

def query_promo_effectiveness(spark):
    """Analyze promo code usage and effectiveness."""
    print("\n📊 Query 3: Promo Code Effectiveness")
    result = spark.sql("""
        SELECT 
            CASE WHEN promo_code = '' OR promo_code IS NULL 
                 THEN 'No Promo' ELSE promo_code END AS promo_code,
            COUNT(*) AS num_transactions,
            ROUND(AVG(total_amount), 2) AS avg_order_value,
            ROUND(SUM(total_amount), 2) AS total_revenue,
            ROUND(AVG(promo_amount), 2) AS avg_discount,
            ROUND(
                SUM(CASE WHEN payment_status = 'Success' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
                1
            ) AS success_rate_pct
        FROM transactions
        GROUP BY CASE WHEN promo_code = '' OR promo_code IS NULL 
                      THEN 'No Promo' ELSE promo_code END
        ORDER BY num_transactions DESC
    """)
    result.show(truncate=False)
    return result


# ─────────────────────────────────────────────────────────────
# Query 4: Monthly Revenue with Running Total
# ─────────────────────────────────────────────────────────────

def query_monthly_revenue_trend(spark):
    """Monthly revenue with running total and month-over-month growth."""
    print("\n📊 Query 4: Monthly Revenue Trend with Running Total")
    result = spark.sql("""
        WITH monthly AS (
            SELECT 
                date_format(created_at, 'yyyy-MM') AS month,
                COUNT(*) AS num_transactions,
                ROUND(SUM(total_amount), 2) AS monthly_revenue
            FROM transactions
            WHERE payment_status = 'Success' AND created_at IS NOT NULL
            GROUP BY date_format(created_at, 'yyyy-MM')
        )
        SELECT 
            month,
            num_transactions,
            monthly_revenue,
            ROUND(SUM(monthly_revenue) OVER (ORDER BY month), 2) AS cumulative_revenue,
            ROUND(
                (monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY month)) * 100.0 / 
                NULLIF(LAG(monthly_revenue) OVER (ORDER BY month), 0), 
                1
            ) AS mom_growth_pct
        FROM monthly
        ORDER BY month
    """)
    result.show(30, truncate=False)
    return result


# ─────────────────────────────────────────────────────────────
# Query 5: Click Stream Funnel Analysis
# ─────────────────────────────────────────────────────────────

def query_clickstream_funnel(spark):
    """Analyze the conversion funnel from click stream events."""
    print("\n📊 Query 5: Click Stream Conversion Funnel")
    result = spark.sql("""
        WITH session_events AS (
            SELECT 
                session_id,
                MAX(CASE WHEN event_name = 'view_product' THEN 1 ELSE 0 END) AS viewed,
                MAX(CASE WHEN event_name = 'add_to_cart' THEN 1 ELSE 0 END) AS added_to_cart,
                MAX(CASE WHEN event_name = 'checkout' THEN 1 ELSE 0 END) AS checked_out,
                COUNT(*) AS total_events
            FROM clickstream
            GROUP BY session_id
        )
        SELECT 
            COUNT(*) AS total_sessions,
            SUM(viewed) AS sessions_with_view,
            SUM(added_to_cart) AS sessions_with_cart,
            SUM(checked_out) AS sessions_with_checkout,
            ROUND(SUM(viewed) * 100.0 / COUNT(*), 1) AS view_rate,
            ROUND(SUM(added_to_cart) * 100.0 / NULLIF(SUM(viewed), 0), 1) AS view_to_cart_rate,
            ROUND(SUM(checked_out) * 100.0 / NULLIF(SUM(added_to_cart), 0), 1) AS cart_to_checkout_rate,
            ROUND(AVG(total_events), 1) AS avg_events_per_session
        FROM session_events
    """)
    result.show(truncate=False)
    return result


# ─────────────────────────────────────────────────────────────
# Query 6: Revenue by Country & Customer Segment
# ─────────────────────────────────────────────────────────────

def query_revenue_by_country_segment(spark):
    """Revenue analysis by country with customer segmentation."""
    print("\n📊 Query 6: Revenue by Country with Customer Segments")
    result = spark.sql("""
        WITH customer_orders AS (
            SELECT 
                c.home_country,
                c.customer_id,
                COUNT(t.booking_id) AS order_count,
                SUM(t.total_amount) AS total_spent,
                CASE 
                    WHEN COUNT(t.booking_id) >= 5 THEN 'VIP'
                    WHEN COUNT(t.booking_id) >= 3 THEN 'Regular'
                    ELSE 'Occasional'
                END AS customer_segment
            FROM customers c
            JOIN transactions t ON c.customer_id = t.customer_id
            WHERE t.payment_status = 'Success'
            GROUP BY c.home_country, c.customer_id
        )
        SELECT 
            home_country,
            customer_segment,
            COUNT(*) AS num_customers,
            ROUND(SUM(total_spent), 2) AS segment_revenue,
            ROUND(AVG(total_spent), 2) AS avg_customer_value
        FROM customer_orders
        GROUP BY home_country, customer_segment
        ORDER BY home_country, segment_revenue DESC
    """)
    result.show(30, truncate=False)
    return result


def run_transformations():
    """Execute all SQL queries."""
    ensure_dirs()
    spark = get_spark_session("ECommerce-Transformations")

    print("\n" + "=" * 60)
    print("🔍 RUNNING SPARK SQL QUERIES")
    print("=" * 60)

    # Load data
    customers_df = load_customers(spark)
    products_df = load_products(spark)
    transactions_df = load_transactions(spark)
    clickstream_df = load_clickstream(spark)

    # Register views
    register_views(spark, customers_df, products_df, transactions_df, clickstream_df)

    # Run queries
    query_revenue_by_category_season(spark)
    query_customer_ltv(spark)
    query_promo_effectiveness(spark)
    query_monthly_revenue_trend(spark)
    query_clickstream_funnel(spark)
    query_revenue_by_country_segment(spark)

    print("\n" + "=" * 60)
    print("✅ ALL QUERIES EXECUTED SUCCESSFULLY")
    print("=" * 60 + "\n")

    spark.stop()


if __name__ == "__main__":
    run_transformations()
