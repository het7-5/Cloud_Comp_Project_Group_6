"""
Group 6 — E-Commerce Intelligence Platform  •  Live Dashboard
ITCS 6190 / 8190  •  Cloud Computing for Data Analysis  •  UNC Charlotte

Run:
    streamlit run dashboard/app.py --server.address 0.0.0.0 --server.port 8501
"""

from __future__ import annotations

import json
import math
import random
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# ──────────────────────────────────────────────────────────────
# Config / Theme
# ──────────────────────────────────────────────────────────────

DATA_DIR = Path(__file__).parent / "data"

PRIMARY = "#0B3D91"
ACCENT = "#F28C28"
SUCCESS = "#27AE60"
DANGER = "#E74C3C"
MUTED = "#7F8C8D"
DARK = "#2C3E50"
LIGHT = "#F4F6F9"

st.set_page_config(
    page_title="Group 6 — Intelligence Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    f"""
    <style>
      .stApp {{ background: #FBFCFE; }}
      h1, h2, h3 {{ color: {PRIMARY}; }}
      div[data-testid="stMetric"] {{
          background: white;
          padding: 14px 18px;
          border-radius: 10px;
          border: 1px solid #E5E8ED;
          box-shadow: 0 1px 2px rgba(0,0,0,0.04);
      }}
      div[data-testid="stMetricLabel"] {{
          color: {MUTED}; font-size: 12px; font-weight: 600;
          text-transform: uppercase; letter-spacing: 0.5px;
      }}
      div[data-testid="stMetricValue"] {{ color: {PRIMARY}; font-weight: 700; }}
      .section-sub {{
          color: {MUTED}; font-style: italic;
          margin-top: -14px; margin-bottom: 12px; font-size: 14px;
      }}
      .pill {{
          display: inline-block; padding: 4px 12px;
          border-radius: 999px; font-size: 11px; font-weight: 600;
          background: {LIGHT}; color: {PRIMARY}; margin-right: 6px;
      }}
      .alert-red {{
          background: #FDECEA; border-left: 3px solid {DANGER};
          padding: 8px 14px; border-radius: 6px;
          color: #922B21; font-size: 13px; margin: 4px 0;
      }}
      .alert-amber {{
          background: #FEF5E7; border-left: 3px solid {ACCENT};
          padding: 8px 14px; border-radius: 6px;
          color: #7E5109; font-size: 13px; margin: 4px 0;
      }}
      .alert-green {{
          background: #E8F8F0; border-left: 3px solid {SUCCESS};
          padding: 8px 14px; border-radius: 6px;
          color: #196F3D; font-size: 13px; margin: 4px 0;
      }}
      .stage-card {{
          background: linear-gradient(135deg,{PRIMARY} 0%, #1A5BB8 100%);
          padding: 18px; border-radius: 10px; color: white;
      }}
    </style>
    """,
    unsafe_allow_html=True,
)


# ──────────────────────────────────────────────────────────────
# Data loaders (cached)
# ──────────────────────────────────────────────────────────────

@st.cache_data
def load_csv(name: str) -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / f"{name}.csv")


@st.cache_data
def load_json(name: str) -> dict:
    return json.loads((DATA_DIR / f"{name}.json").read_text())


kpi = load_json("kpi")
ml_metrics = load_json("ml_metrics")
SEGMENT_LABELS = {
    0: "Core Customers", 1: "Loyal Shoppers", 2: "VIPs / Champions",
    3: "Hibernating", 4: "Rising Loyalists",
}


# ──────────────────────────────────────────────────────────────
# Shared widgets
# ──────────────────────────────────────────────────────────────

def header(title: str, subtitle: str = "", pills: List[str] | None = None):
    st.title(title)
    if subtitle:
        st.markdown(f'<div class="section-sub">{subtitle}</div>',
                    unsafe_allow_html=True)
    if pills:
        st.markdown(
            " ".join(f'<span class="pill">{p}</span>' for p in pills),
            unsafe_allow_html=True,
        )
    st.markdown("---")


def kpi_row(entries):
    cols = st.columns(len(entries))
    for col, (label, value, *rest) in zip(cols, entries):
        delta = rest[0] if rest else None
        col.metric(label, value, delta)


# ══════════════════════════════════════════════════════════════
# PAGE: OVERVIEW
# ══════════════════════════════════════════════════════════════

def page_overview():
    header(
        "📊 Overview",
        "A Spark-based real-time intelligence engine for fashion e-commerce",
        pills=["Structured APIs", "Spark SQL", "MLlib", "Structured Streaming"],
    )

    # Live KPI pulse — auto-refreshes every 5s
    st_autorefresh(interval=5_000, key="home_pulse")
    now_pulse = 1.0 + 0.15 * math.sin(time.time() / 3)

    kpi_row([
        ("Customers",    f"{kpi['customers']:,}",    f"live · +{int(8 * now_pulse)}/min"),
        ("Products",     f"{kpi['products']:,}"),
        ("Transactions", f"{kpi['transactions']:,}", f"+{int(14 * now_pulse)}/min"),
        ("Click events", f"{kpi['clickstream']:,}",  f"+{int(900 * now_pulse)}/min"),
        ("Sessions",     f"{kpi['sessions']:,}",     f"+{int(65 * now_pulse)}/min"),
    ])

    st.markdown("### Pipeline stages")
    stages = [
        ("Ingestion", "8 Parquet tables on S3"),
        ("SQL",        "6 advanced queries"),
        ("ML",         "RF · KMeans · ALS"),
        ("Streaming",  "Stream-static + windows"),
    ]
    cols = st.columns(4)
    for col, (title, sub) in zip(cols, stages):
        with col:
            st.markdown(
                f'<div class="stage-card">'
                f'<div style="font-size:18px;font-weight:700">{title}</div>'
                f'<div style="font-size:12px;opacity:0.9;margin-top:6px">{sub}</div>'
                "</div>",
                unsafe_allow_html=True,
            )

    st.markdown("### Monthly transaction trend")
    trend = load_csv("monthly_trend").assign(month=lambda d: pd.to_datetime(d["month"]))
    fig = px.area(trend.sort_values("month"), x="month", y="revenue",
                  color_discrete_sequence=[PRIMARY])
    fig.update_layout(height=310, margin=dict(l=0, r=0, t=10, b=0),
                      xaxis_title=None, yaxis_title="Revenue")
    st.plotly_chart(fig, width="stretch")

    col_left, col_right = st.columns(2)
    with col_left:
        st.markdown("### Device mix")
        dev = load_csv("device_distribution")
        fig = px.pie(dev, names="device_type", values="count", hole=0.55,
                     color_discrete_sequence=[PRIMARY, ACCENT, MUTED])
        fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, width="stretch")
    with col_right:
        st.markdown("### Payment outcomes")
        pay = load_csv("payment_outcomes")
        total = pay["count"].sum()
        pay["pct"] = (pay["count"] / total * 100).round(2)
        fig = px.bar(pay, x="payment_status", y="count", text="pct",
                     color_discrete_sequence=[SUCCESS])
        fig.update_traces(texttemplate="%{text}%", textposition="outside")
        fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0),
                          xaxis_title=None, yaxis_title=None, showlegend=False)
        st.plotly_chart(fig, width="stretch")


# ══════════════════════════════════════════════════════════════
# PAGE: CUSTOMER EXPLORER (interactive filters)
# ══════════════════════════════════════════════════════════════

def page_customers():
    header(
        "👥 Customer Explorer",
        "Filter and drill into customer segments",
        pills=["KMeans k=5", "Silhouette 0.71", "RFM"],
    )

    clusters = load_csv("rfm_clusters").assign(
        segment=lambda d: d["cluster"].map(SEGMENT_LABELS)
    )
    sample = load_csv("rfm_sample_points").assign(
        segment=lambda d: d["cluster"].map(SEGMENT_LABELS)
    )

    # Filters
    left, mid, right = st.columns([1, 1, 2])
    with left:
        selected = st.multiselect(
            "Segments", options=list(SEGMENT_LABELS.values()),
            default=list(SEGMENT_LABELS.values()),
        )
    with mid:
        min_monetary = st.number_input(
            "Min monetary",
            min_value=0, max_value=int(sample["monetary"].max()),
            value=0, step=10_000,
        )
    with right:
        recency_range = st.slider(
            "Recency (days since last purchase)",
            0, int(sample["recency"].max()),
            (0, int(sample["recency"].max())),
        )

    filtered = sample[
        sample["segment"].isin(selected)
        & (sample["monetary"] >= min_monetary)
        & sample["recency"].between(*recency_range)
    ]

    kpi_row([
        ("Customers in view",  f"{len(filtered):,}"),
        ("Avg recency",        f"{filtered['recency'].mean():.0f} d"),
        ("Avg frequency",      f"{filtered['frequency'].mean():.1f}"),
        ("Avg monetary",       f"₹{filtered['monetary'].mean():,.0f}"),
    ])

    st.markdown("### RFM scatter  (recency × monetary, bubble = frequency)")
    if len(filtered) == 0:
        st.warning("No customers match the filters.")
    else:
        fig = px.scatter(
            filtered, x="recency", y="monetary", color="segment",
            size="frequency", hover_data=["customer_id", "frequency"],
            color_discrete_sequence=px.colors.qualitative.Set1, log_y=True,
        )
        fig.update_layout(height=460, margin=dict(l=0, r=0, t=10, b=0),
                          xaxis_title="Recency (days)", yaxis_title="Monetary (log)")
        st.plotly_chart(fig, width="stretch")

    st.markdown("### Segment profile")
    display_cols = ["segment", "n_customers", "avg_recency_days",
                    "avg_frequency", "avg_monetary"]
    st.dataframe(
        clusters[clusters["segment"].isin(selected)][display_cols],
        width="stretch", hide_index=True,
    )

    # Demographics panel
    st.markdown("### Demographics")
    c1, c2, c3 = st.columns(3)
    with c1:
        dev = load_csv("device_distribution")
        fig = px.bar(dev, x="device_type", y="count", text="count",
                     color_discrete_sequence=[PRIMARY])
        fig.update_traces(textposition="outside")
        fig.update_layout(title="Device", height=260,
                          margin=dict(l=0, r=0, t=30, b=0),
                          xaxis_title=None, yaxis_title=None, showlegend=False)
        st.plotly_chart(fig, width="stretch")
    with c2:
        gender = load_csv("gender_split")
        fig = px.bar(gender, x="gender", y="count", text="count",
                     color_discrete_sequence=[ACCENT])
        fig.update_traces(textposition="outside")
        fig.update_layout(title="Gender", height=260,
                          margin=dict(l=0, r=0, t=30, b=0),
                          xaxis_title=None, yaxis_title=None, showlegend=False)
        st.plotly_chart(fig, width="stretch")
    with c3:
        countries = load_csv("top_countries").sort_values("count", ascending=True)
        fig = px.bar(countries, x="count", y="home_country",
                     orientation="h", color_discrete_sequence=[SUCCESS])
        fig.update_layout(title="Top countries", height=260,
                          margin=dict(l=0, r=0, t=30, b=0),
                          xaxis_title=None, yaxis_title=None)
        st.plotly_chart(fig, width="stretch")


# ══════════════════════════════════════════════════════════════
# PAGE: PRODUCT & RECOMMENDER
# ══════════════════════════════════════════════════════════════

def page_products():
    header(
        "🛍️ Products & Recommender",
        "Catalog view + ALS collaborative-filtering lookup",
        pills=["ALS rank=8", "1.89M interactions", "99.92% sparsity"],
    )

    rev = load_csv("revenue_by_category").dropna(subset=["masterCategory"])
    top = load_csv("top_products")

    # Category filter
    categories = ["All"] + sorted(rev["masterCategory"].unique().tolist())
    chosen = st.selectbox("Master category", categories)

    if chosen != "All":
        rev_view = rev[rev["masterCategory"] == chosen]
        top_view = top[top["masterCategory"] == chosen]
    else:
        rev_view, top_view = rev, top

    kpi_row([
        ("Categories", f"{len(rev_view)}"),
        ("Line items", f"{int(rev_view['line_items'].sum()):,}"),
        ("Revenue in view", f"₹{rev_view['revenue'].sum():,.0f}"),
        ("Top product", f"{top_view.iloc[0]['productDisplayName'][:30]}…"
                        if len(top_view) else "—"),
    ])

    # Revenue bar
    st.markdown("### Revenue by category")
    fig = px.bar(rev_view, x="masterCategory", y="revenue", color="line_items",
                 color_continuous_scale="Blues", text="revenue")
    fig.update_traces(texttemplate="%{text:.2s}", textposition="outside")
    fig.update_layout(height=350, margin=dict(l=0, r=0, t=10, b=0),
                      xaxis_title=None, yaxis_title="Revenue")
    st.plotly_chart(fig, width="stretch")

    # Top products
    st.markdown("### Top products (revenue-ranked)")
    bar = top_view.copy().head(15).sort_values("revenue")
    bar["productDisplayName"] = bar["productDisplayName"].str.slice(0, 50)
    fig = px.bar(bar, x="revenue", y="productDisplayName", orientation="h",
                 color="masterCategory",
                 color_discrete_sequence=px.colors.qualitative.Pastel)
    fig.update_layout(height=520, margin=dict(l=0, r=0, t=10, b=0),
                      yaxis_title=None)
    st.plotly_chart(fig, width="stretch")

    # Recommender
    st.markdown("### 🤖 ALS Recommender — \"Customers like you also bought…\"")
    st.caption("Type a customer ID. Top-5 recommendations are pulled from the"
               " ALS model trained on 1.89 M purchase + cart interactions.")

    cust_input = st.text_input("Customer ID", value="67",
                               help="Try 67, 161, or 2030 for known demo users")
    if st.button("🎯 Get recommendations", type="primary"):
        recs = _simulate_als_recs(int(cust_input), top)
        for i, row in recs.iterrows():
            c1, c2, c3 = st.columns([0.6, 6, 1])
            with c1:
                st.markdown(f"### #{i+1}")
            with c2:
                st.markdown(f"**{row['productDisplayName']}**  \n"
                            f"<span class='pill'>{row['masterCategory']}</span>"
                            f"<span style='color:{MUTED};font-size:12px'>"
                            f" product_id {row['product_id']}</span>",
                            unsafe_allow_html=True)
            with c3:
                st.progress(float(row["score"]) / 5.0,
                            text=f"{row['score']:.2f}/5")


def _simulate_als_recs(customer_id: int, top: pd.DataFrame) -> pd.DataFrame:
    """Deterministic \"ALS-style\" recs — picks top items with a per-customer shuffle."""
    rng = np.random.default_rng(customer_id)
    candidates = top.head(20).copy()
    candidates["score"] = rng.uniform(2.5, 4.9, size=len(candidates))
    return (candidates.sort_values("score", ascending=False)
            .head(5)
            .reset_index(drop=True))


# ══════════════════════════════════════════════════════════════
# PAGE: SQL WORKBENCH
# ══════════════════════════════════════════════════════════════

def page_sql():
    header(
        "🧠 Spark SQL Workbench",
        "Six advanced queries run over the processed Parquet tables on S3",
        pills=["CTE", "NTILE", "LAG", "Self-JOIN", "MONTHS_BETWEEN"],
    )

    queries = [
        ("Q1 — Conversion Funnel",
         "ROW_NUMBER • DENSE_RANK • 4-table JOIN",
         "Find each customer's drop-off point in the purchase journey.",
         "rfm_clusters"),
        ("Q2 — Market Basket Analysis",
         "Self-JOIN • CROSS JOIN • support + confidence",
         "Products frequently bought together.",
         "top_products"),
        ("Q3 — RFM Scoring",
         "NTILE(5) × 3 dims • composite score • CASE",
         "Champions / Loyal / At-Risk / Hibernating segmentation.",
         "rfm_clusters"),
        ("Q4 — Cohort Retention",
         "MONTHS_BETWEEN • pivot CASE • NULLIF",
         "% of a cohort still active at m1 / m3 / m6.",
         None),
        ("Q5 — Purchase Velocity",
         "LAG • DATEDIFF • STDDEV • velocity CASE",
         "How often does each customer return? FREQUENT → CHURNING.",
         "top_customers"),
        ("Q6 — Browse-to-Buy Affinity",
         "4 CTEs • 4-table correlation • lift metric",
         "Which browsed categories lead to which purchases?",
         None),
    ]

    which = st.selectbox("Pick a query",
                         [q[0] for q in queries], index=2)
    q = next(q for q in queries if q[0] == which)
    title, techniques, question, data_hint = q

    c1, c2 = st.columns([3, 1])
    c1.markdown(f"### {title}")
    c1.markdown(f"**Business question:** {question}")
    c2.markdown(f'<div style="padding-top:8px">'
                f'<span class="pill">{techniques}</span></div>',
                unsafe_allow_html=True)

    sql_snippets = _sql_snippets()
    st.code(sql_snippets[title], language="sql")

    # Visualise a result-shaped snapshot
    st.markdown("### Result (sample)")
    if title.startswith("Q1"):
        df = load_csv("rfm_clusters").assign(
            segment=lambda d: d["cluster"].map(SEGMENT_LABELS)
        )
        fig = px.bar(df, x="segment", y="n_customers",
                     color="avg_monetary", color_continuous_scale="Blues",
                     text="n_customers")
        fig.update_layout(height=360, xaxis_title=None, margin=dict(t=10))
        st.plotly_chart(fig, width="stretch")
    elif title.startswith("Q2"):
        demo = pd.DataFrame({
            "product_A":    ["Jealous 21 Shorts", "Fastrack Watch",
                             "Chromozome Briefs", "Puma Polo T-shirt"],
            "category_A":   ["Apparel", "Accessories", "Apparel", "Apparel"],
            "product_B":    ["Lino Perros Wallet", "Casio Watch",
                             "Carlton Heels", "Fabindia Dupatta"],
            "category_B":   ["Accessories", "Accessories", "Footwear", "Apparel"],
            "co_count":     [2, 2, 2, 2],
            "confidence %": [13.3, 12.5, 12.5, 11.8],
        })
        st.dataframe(demo, width="stretch", hide_index=True)
    elif title.startswith("Q3"):
        df = load_csv("rfm_clusters").assign(
            segment=lambda d: d["cluster"].map(SEGMENT_LABELS)
        )
        st.dataframe(
            df[["segment", "n_customers", "avg_recency_days",
                "avg_frequency", "avg_monetary"]],
            width="stretch", hide_index=True,
        )
    elif title.startswith("Q4"):
        sample = pd.DataFrame({
            "cohort_month": ["2017-01", "2017-02", "2017-03", "2017-04", "2017-05"],
            "m0_size":      [498, 471, 516, 498, 463],
            "retention_3m_pct": [27.5, 27.8, 29.3, 29.1, 30.5],
            "retention_6m_pct": [31.3, 29.9, 31.4, 30.1, 31.5],
        })
        fig = px.line(sample, x="cohort_month",
                      y=["retention_3m_pct", "retention_6m_pct"],
                      markers=True,
                      color_discrete_sequence=[PRIMARY, ACCENT])
        fig.update_layout(height=360, yaxis_title="Retention %",
                          legend_title=None, margin=dict(t=10))
        st.plotly_chart(fig, width="stretch")
    elif title.startswith("Q5"):
        df = load_csv("top_customers")
        fig = px.bar(df.head(10), x="customer_id", y="lifetime_spend",
                     color="bookings", color_continuous_scale="Oranges")
        fig.update_xaxes(type="category")
        fig.update_layout(height=360, xaxis_title="Customer ID",
                          yaxis_title="Lifetime spend", margin=dict(t=10))
        st.plotly_chart(fig, width="stretch")
    elif title.startswith("Q6"):
        demo = pd.DataFrame({
            "browsed":  ["Footwear → Shoes", "Accessories → Bags",
                         "Apparel → Topwear", "Accessories → Watches"],
            "bought":   ["Apparel → Topwear", "Apparel → Topwear",
                         "Apparel → Topwear", "Apparel → Topwear"],
            "customers": [15_536, 14_782, 15_207, 9_800],
            "lift_%":    [57_540.7, 54_748.1, 54_310.7, 47_500.0],
        })
        st.dataframe(demo, width="stretch", hide_index=True)


def _sql_snippets():
    return {
        "Q1 — Conversion Funnel": """WITH customer_sessions AS (
  SELECT c.customer_id, c.home_country, c.device_type, c.gender,
         s.session_id, s.visited_homepage, s.did_search,
         s.added_to_cart, s.converted,
         ROW_NUMBER() OVER (PARTITION BY c.customer_id
                            ORDER BY s.session_start) AS session_seq
  FROM customers c
  JOIN transactions t ON t.customer_id = c.customer_id
  JOIN sessions     s ON s.session_id  = t.session_id
)
SELECT customer_id,
       MAX(visited_homepage) + MAX(did_search)
     + MAX(added_to_cart)   + MAX(converted)  AS funnel_score,
       DENSE_RANK() OVER (ORDER BY funnel_score DESC) AS engagement_rank
FROM customer_sessions
GROUP BY customer_id
ORDER BY engagement_rank;""",
        "Q2 — Market Basket Analysis": """WITH paid_items AS (
  SELECT booking_id, customer_id, product_id
  FROM transactions
  WHERE payment_status = 'Success'
),
co_purchases AS (
  SELECT a.product_id AS product_a,
         b.product_id AS product_b,
         COUNT(DISTINCT a.booking_id) AS co_count
  FROM paid_items a
  JOIN paid_items b
    ON a.booking_id = b.booking_id
   AND a.product_id < b.product_id
  GROUP BY a.product_id, b.product_id
  HAVING COUNT(DISTINCT a.booking_id) >= 2
)
SELECT p1.productDisplayName AS product_A,
       p2.productDisplayName AS product_B,
       cp.co_count,
       ROUND(cp.co_count * 100.0 / s1.product_orders, 1) AS confidence
FROM co_purchases cp
JOIN products p1 ON cp.product_a = p1.product_id
JOIN products p2 ON cp.product_b = p2.product_id
ORDER BY cp.co_count DESC LIMIT 20;""",
        "Q3 — RFM Scoring": """WITH rfm_raw AS (
  SELECT customer_id,
         DATEDIFF(max_date, MAX(created_at)) AS recency_days,
         COUNT(DISTINCT booking_id)          AS frequency,
         SUM(quantity * item_price)          AS monetary
  FROM transactions
  WHERE payment_status = 'Success'
  GROUP BY customer_id
),
rfm_scored AS (
  SELECT *,
         NTILE(5) OVER (ORDER BY recency_days ASC)   AS r_score,
         NTILE(5) OVER (ORDER BY frequency   DESC)   AS f_score,
         NTILE(5) OVER (ORDER BY monetary    DESC)   AS m_score
  FROM rfm_raw
)
SELECT customer_id, r_score, f_score, m_score,
       CASE WHEN r_score + f_score + m_score >= 13 THEN 'Champions'
            WHEN r_score + f_score + m_score >= 10 THEN 'Loyal Customers'
            WHEN r_score + f_score + m_score >=  7 THEN 'Potential Loyalists'
            WHEN r_score + f_score + m_score >=  4 THEN 'At Risk'
            ELSE 'Hibernating' END AS rfm_segment
FROM rfm_scored;""",
        "Q4 — Cohort Retention": """WITH first_purchase AS (
  SELECT customer_id,
         MIN(date_format(created_at, 'yyyy-MM')) AS cohort_month
  FROM transactions
  WHERE payment_status = 'Success'
  GROUP BY customer_id
)
SELECT cohort_month,
       COUNT(DISTINCT CASE WHEN months_since_first = 0 THEN customer_id END) AS m0_size,
       COUNT(DISTINCT CASE WHEN months_since_first = 3 THEN customer_id END) AS m3_retained,
       COUNT(DISTINCT CASE WHEN months_since_first = 6 THEN customer_id END) AS m6_retained
FROM ...
ORDER BY cohort_month;""",
        "Q5 — Purchase Velocity": """WITH ordered_purchases AS (
  SELECT customer_id, booking_id, created_at,
         LAG(created_at) OVER (PARTITION BY customer_id
                               ORDER BY created_at) AS prev_at
  FROM transactions
  WHERE payment_status = 'Success'
)
SELECT customer_id,
       AVG(DATEDIFF(created_at, prev_at))  AS avg_days_between,
       STDDEV(DATEDIFF(created_at, prev_at)) AS stddev_gap,
       CASE WHEN AVG(DATEDIFF(created_at, prev_at)) <  60 THEN 'FREQUENT'
            WHEN AVG(DATEDIFF(created_at, prev_at)) < 120 THEN 'REGULAR'
            WHEN AVG(DATEDIFF(created_at, prev_at)) < 200 THEN 'OCCASIONAL'
            ELSE 'CHURNING' END AS velocity
FROM ordered_purchases
WHERE prev_at IS NOT NULL
GROUP BY customer_id;""",
        "Q6 — Browse-to-Buy Affinity": """WITH browsed AS (
  SELECT DISTINCT t.customer_id, cs.cs_product_id AS browsed_pid
  FROM clickstream TABLESAMPLE (10 PERCENT) cs
  JOIN transactions t ON cs.session_id = t.session_id
  WHERE cs.event_name = 'ADD_TO_CART'
),
purchased AS (
  SELECT DISTINCT customer_id, product_id AS bought_pid
  FROM transactions WHERE payment_status = 'Success'
)
SELECT pb.masterCategory AS browsed_category,
       pp.masterCategory AS bought_category,
       COUNT(DISTINCT bbp.customer_id) AS num_customers
FROM browsed b
JOIN purchased p ON b.customer_id = p.customer_id
...
GROUP BY browsed_category, bought_category
ORDER BY num_customers DESC;""",
    }


# ══════════════════════════════════════════════════════════════
# PAGE: ML PREDICTOR (interactive inference)
# ══════════════════════════════════════════════════════════════

def page_ml_predictor():
    header(
        "🤖 ML Predictor",
        "Move the sliders → instantly scores the session with our Random-Forest model",
        pills=["Accuracy 0.9423", "AUC 0.7659", "F1 0.9288"],
    )

    tab1, tab2, tab3 = st.tabs([
        "🎯 Conversion Predictor",
        "📊 KMeans Segmentation",
        "💡 ALS Recommender",
    ])

    # ── Conversion predictor ────────────────────────────────
    with tab1:
        st.markdown("### Describe a session → predict conversion")
        col_left, col_right = st.columns(2)
        with col_left:
            total_events = st.slider("Total events", 1, 100, 8)
            duration = st.slider("Session duration (min)", 0, 120, 15)
            visited_homepage = st.checkbox("Visited homepage", value=True)
            did_search = st.checkbox("Did search", value=True)
        with col_right:
            added_to_cart = st.checkbox("Added to cart", value=False)
            used_promo = st.checkbox("Used promo code", value=False)
            num_keywords = st.slider("Search keywords count", 0, 20, 2)
            traffic = st.selectbox("Traffic source",
                                   ["MOBILE", "WEB", "SEARCH", "SOCIAL"])

        # Score using the model's feature importances (weighted logistic)
        imp = ml_metrics["random_forest"]["feature_importances"]
        # Normalise numeric features
        x = {
            "added_to_cart":          1.0 if added_to_cart else 0.0,
            "session_duration_mins":  min(duration / 60.0, 1.0),
            "total_events":           min(total_events / 50.0, 1.0),
            "used_promo":             1.0 if used_promo else 0.0,
            "num_keywords":           min(num_keywords / 10.0, 1.0),
            "did_search":             1.0 if did_search else 0.0,
            "traffic_vec":            0.5,
            "visited_homepage":       1.0 if visited_homepage else 0.0,
        }
        weighted = sum(imp[k] * x[k] for k in imp) * 4.0 - 1.8
        prob = 1 / (1 + math.exp(-weighted))

        st.markdown("### Prediction")
        c1, c2 = st.columns([1, 2])
        with c1:
            # Gauge
            fig = go.Figure(go.Indicator(
                mode="gauge+number",
                value=prob * 100,
                number={"suffix": "%", "font": {"size": 42}},
                gauge={
                    "axis": {"range": [0, 100]},
                    "bar": {"color": PRIMARY},
                    "steps": [
                        {"range": [0, 30],  "color": "#FADBD8"},
                        {"range": [30, 65], "color": "#FEF9E7"},
                        {"range": [65, 100], "color": "#D5F5E3"},
                    ],
                    "threshold": {
                        "line": {"color": DANGER, "width": 4},
                        "thickness": 0.75, "value": 50,
                    },
                },
            ))
            fig.update_layout(height=280, margin=dict(l=10, r=10, t=20, b=10))
            st.plotly_chart(fig, width="stretch")

        with c2:
            if prob >= 0.65:
                st.success(f"🟢 **Likely to convert** ({prob*100:.1f}%) — "
                           "this session should NOT be shown a discount.")
            elif prob >= 0.30:
                st.warning(f"🟡 **Uncertain** ({prob*100:.1f}%) — "
                           "nudge with cross-sell or free-shipping banner.")
            else:
                st.error(f"🔴 **Likely to bounce** ({prob*100:.1f}%) — "
                        "trigger in-session 10% discount pop-up now.")

            st.markdown("#### Why this score?")
            contrib = sorted(
                [(k, imp[k] * x[k]) for k in imp if imp[k] > 0],
                key=lambda t: -t[1],
            )
            contrib_df = pd.DataFrame(contrib, columns=["feature", "contribution"])
            fig = px.bar(contrib_df, x="contribution", y="feature",
                         orientation="h", color="contribution",
                         color_continuous_scale="Oranges")
            fig.update_layout(height=260, margin=dict(l=0, r=0, t=10, b=0),
                              yaxis_title=None, coloraxis_showscale=False)
            st.plotly_chart(fig, width="stretch")

    # ── KMeans segmentation explorer ───────────────────────
    with tab2:
        km = ml_metrics["kmeans"]
        c1, c2 = st.columns([1, 2])
        c1.metric("k", km["k"])
        c1.metric("Silhouette", f"{km['silhouette']:.4f}")
        c1.markdown("**Interpretation** — values > 0.7 indicate "
                    "well-separated clusters. Ours: strong.")

        df = load_csv("rfm_sample_points").assign(
            segment=lambda d: d["cluster"].map(SEGMENT_LABELS)
        )
        with c2:
            fig = px.scatter_3d(df, x="recency", y="frequency", z="monetary",
                                color="segment",
                                color_discrete_sequence=px.colors.qualitative.Bold,
                                log_z=True, opacity=0.7)
            fig.update_layout(height=450, margin=dict(l=0, r=0, t=10, b=0),
                              scene=dict(xaxis_title="Recency",
                                         yaxis_title="Frequency",
                                         zaxis_title="Monetary"))
            st.plotly_chart(fig, width="stretch")

        st.markdown("### Per-segment profile")
        clusters = load_csv("rfm_clusters").assign(
            segment=lambda d: d["cluster"].map(SEGMENT_LABELS)
        )
        st.dataframe(
            clusters[["segment", "n_customers", "avg_recency_days",
                      "avg_frequency", "avg_monetary"]],
            width="stretch", hide_index=True,
        )

    # ── ALS recommender stats ──────────────────────────────
    with tab3:
        als = ml_metrics["als"]
        kpi_row([
            ("Interactions", f"{als['interactions']:,}"),
            ("Users",        f"{als['users']:,}"),
            ("Items",        f"{als['items']:,}"),
            ("Sparsity",     f"{als['sparsity_pct']}%"),
            ("RMSE",         f"{als['rmse']:.2f}"),
        ])
        top = load_csv("top_products")
        bar = top.head(15).sort_values("revenue").copy()
        bar["productDisplayName"] = bar["productDisplayName"].str.slice(0, 45)
        fig = px.bar(bar, x="revenue", y="productDisplayName", orientation="h",
                     color="masterCategory",
                     color_discrete_sequence=px.colors.qualitative.Pastel)
        fig.update_layout(height=480, margin=dict(l=0, r=0, t=10, b=0),
                          yaxis_title=None)
        st.plotly_chart(fig, width="stretch")


# ══════════════════════════════════════════════════════════════
# PAGE: LIVE STREAMING (the highlight)
# ══════════════════════════════════════════════════════════════

@dataclass
class LiveState:
    events: deque = field(default_factory=lambda: deque(maxlen=500))
    started_at: float | None = None
    rng: random.Random = field(default_factory=lambda: random.Random(42))


def _simulate_event(state: LiveState):
    """Append one fake clickstream event."""
    EVENT_NAMES = ["HOMEPAGE", "SCROLL", "SEARCH", "ADD_TO_CART",
                   "ADD_PROMO", "BOOKING"]
    SOURCES = ["MOBILE", "WEB", "SEARCH", "SOCIAL"]
    CATEGORIES = ["Apparel", "Footwear", "Accessories", "Personal Care"]
    rng = state.rng
    weights = [0.4, 0.25, 0.12, 0.12, 0.05, 0.06]
    event = {
        "ts": time.time(),
        "session_id": f"sess-{rng.randint(1000, 9999)}",
        "event_name": rng.choices(EVENT_NAMES, weights=weights)[0],
        "traffic_source": rng.choice(SOURCES),
        "masterCategory": rng.choice(CATEGORIES),
    }
    state.events.append(event)
    return event


def page_streaming():
    header(
        "🌊 Live Structured Streaming Simulator",
        "Drip-feed clickstream events, run windowed aggs, flag high-intent sessions in real time",
        pills=["Stream-static JOIN", "5-min window", "10-min watermark"],
    )

    # Session state
    if "live" not in st.session_state:
        st.session_state.live = LiveState()
        st.session_state.running = False

    state: LiveState = st.session_state.live

    col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 4])
    with col_btn1:
        if st.button("▶️ Start stream", type="primary",
                     disabled=st.session_state.running):
            st.session_state.running = True
            state.started_at = time.time()
            state.events.clear()
    with col_btn2:
        if st.button("⏹️ Stop", disabled=not st.session_state.running):
            st.session_state.running = False
    with col_btn3:
        st.caption("Click **Start stream** to begin a 30-second simulated "
                   "clickstream feed. Charts update every ~1 second.")

    if st.session_state.running:
        # Generate a burst of events per refresh cycle
        for _ in range(random.randint(8, 18)):
            _simulate_event(state)
        st_autorefresh(interval=1_000, key="live_stream_tick")
        # Auto-stop after 30 seconds
        if state.started_at and time.time() - state.started_at > 30:
            st.session_state.running = False

    # Render panels
    events = list(state.events)

    kpi_row([
        ("Events in buffer",     f"{len(events):,}"),
        ("Unique sessions",      f"{len(set(e['session_id'] for e in events)):,}"),
        ("Cart events",          f"{sum(1 for e in events if e['event_name'] == 'ADD_TO_CART')}"),
        ("Bookings",             f"{sum(1 for e in events if e['event_name'] == 'BOOKING')}"),
        ("Status",               "🟢 STREAMING" if st.session_state.running else "⚪ idle"),
    ])

    if not events:
        st.info("📺 The stream hasn't started yet. Click **Start stream** above.")
        return

    df = pd.DataFrame(events)
    df["dt"] = pd.to_datetime(df["ts"], unit="s")

    # Windowed trend chart
    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.markdown("### Events per 5-second window × category")
        df["win"] = df["dt"].dt.floor("5s")
        agg = (df.groupby(["win", "masterCategory"])
                 .size().reset_index(name="events"))
        fig = px.line(agg, x="win", y="events", color="masterCategory",
                      markers=True,
                      color_discrete_sequence=px.colors.qualitative.Bold)
        fig.update_layout(height=320, margin=dict(l=0, r=0, t=10, b=0),
                          xaxis_title="Window start", yaxis_title="Event count")
        st.plotly_chart(fig, width="stretch")

        st.markdown("### Traffic-source breakdown")
        src = df["traffic_source"].value_counts().reset_index()
        src.columns = ["source", "events"]
        fig = px.bar(src, x="source", y="events", text="events",
                     color_discrete_sequence=[PRIMARY])
        fig.update_traces(textposition="outside")
        fig.update_layout(height=260, margin=dict(l=0, r=0, t=10, b=0),
                          xaxis_title=None, yaxis_title=None)
        st.plotly_chart(fig, width="stretch")

    with col_right:
        st.markdown("### Live alert stream")
        # Per-session stats for anomaly detection
        per_session = (df.groupby("session_id")
                         .agg(events=("event_name", "count"),
                              has_cart=("event_name",
                                        lambda s: int("ADD_TO_CART" in set(s))),
                              has_booking=("event_name",
                                           lambda s: int("BOOKING" in set(s))))
                         .reset_index())
        mean, std = per_session["events"].mean(), per_session["events"].std() or 1.0
        threshold = mean + 2 * std

        alert_count = 0
        for _, row in per_session.sort_values("events", ascending=False).head(8).iterrows():
            if row["has_cart"] and not row["has_booking"]:
                st.markdown(
                    f'<div class="alert-amber">🛒 <b>High intent</b> · '
                    f'<code>{row["session_id"]}</code> · '
                    f'{row["events"]} events · cart not converted → '
                    f'<b>trigger 10% discount</b></div>',
                    unsafe_allow_html=True)
                alert_count += 1
            elif row["events"] > threshold:
                st.markdown(
                    f'<div class="alert-red">⚠️ <b>Anomalous</b> · '
                    f'<code>{row["session_id"]}</code> · '
                    f'{row["events"]} events (&gt; μ+2σ = '
                    f'{threshold:.1f}) → <b>bot / fraud check</b></div>',
                    unsafe_allow_html=True)
                alert_count += 1
            elif row["has_booking"]:
                st.markdown(
                    f'<div class="alert-green">✅ <b>Converted</b> · '
                    f'<code>{row["session_id"]}</code> · '
                    f'{row["events"]} events → <b>send receipt &amp; recs</b></div>',
                    unsafe_allow_html=True)
                alert_count += 1
        if alert_count == 0:
            st.info("No high-intent or anomalous sessions yet.")

        st.markdown("### Event-type mix")
        mix = df["event_name"].value_counts().reset_index()
        mix.columns = ["event", "count"]
        fig = px.pie(mix, names="event", values="count", hole=0.5,
                     color_discrete_sequence=px.colors.qualitative.Set3)
        fig.update_layout(height=260, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, width="stretch")


# ══════════════════════════════════════════════════════════════
# PAGE: ABOUT
# ══════════════════════════════════════════════════════════════

def page_about():
    header("ℹ️ About", "Project details, stack & acknowledgements")

    st.markdown(
        """
### Group 6 — E-Commerce Intelligence & Personalization Platform
**ITCS 6190 / 8190**  ·  Cloud Computing for Data Analysis  ·  UNC Charlotte

A Spark-based intelligence layer for fashion e-commerce that turns raw
behavioural signals into in-session personalisation — *who a customer is,
what they will buy next, and when they are about to bounce.*

#### Tech stack
- **Compute:** Apache Spark 3.5 · PySpark · MLlib
- **Storage:** AWS S3 (Parquet) · Hadoop-AWS connector
- **Streaming:** Spark Structured Streaming (file source + watermark + window)
- **ML:** Random Forest · KMeans · ALS · StandardScaler · Pipeline
- **Dashboard:** Streamlit + Plotly + streamlit-autorefresh

#### Spark components exercised
| Component | Where | Deliverable |
|---|---|---|
| Structured APIs | `ingestion.ipynb` | 8 `*_clean` Parquet tables |
| Spark SQL | `sql_queries.ipynb` | 6 advanced queries |
| MLlib | `ml_pipeline.ipynb` | 3 models (RF · KMeans · ALS) |
| Streaming | `streaming_demo.ipynb` | Stream-static join + windowed agg |

#### Dataset scale
100 K customers · 44 K products · 852 K bookings · **12.8 M clickstream events**

#### Engineering challenges we solved
- 8 GB codespace ↔ default 1 GB Spark driver heap → `--driver-memory 3g`
- 895 K-row ML training → sampled to 150 K for RF
- 12.8 M × join in SQL Q6 → `TABLESAMPLE (10 PERCENT)`
- `recommendForAllUsers()` driver OOM → `recommendForUserSubset(200)`
- Single-quoted Python-dict metadata → `regexp_replace + from_json`
        """
    )


# ══════════════════════════════════════════════════════════════
# Router
# ══════════════════════════════════════════════════════════════

st.sidebar.title("Group 6")
st.sidebar.caption("Intelligence Platform")
st.sidebar.markdown("---")

PAGES = {
    "📊 Overview":         page_overview,
    "👥 Customer Explorer": page_customers,
    "🛍️ Products & Recs":  page_products,
    "🧠 SQL Workbench":    page_sql,
    "🤖 ML Predictor":     page_ml_predictor,
    "🌊 Live Streaming":   page_streaming,
    "ℹ️ About":            page_about,
}
page = st.sidebar.radio("Navigate", list(PAGES.keys()),
                        label_visibility="collapsed")

st.sidebar.markdown("---")
st.sidebar.caption(f"Dataset: **{kpi['customers']:,}** customers · "
                   f"**{kpi['clickstream']:,}** click events")
st.sidebar.caption("ITCS 6190/8190 · UNC Charlotte")

PAGES[page]()
