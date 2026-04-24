"""
Live data generator for the HTML dashboard — CEO-grade metrics.

Regenerates ``dashboard/data/live.json`` every N seconds with evolving
revenue-at-risk, revenue-recovered, forecast, churn risk, interventions,
sliding event feed, and $-weighted alerts.
"""

from __future__ import annotations

import json
import math
import random
import time
from datetime import datetime
from pathlib import Path

DATA = Path(__file__).parent / "data"
LIVE = DATA / "live.json"

EVENT_NAMES = ["HOMEPAGE", "SCROLL", "SEARCH", "ADD_TO_CART",
               "ADD_PROMO", "BOOKING"]
CATEGORIES = ["Apparel", "Footwear", "Accessories", "Personal Care"]
SOURCES = ["MOBILE", "WEB", "SEARCH", "SOCIAL"]

# Alert template: realistic CEO-facing business alerts with $-impact
ALERT_TEMPLATES = [
    {"type": "high_intent",
     "msg":  "42 sessions with cart but no booking — likely to bounce in 3 min",
     "action": "Fire in-session 10% discount pop-up (precision 89%)",
     "impact": 342_000,
     "confidence": 0.89},
    {"type": "anomaly",
     "msg":  "Session velocity spike μ+2.4σ — likely bot/scraper activity",
     "action": "Auto-block IP + rate-limit + flag for fraud review",
     "impact": 58_000,
     "confidence": 0.94},
    {"type": "churn",
     "msg":  "28 VIPs inactive ≥ 14 days — projected lifetime loss $2.1M",
     "action": "Trigger personalised 20% win-back + early-access campaign",
     "impact": 2_100_000,
     "confidence": 0.76},
    {"type": "conversion",
     "msg":  "Conversion on SEARCH traffic up 18% vs baseline last hour",
     "action": "Increase SEARCH-channel paid bids by 15% (ROI 3.2×)",
     "impact": 127_000,
     "confidence": 0.82},
    {"type": "high_intent",
     "msg":  "Premium watch (#17247) viewed 3× by 12 customers — hot product",
     "action": "Push to homepage hero + promote in retargeting ads",
     "impact": 285_000,
     "confidence": 0.71},
    {"type": "stockout",
     "msg":  "Inventory low on top-seller SKU 15970 (47 units left)",
     "action": "Trigger reorder + cap promo exposure until restock",
     "impact": 195_000,
     "confidence": 0.98},
    {"type": "revenue_leak",
     "msg":  "Payment failures up 2.1× on Debit Card route (last 20 min)",
     "action": "Failover to secondary gateway + notify payment team",
     "impact": 89_000,
     "confidence": 0.91},
]


def live_snapshot(start_ts: float, session_state: dict):
    dt = time.time() - start_ts
    pulse = 1.0 + 0.3 * math.sin(dt / 6)

    orders       = int(120 + dt * 4.5 * pulse + random.randint(0, 5))
    success      = int(orders * 0.957)
    abandoned    = int(orders * 0.42 + random.randint(-3, 3))
    sessions     = int(1247 + dt * 9.0 * pulse + random.randint(0, 12))
    shoppers     = int(340 + 60 * pulse + random.randint(-20, 25))
    revenue      = int(orders * 285_000 * pulse)
    at_risk      = int(abandoned * 0.6 + random.randint(-2, 2))

    # ─── CEO-grade metrics ───
    avg_cart_value     = 420_000
    revenue_at_risk    = int(abandoned * avg_cart_value * 0.35)  # cart × churn prob
    # Revenue recovered today (grows monotonically as interventions fire)
    session_state["revenue_recovered"] += random.randint(25_000, 85_000)
    revenue_recovered  = session_state["revenue_recovered"]
    # 7-day revenue forecast (based on current run-rate × ARIMA-ish growth 4%)
    forecast_7d        = int(revenue * 7 * 1.042)
    forecast_30d       = int(revenue * 30 * 1.038)
    # Net lift from dashboard interventions
    baseline_rev       = int(revenue * 0.815)  # what revenue would be without us
    net_lift_pct       = round((revenue - baseline_rev) / baseline_rev * 100, 1)
    # Churn risk customers (VIPs > 14 days inactive)
    churn_risk         = int(28 + random.randint(-2, 4))
    churn_saved_today  = session_state["churn_saved"]
    if random.random() < 0.25:
        session_state["churn_saved"] += random.randint(1, 3)
    # Interventions taken
    if random.random() < 0.35:
        session_state["interventions"] += random.randint(1, 4)
    interventions      = session_state["interventions"]
    # Conversion rate (live)
    conversion_rate    = round(34.2 + 2 * math.sin(dt / 5), 2)
    aov                = round(avg_cart_value * pulse)
    # Customer Lifetime Value (weighted avg)
    clv_avg            = int(4_850_000 + 100_000 * math.sin(dt / 8))

    # Spark sparklines
    def series(base, span):
        return [int(base + random.randint(-span, span)) for _ in range(12)]

    rev_hist = [int(revenue * (0.8 + 0.1 * random.random()) * (0.5 + i / 30))
                for i in range(30)]

    feed = []
    for _ in range(8):
        feed.append({
            "ts": int(time.time() * 1000) - random.randint(0, 30_000),
            "event": random.choice(EVENT_NAMES),
            "source": random.choice(SOURCES),
            "category": random.choice(CATEGORIES),
            "session": f"sess-{random.randint(1000, 9999)}",
        })
    feed.sort(key=lambda x: -x["ts"])

    # Pick 4 alerts, fresh session IDs
    random.seed(int(time.time() // 6))
    alerts = []
    picked = random.sample(ALERT_TEMPLATES, k=min(4, len(ALERT_TEMPLATES)))
    for a in picked:
        alerts.append({
            **a,
            "session_id":  f"sess-{random.randint(1000, 9999)}",
            "detected_at": datetime.now().strftime("%H:%M:%S"),
        })
    random.seed()

    traffic_mix = {
        "MOBILE": int(65 + 5 * pulse),
        "WEB":    int(22 - 3 * pulse),
        "SEARCH": int(8 + random.randint(-2, 2)),
        "SOCIAL": int(5 + random.randint(-1, 1)),
    }

    # Channel performance (by traffic source × conversion × AOV × volume)
    channels = [
        {"source": "MOBILE", "sessions": int(sessions*0.65), "conv_rate": 0.342,
         "aov": 420_000, "revenue_today": int(sessions*0.65 * 0.342 * 420_000 / 1000)},
        {"source": "WEB",    "sessions": int(sessions*0.22), "conv_rate": 0.289,
         "aov": 565_000, "revenue_today": int(sessions*0.22 * 0.289 * 565_000 / 1000)},
        {"source": "SEARCH", "sessions": int(sessions*0.08), "conv_rate": 0.412,
         "aov": 380_000, "revenue_today": int(sessions*0.08 * 0.412 * 380_000 / 1000)},
        {"source": "SOCIAL", "sessions": int(sessions*0.05), "conv_rate": 0.231,
         "aov": 295_000, "revenue_today": int(sessions*0.05 * 0.231 * 295_000 / 1000)},
    ]

    # ─── Growth & forecast signals (initiatives 1-12) ───
    # 90-day daily revenue forecast (mean + 90% CI bands)
    daily_rev_base = revenue  # today's run rate
    forecast_daily = []
    for d in range(90):
        growth = 1 + (d / 90) * 0.12          # 12% growth by day 90
        seasonal = 1 + 0.08 * math.sin(d / 7) # weekly cycle
        mean_v   = daily_rev_base * growth * seasonal
        forecast_daily.append({
            "day":   d,
            "mean":  int(mean_v),
            "lower": int(mean_v * 0.88),
            "upper": int(mean_v * 1.14),
        })

    churn_dollars_quarter = int(churn_risk * 4_850_000 * 0.58)  # Σ(P(churn) × CLV)
    promo_tp_dollars      = revenue_recovered
    promo_fp_dollars      = int(interventions * 28_500 * 0.22)  # FP rate ≈ 22%
    promo_efficiency      = round(promo_tp_dollars /
                                  max(promo_tp_dollars + promo_fp_dollars, 1) * 100, 1)

    # Category stock-out risk (simulated days-of-inventory)
    categories_stock = [
        {"cat": "Apparel",       "doi": 12.4, "risk": "low"},
        {"cat": "Footwear",      "doi": 6.8,  "risk": "medium"},
        {"cat": "Accessories",   "doi": 3.2,  "risk": "high"},
        {"cat": "Personal Care", "doi": 18.9, "risk": "low"},
    ]
    for c in categories_stock:
        c["doi"] = round(c["doi"] + random.uniform(-0.5, 0.5), 1)

    # VIP activity health
    vip_health = {
        "total_vips":       394,
        "active_7d":        int(350 + random.randint(-8, 8)),
        "inactive_14d":     churn_risk,
        "saved_this_week":  churn_saved_today + random.randint(2, 6),
        "lifetime_value":   "₹115.9M avg",
    }

    return {
        "updated_at":  datetime.now().strftime("%H:%M:%S"),
        "updated_ms":  int(time.time() * 1000),
        "kpis": {
            "orders": orders, "success": success, "abandoned": abandoned,
            "sessions": sessions, "shoppers": shoppers,
            "revenue": revenue, "at_risk": at_risk,
        },
        "growth": {
            "forecast_daily":        forecast_daily,
            "churn_dollars_quarter": churn_dollars_quarter,
            "promo_tp_dollars":      promo_tp_dollars,
            "promo_fp_dollars":      promo_fp_dollars,
            "promo_efficiency_pct":  promo_efficiency,
            "categories_stock":      categories_stock,
            "vip_health":            vip_health,
        },
        "ceo": {
            "revenue_at_risk":   revenue_at_risk,
            "revenue_recovered": revenue_recovered,
            "forecast_7d":       forecast_7d,
            "forecast_30d":      forecast_30d,
            "net_lift_pct":      net_lift_pct,
            "baseline_rev":      baseline_rev,
            "churn_risk":        churn_risk,
            "churn_saved_today": churn_saved_today,
            "interventions":     interventions,
            "conversion_rate":   conversion_rate,
            "aov":               aov,
            "clv_avg":           clv_avg,
        },
        "channels": channels,
        "sparks": {
            "orders":   series(orders, 20),
            "success":  series(success, 15),
            "abandon":  series(abandoned, 10),
            "sessions": series(sessions, 25),
        },
        "revenue_history": rev_hist,
        "traffic_mix":     traffic_mix,
        "feed":            feed,
        "alerts":          alerts,
    }


def main(interval: float = 2.0):
    DATA.mkdir(parents=True, exist_ok=True)
    start = time.time()
    session_state = {"revenue_recovered": 1_250_000,
                     "churn_saved": 4,
                     "interventions": 47}
    print(f"live_feed → {LIVE} every {interval}s (ctrl-c to stop)")
    while True:
        snap = live_snapshot(start, session_state)
        tmp = LIVE.with_suffix(".tmp")
        tmp.write_text(json.dumps(snap))
        tmp.replace(LIVE)
        time.sleep(interval)


if __name__ == "__main__":
    main()
