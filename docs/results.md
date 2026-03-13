# Results

## EDA Key Findings

### Customer Demographics
- **100,000 customers**, 64.2% Female / 35.8% Male
- **76.6% Android**, 23.4% iOS — mobile-first platform
- **100% Indonesia-based** — single market e-commerce app
- Steady customer growth 2016–2022, acceleration in 2020–2022

### Product Catalog
- **44,000 products** across Apparel (48%), Accessories (26%), Footwear (21%)
- **Summer products** make up 48.3% of catalog
- Black is the #1 colour (10K+ products), followed by White and Blue
- Products skew slightly toward Men vs Women

### Transaction Patterns
- **850,000 transactions** with **95.7% payment success rate**
- Credit Card is the most popular payment method (~300K transactions)
- Indonesian e-wallets well represented: Gopay, OVO, Debit Card, LinkAja
- Revenue grew exponentially from 2016 to 2022
- Peak activity: 10am–8pm, weekends (Saturday/Sunday) highest volume
- Order values heavily right-skewed (mean ~550K IDR, median lower)

### Click Stream Behavior
- **12.8 million events** across ~900K sessions
- Top events: CLICK (2.5M), HOMEPAGE (2.5M), ADD_TO_CART (1.9M), SCROLL (1.7M)
- **90% Mobile traffic**, 10% Web
- Average **14.3 events per session** — good engagement
- Fairly uniform activity across hours, slight afternoon peak

### Cross-Table Insights
- Android generates ~3.5x more revenue than iOS (proportional to user base)
- Payment success rate uniformly high across all methods (~95.6–95.8%)
- OVO has the highest success rate (95.8%), LinkAja the lowest (95.6%)

---

## SQL Query Results

### Query 1: Revenue by Payment Method with Ranking
- Credit Card leads in both transaction count and total revenue
- `DENSE_RANK()` reveals consistent ordering across Success/Failed statuses
- **Spark SQL features:** `DENSE_RANK()`, `PARTITION BY`, `GROUP BY`, window functions

### Query 2: Customer Lifetime Value (Top 15)
- Top 15 customers show CLV values significantly above the mean
- High-CLV customers use a mix of payment methods
- **Spark SQL features:** CTEs (`WITH`), `JOIN`, `DENSE_RANK()`, multi-table aggregation

### Query 3: Promo Code Effectiveness
- Promo code usage has minimal impact on payment success rates
- Customers with promo codes have slightly higher average order values
- **Spark SQL features:** `CASE WHEN`, conditional aggregation, percentage calculation

### Query 4: Monthly Revenue with Running Total & MoM Growth
- Month-over-month growth shows consistent upward trend
- Cumulative revenue follows an exponential curve
- Seasonal dips visible in certain months
- **Spark SQL features:** `SUM() OVER`, `LAG()`, window frames, `NULLIF`

### Query 5: Click Stream Conversion Funnel
- Sessions progress through: Homepage → Product View → Add to Cart → Checkout
- Drop-off rates quantified at each funnel stage
- **Spark SQL features:** `CASE WHEN` pivoting, session-level aggregation, `NULLIF`

### Query 6: Revenue by Country & Customer Segment
- VIP customers (5+ orders) generate disproportionate revenue
- Occasional customers (1-2 orders) form the largest segment by count
- **Spark SQL features:** Nested CTE, `CASE WHEN` segmentation, multi-level aggregation

---

## ML Pipeline Results

### Task 1: Payment Status Classification

**Target:** `payment_status` (Success / Failed)  
**Class distribution:** 95.7% Success, 4.3% Failed (highly imbalanced)  
**Approach:** Class weights applied to handle imbalance — minority class gets ~11x weight  
**Dataset:** ~50K sampled rows (from 852K) with 80/20 train/test split

| Model | AUC | F1 | Precision | Recall | Accuracy |
|-------|:---:|:---:|:---------:|:------:|:--------:|
| Logistic Regression | 0.5031 | 0.7367 | 0.9120 | 0.6301 | 63.01% |
| Random Forest (20 trees) | 0.4926 | 0.9314 | 0.9099 | 0.9539 | 95.39% |
| **Gradient-Boosted Trees** | **0.5226** | 0.7259 | 0.9152 | 0.6160 | 61.60% |

**Analysis:**
- The AUC scores near 0.5 indicate that payment status is **not easily predictable** from the available features (amount, payment method, device type)
- Random Forest achieves highest accuracy but predicts almost everything as "Success" (majority class)
- With class weights, Logistic Regression and GBT correctly identify some "Failed" transactions, though at the cost of overall accuracy
- This suggests that payment failure is likely driven by factors not captured in our features (e.g., bank-side issues, network errors)

### Task 2: Customer Segmentation (KMeans Clustering)

**Features:** total_spend, num_transactions, avg_order_value, num_sessions, promo_usage_rate, payment_methods_used  
**Scaling:** StandardScaler applied before clustering  
**Optimal k:** Selected via Silhouette Score

| k | Silhouette Score |
|:-:|:---------------:|
| 3 | 0.4940 |
| 4 | 0.5634 |
| **5** | **0.6289** |
| 6 | 0.4005 |
| 8 | 0.5060 |

**Best k = 5** (Silhouette = 0.6289 — strong cluster separation)

**Cluster Profiles (50,242 customers):**

| Cluster | Customers | Avg Spend (IDR) | Avg Txns | Avg Order Value | Segment Label |
|:-------:|:---------:|:---------------:|:--------:|:---------------:|:-------------:|
| 0 | 31,397 | 2.59M | 5.2 | 460K | **Casual Shoppers** |
| 1 | 2,539 | 383K | 1.7 | 217K | **Budget Shoppers** |
| 2 | 1,827 | 71.2M | 128.4 | 558K | **Power Buyers** |
| 3 | 12,193 | 18.4M | 33.3 | 562K | **Regular Customers** |
| 4 | 2,286 | 5.26M | 2.9 | 2.05M | **Big-Ticket Buyers** |

**Insights:**
- **Cluster 0 (Casual Shoppers):** Largest group (62.5%), moderate spending, ~5 transactions
- **Cluster 1 (Budget Shoppers):** Low engagement, few transactions, smallest order values
- **Cluster 2 (Power Buyers):** Small group (<4%) but extreme engagement — 128 average transactions!
- **Cluster 3 (Regular Customers):** 24.3% of customers, consistent mid-high spending
- **Cluster 4 (Big-Ticket Buyers):** Very high average order value (2M+ IDR) but few transactions — likely premium product purchasers

---

## Streaming Results

- Simulated real-time click stream event processing using Spark Structured Streaming
- Stream Simulator wrote click stream rows as JSON files to `data/stream_input/`
- Structured Streaming consumer processed events with:
  - **5-minute tumbling window** aggregations for event counts per event type
  - **Approximate distinct session counts** per window (using `approx_count_distinct`)
  - **Traffic source breakdown** (Mobile vs Web) per window
- Watermarking with 10-minute late data tolerance
- Results output to console and Parquet sink in `data/stream_output/traffic_breakdown/`
