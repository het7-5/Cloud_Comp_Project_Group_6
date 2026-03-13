# Dataset Overview

## Source

**E-commerce App Transactional Dataset**  
- **Platform**: [Kaggle](https://www.kaggle.com/datasets/bytadit/transactional-ecommerce)
- **License**: CC BY-NC-ND 4.0
- **Domain**: Fashion e-commerce (Indonesian market)
- **Total Size**: ~2 GB (4 CSV files)

## Tables

### 1. Customer (`customer.csv`) — ~100,000 rows, ~25 MB

| Column | Type | Description |
|--------|------|-------------|
| `customer_id` | string | Unique customer identifier |
| `first_name` | string | Customer first name |
| `last_name` | string | Customer last name |
| `username` | string | App username |
| `email` | string | Email address |
| `gender` | string | Male / Female |
| `birthdate` | date | Date of birth |
| `device_type` | string | Android / iOS |
| `device_id` | string | Device identifier |
| `device_version` | string | Device model/version |
| `home_location_lat` | double | Home latitude |
| `home_location_long` | double | Home longitude |
| `home_location` | string | City/region name |
| `home_country` | string | Country (Indonesia) |
| `first_join_date` | date | Registration date |

### 2. Product (`product.csv`) — ~44,000 rows, ~4 MB

| Column | Type | Description |
|--------|------|-------------|
| `id` → `product_id` | int | Unique product identifier |
| `gender` | string | Target gender (Men, Women, Unisex, Boys, Girls) |
| `masterCategory` | string | Top-level category (Apparel, Accessories, Footwear, etc.) |
| `subCategory` | string | Sub-category (Topwear, Bottomwear, Shoes, etc.) |
| `articleType` | string | Specific article type (Shirts, Jeans, etc.) |
| `baseColour` | string | Primary colour |
| `season` | string | Summer, Winter, Fall, Spring |
| `year` | int | Year of listing |
| `usage` | string | Casual, Formal, Sports, etc. |
| `productDisplayName` | string | Full product display name |

### 3. Transaction (`transactions.csv`) — ~850,000 rows, ~256 MB

| Column | Type | Description |
|--------|------|-------------|
| `created_at` | timestamp | Transaction timestamp |
| `customer_id` | string | FK → Customer |
| `booking_id` | string | Unique booking identifier |
| `session_id` | string | FK → ClickStream session |
| `product_metadata` | JSON string | `{"items": [{"product_id": int, "quantity": int, "price": double}]}` |
| `payment_method` | string | Credit Card, Gopay, OVO, Debit Card, LinkAja |
| `payment_status` | string | Success / Failed |
| `promo_amount` | double | Discount applied |
| `promo_code` | string | Promo code used (if any) |
| `shipment_fee` | double | Shipping cost |
| `shipment_date_limit` | date | Expected delivery date |
| `shipment_location_lat` | double | Delivery latitude |
| `shipment_location_long` | double | Delivery longitude |
| `total_amount` | double | Total order value (IDR) |

### 4. Click Stream (`click_stream.csv`) — ~12.8M rows, ~1.7 GB

| Column | Type | Description |
|--------|------|-------------|
| `session_id` | string | Session identifier |
| `event_name` | string | CLICK, HOMEPAGE, ADD_TO_CART, SCROLL, view_product, checkout, etc. |
| `event_time` | timestamp | Event timestamp |
| `event_id` | string | Unique event identifier |
| `traffic_source` | string | MOBILE / WEB |
| `event_metadata` | JSON string | `{"page": str, "duration_sec": int, "product_id": int, "query": str, "promo_code": str}` |

## Key Statistics

| Metric | Value |
|--------|-------|
| Total Customers | ~100,000 |
| Total Products | ~44,000 |
| Total Transactions | ~850,000 |
| Total Click Events | ~12.8 million |
| Payment Success Rate | 95.7% |
| Gender Split | 64% Female, 36% Male |
| Device Split | 77% Android, 23% iOS |
| Platform Traffic | 90% Mobile, 10% Web |
| Market | Indonesia (single market) |
| Time Range | 2016–2022 |

## Preprocessing Steps

1. **Schema Validation**: Explicit schemas defined for all 4 tables with proper data types
2. **Timestamp Parsing**: `created_at`, `event_time` → Spark TimestampType; `first_join_date`, `birthdate` → DateType
3. **JSON Parsing**: `product_metadata` and `event_metadata` columns parsed with `from_json()` into structs
4. **String Trimming**: All string columns trimmed of whitespace
5. **Null Handling**: `promo_amount` nulls filled with 0.0
6. **Column Renaming**: Product `id` → `product_id` for join consistency
7. **Output Format**: All cleaned DataFrames saved as Parquet in `data/processed/`
