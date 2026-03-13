# Limitations

## Data Limitations

### Single Market Bias
- All customers are based in **Indonesia** — results are not generalizable to other markets
- Geographic analysis is limited since there is no cross-country variation
- Currency values are in IDR (Indonesian Rupiah)

### Class Imbalance (Classification)
- Payment success rate is **95.7%** — the `Failed` class is heavily underrepresented (~4.3%)
- Classification models may overfit to the majority class (Success)
- AUC and F1 scores should be interpreted carefully; accuracy alone is misleading
- Potential improvement: use oversampling (SMOTE), undersampling, or class weights

### Temporal Coverage
- Data spans 2016–2022 but distribution is skewed toward recent years
- Early years (2016–2017) have significantly fewer transactions
- Seasonal patterns may not be reliable for early years

### Data Quality
- Some `product_metadata` and `event_metadata` JSON values may fail to parse
- Missing values in promo_amount (filled with 0.0 — assumes no promo)
- A few timestamp parsing failures in transaction/click stream data

## Technical Limitations

### Processing Performance
- Click stream table (~12.8M rows, 1.7 GB) is resource-intensive
- Local mode (`local[*]`) has memory constraints; driver set to 4 GB
- Full pipeline may take 15–30 minutes on a single machine
- Streaming demo uses file-based simulation, not a real message broker (e.g., Kafka)

### Java Compatibility
- PySpark 3.5.x is **incompatible with Java 17.0.12+** (`Subject.getSubject()` throws `UnsupportedOperationException`)
- Must use **Java 11** (JDK 11.0.25+ tested and verified)
- Windows requires Hadoop `winutils.exe` in `HADOOP_HOME/bin`

### ML Model Simplicity
- Feature engineering is relatively basic — no deep feature interactions exploited
- No hyperparameter tuning (e.g., cross-validation, grid search) implemented
- Clustering uses standard KMeans — more sophisticated methods (DBSCAN, Gaussian Mixture) not explored
- No temporal features (e.g., recency, time since first order) used in classification

### Streaming Constraints
- File-based stream simulation (not Kafka/Kinesis)
- Limited to batch-mode file reading with `maxFilesPerTrigger`
- Watermarking and windowing tested with historical timestamps, not live data
- Stream timeout is fixed; in production, the stream would run continuously

## Assumptions

1. **Promo amount null = no promo**: Missing promo_amount values are assumed to be 0.0
2. **Product metadata format**: Assumed consistent JSON structure `{"items": [...]}`
3. **Event names**: Click stream event names are used as-is; no normalization of casing
4. **Customer uniqueness**: `customer_id` is assumed unique across the dataset
5. **Payment status binary**: Only `Success` and `Failed` statuses are considered for ML; any other values are filtered
