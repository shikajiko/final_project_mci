CREATE DATABASE IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.payment_summary (
    payment_type            String,
    installment_bucket      String,
    year                    Int32,
    month                   Int32,
    month_name              String,
    total_transactions      Int64,
    total_payment_value     Float64,
    avg_payment_value       Float64,
    max_payment_value       Float64,
    total_installment_users Int64,
    pct_installment         Float64
) ENGINE = MergeTree()
ORDER BY (payment_type, installment_bucket, year, month);

CREATE TABLE IF NOT EXISTS mart.high_value_customers (
    customer_unique_id      String,
    customer_state          String,
    total_orders            Int64,
    total_payment_value     Float64,
    avg_payment_value       Float64,
    preferred_payment_type  String,
    pct_installment_usage   Float64,
    avg_installments        Float64,
    last_order_date         String,
    first_order_date        String,
    recency_days            Int32,
    clv_estimate            Float64
) ENGINE = MergeTree()
ORDER BY customer_unique_id;

CREATE TABLE IF NOT EXISTS mart.geo_payment (
    customer_state              String,
    payment_type                String,
    total_transactions          Int64,
    total_payment_value         Float64,
    avg_payment_value           Float64,
    high_value_customer_count   Int64,
    pct_high_value              Float64,
    avg_lat                     Nullable(Float64),
    avg_lng                     Nullable(Float64)
) ENGINE = MergeTree()
ORDER BY (customer_state, payment_type);

CREATE TABLE IF NOT EXISTS mart.installment_impact (
    installment_bucket      String,
    total_transactions      Int64,
    avg_payment_value       Float64,
    avg_order_value         Float64,
    avg_installments        Float64,
    total_revenue           Float64,
    pct_of_total_revenue    Float64
) ENGINE = MergeTree()
ORDER BY installment_bucket;

CREATE TABLE IF NOT EXISTS mart.revenue_concentration (
    customer_unique_id      String,
    customer_state          String,
    total_payment_value     Float64,
    revenue_rank            Int64,
    total_customers         Int64,
    rank_pct                Float64,
    cumulative_revenue      Float64,
    total_revenue           Float64,
    cumulative_revenue_pct  Float64,
    preferred_payment_type  String,
    total_orders            Int64
) ENGINE = MergeTree()
ORDER BY revenue_rank;

CREATE TABLE IF NOT EXISTS mart.payment_distribution (
    payment_type            String,
    transaction_count       Int64,
    mean_payment_value      Float64,
    stddev_payment_value    Float64,
    p10                     Float64,
    p25                     Float64,
    p50_median              Float64,
    p75                     Float64,
    p90                     Float64,
    max_payment_value       Float64,
    min_payment_value       Float64
) ENGINE = MergeTree()
ORDER BY payment_type;

CREATE TABLE IF NOT EXISTS mart.revenue_leakage (
    order_status            String,
    payment_type            String,
    transaction_count       Int64,
    total_payment_value     Float64,
    avg_payment_value       Float64,
    installment_count       Int64,
    pct_installment         Float64
) ENGINE = MergeTree()
ORDER BY (order_status, payment_type);