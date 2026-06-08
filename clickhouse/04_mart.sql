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
)
ENGINE = MergeTree()
ORDER BY (year, month, payment_type, installment_bucket);

CREATE TABLE IF NOT EXISTS mart.high_value_customers (
    customer_unique_id      String,
    customer_state          String,
    total_orders            Int64,
    total_payment_value     Float64,
    avg_payment_value       Float64,
    preferred_payment_type  String,
    pct_installment_usage   Float64,
    avg_installments        Float64
)
ENGINE = MergeTree()
ORDER BY total_payment_value;

CREATE TABLE IF NOT EXISTS mart.geo_payment (
    customer_state          String,
    payment_type            String,
    total_transactions      Int64,
    total_payment_value     Float64,
    avg_payment_value       Float64,
    high_value_customer_count Int64,
    pct_high_value          Float64,
    avg_lat                 Nullable(Float64),
    avg_lng                 Nullable(Float64)
)
ENGINE = MergeTree()
ORDER BY (customer_state, payment_type);

CREATE TABLE IF NOT EXISTS mart.installment_impact (
    installment_bucket      String,
    total_transactions      Int64,
    avg_payment_value       Float64,
    avg_order_value         Float64,
    avg_installments        Float64,
    total_revenue           Float64,
    pct_of_total_revenue    Float64
)
ENGINE = MergeTree()
ORDER BY installment_bucket;