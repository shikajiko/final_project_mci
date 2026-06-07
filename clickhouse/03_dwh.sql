CREATE DATABASE IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.dim_customer (
    customer_id             String,
    customer_unique_id      String,
    customer_city           String,
    customer_state          String,
    customer_zip_code_prefix String,
    lat                     Nullable(Float64),
    lng                     Nullable(Float64)
)
ENGINE = MergeTree()
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_id     String,   
    full_date   Date,
    year        Int32,
    quarter     Int32,
    month       Int32,
    month_name  String,
    day         Int32,
    day_of_week Int32,    
    day_name    String
)
ENGINE = MergeTree()
ORDER BY date_id;

CREATE TABLE IF NOT EXISTS dwh.dim_payment_method (
    payment_method_id   Int32,
    payment_type        String,
    is_installment_type UInt8   
)
ENGINE = MergeTree()
ORDER BY payment_method_id;

CREATE TABLE IF NOT EXISTS dwh.fact_orders (
    order_id                    String,
    customer_id                 String,
    date_id                     String,   
    order_status                String,
    total_order_value           Float64,  
    total_freight               Float64,  
    total_order_revenue         Float64,  
    item_count                  Int32,
    days_to_deliver_actual      Nullable(Float64),
    days_to_deliver_estimated   Nullable(Float64),
    delivery_delay_days         Nullable(Float64),
    is_delivered                Int32,
    is_late                     Int32
)
ENGINE = MergeTree()
ORDER BY (order_id, customer_id);

CREATE TABLE IF NOT EXISTS dwh.fact_payments (
    order_id                String,
    customer_id             String,
    date_id                 String,   
    payment_type            String,
    payment_sequential      Int32,
    payment_installments    Int32,
    payment_value           Float64,
    is_installment          Int32,
    installment_bucket      String,
    is_high_value_payment   Int32,
    total_order_value       Float64,  
    customer_state          String,   
    lat                     Nullable(Float64),
    lng                     Nullable(Float64)
)
ENGINE = MergeTree()
ORDER BY (order_id, payment_sequential);