CREATE DATABASE IF NOT EXISTS ods;

CREATE TABLE IF NOT EXISTS ods.orders (
    order_id                        String,
    customer_id                     String,
    order_status                    String,
    order_purchase_timestamp        Nullable(DateTime),
    order_approved_at               Nullable(DateTime),
    order_delivered_carrier_date    Nullable(DateTime),
    order_delivered_customer_date   Nullable(DateTime),
    order_estimated_delivery_date   Nullable(DateTime),
    days_to_deliver_actual          Nullable(Float64),
    days_to_deliver_estimated       Nullable(Float64),
    delivery_delay_days             Nullable(Float64),
    is_delivered                    Int32,
    is_late                         Int32
)
ENGINE = MergeTree()
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS ods.order_payments (
    order_id                String,
    payment_sequential      Int32,
    payment_type            String,
    payment_installments    Int32,
    payment_value           Float64,
    is_installment          Int32,
    installment_bucket      String,
    is_high_value_payment   Int32
)
ENGINE = MergeTree()
ORDER BY (order_id, payment_sequential);

CREATE TABLE IF NOT EXISTS ods.order_items (
    order_id                String,
    order_item_id           Int32,
    product_id              String,
    seller_id               String,
    shipping_limit_date     Nullable(DateTime),
    price                   Float64,
    freight_value           Float64,
    line_total              Float64,
    freight_ratio           Nullable(Float64)
)
ENGINE = MergeTree()
ORDER BY (order_id, order_item_id);

CREATE TABLE IF NOT EXISTS ods.customers (
    customer_id             String,
    customer_unique_id      String,
    customer_zip_code_prefix String,
    customer_city           String,
    customer_state          String,
    lat                     Nullable(Float64),
    lng                     Nullable(Float64)
)
ENGINE = MergeTree()
ORDER BY customer_id;