CREATE TABLE IF NOT EXISTS raw.orders (
    order_id String,
    customer_id String,
    order_status String,
    order_purchase_timestamp String,
    order_approved_at String,
    order_delivered_carrier_date String,
    order_delivered_customer_date String,
    order_estimated_delivery_date String
)
ENGINE = MergeTree
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS raw.order_payments (
    order_id String,
    payment_sequential UInt8,
    payment_type String,
    payment_installments UInt8,
    payment_value Float64
)
ENGINE = MergeTree
ORDER BY (order_id, payment_sequential);

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id String,
    customer_unique_id String,
    customer_zip_code_prefix UInt32,
    customer_city String,
    customer_state String
)
ENGINE = MergeTree
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS raw.geolocation (
    geolocation_zip_code_prefix UInt32,
    geolocation_lat Float64,
    geolocation_lng Float64,
    geolocation_city String,
    geolocation_state String
)
ENGINE = MergeTree
ORDER BY geolocation_zip_code_prefix;
