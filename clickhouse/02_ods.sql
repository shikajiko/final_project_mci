CREATE TABLE IF NOT EXISTS ods.orders (
    order_id String,
    customer_id String,
    order_status String,
    order_purchase_timestamp DateTime,
    order_approved_at Nullable(DateTime),
    order_delivered_carrier_date Nullable(DateTime),
    order_delivered_customer_date Nullable(DateTime),
    order_estimated_delivery_date Nullable(DateTime),
    actual_delivery_days Nullable(Int32),
    estimated_delivery_days Nullable(Int32),
    is_late Nullable(UInt8)
)
ENGINE = MergeTree()
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS ods.order_payments (
    order_id String,
    payment_sequential Int32,
    payment_type String,
    payment_installments Int32,
    payment_value Float64,
    total_order_payment_value Float64,
    max_installments Int32,
    distinct_payment_methods Int32
)
ENGINE = MergeTree()
ORDER BY (order_id, payment_sequential);

CREATE TABLE IF NOT EXISTS ods.order_items (
    order_id String,
    order_item_id Int32,
    product_id String,
    seller_id  String,
    shipping_limit_date Nullable(DateTime),
    price Float64,
    freight_value Float64,
    order_total_value Float64,
    order_total_freight Float64,
    order_item_count Int32
)
ENGINE = MergeTree()
ORDER BY (order_id, order_item_id);

CREATE TABLE IF NOT EXISTS ods.customers (
    customer_id String,
    customer_unique_id String,
    customer_zip_code_prefix String,
    customer_city String,
    customer_state String,
    lat Nullable(Float64),
    lng Nullable(Float64)
)
ENGINE = MergeTree()
ORDER BY customer_id;