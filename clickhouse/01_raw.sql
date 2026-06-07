CREATE TABLE IF NOT EXISTS raw.orders (
    order_id String,
    customer_id Nullable(String),
    order_status Nullable(String),
    order_purchase_timestamp Nullable(String),
    order_approved_at Nullable(String),
    order_delivered_carrier_date Nullable(String),
    order_delivered_customer_date Nullable(String),
    order_estimated_delivery_date Nullable(String)
)
ENGINE = MergeTree
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS raw.order_payments (
    order_id String,
    payment_sequential Nullable(String),
    payment_type Nullable(String),
    payment_installments Nullable(String),
    payment_value Nullable(String)
)
ENGINE = MergeTree
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id String,
    customer_unique_id Nullable(String),
    customer_zip_code_prefix Nullable(String),
    customer_city Nullable(String),
    customer_state Nullable(String)
)
ENGINE = MergeTree
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS raw.geolocation (
    geolocation_zip_code_prefix String,
    geolocation_lat Nullable(String),
    geolocation_lng Nullable(String),
    geolocation_city Nullable(String),
    geolocation_state Nullable(String)
)
ENGINE = MergeTree
ORDER BY geolocation_zip_code_prefix;

CREATE TABLE IF NOT EXISTS raw.order_items (
    order_id String,
    order_item_id String,
    product_id Nullable(String),
    seller_id Nullable(String),
    shipping_limit_date Nullable(String),
    price Nullable(String),
    freight_value Nullable(String)
)
ENGINE = MergeTree
ORDER BY (order_id, order_item_id);

CREATE TABLE IF NOT EXISTS raw.order_reviews (
    review_id Nullable(String),
    order_id Nullable(String),
    review_score Nullable(String),
    review_comment_title Nullable(String),
    review_comment_message Nullable(String),
    review_creation_date Nullable(String),
    review_answer_timestamp Nullable(String)
)
ENGINE = MergeTree
ORDER BY (assumeNotNull(review_id), assumeNotNull(order_id));

CREATE TABLE IF NOT EXISTS raw.products (
    product_id String,
    product_category_name Nullable(String),
    product_name_lenght Nullable(String),
    product_description_lenght Nullable(String),
    product_photos_qty Nullable(String),
    product_weight_g Nullable(String),
    product_height_cm Nullable(String),
    product_length_cm Nullable(String),
    product_width_cm Nullable(String)
)
ENGINE = MergeTree
ORDER BY product_id;

CREATE TABLE IF NOT EXISTS raw.sellers (
    seller_id String,
    seller_zip_code_prefix Nullable(String),
    seller_city Nullable(String),
    seller_state Nullable(String)
)
ENGINE = MergeTree
ORDER BY seller_id;

CREATE TABLE IF NOT EXISTS raw.product_category_translation (
    product_category_name String,
    product_category_name_english Nullable(String)
)
ENGINE = MergeTree
ORDER BY product_category_name;