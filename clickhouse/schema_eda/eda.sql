-----------------
--  ROW COUNTS --
-----------------
SELECT 'orders'                    AS tbl, count() AS row_count FROM raw.orders
UNION ALL SELECT 'order_items',              count() FROM raw.order_items
UNION ALL SELECT 'order_payments',           count() FROM raw.order_payments
UNION ALL SELECT 'order_reviews',            count() FROM raw.order_reviews
UNION ALL SELECT 'customers',               count() FROM raw.customers
UNION ALL SELECT 'sellers',                 count() FROM raw.sellers
UNION ALL SELECT 'products',                count() FROM raw.products
UNION ALL SELECT 'geolocation',             count() FROM raw.geolocation
UNION ALL SELECT 'product_category_translation', count() FROM raw.product_category_translation;

------------------------
-- SCHEMA DEFINITIONS --
------------------------
DESCRIBE raw.orders;

DESCRIBE raw.order_reviews;

DESCRIBE raw.order_payments;

DESCRIBE raw.order_items;

DESCRIBE raw.customers;

DESCRIBE raw.sellers;

DESCRIBE raw.products;

-------------------
-- NULL PROFILES --
-------------------
-- orders
SELECT
    countIf(order_id IS NULL)                     AS null_order_id,
    countIf(customer_id IS NULL)                  AS null_customer_id,
    countIf(order_status IS NULL)                 AS null_status,
    countIf(order_purchase_timestamp IS NULL)     AS null_purchase_ts,
    countIf(order_delivered_customer_date IS NULL) AS null_delivered,
    countIf(order_estimated_delivery_date IS NULL) AS null_estimated,
    count()                                        AS total
FROM raw.orders;

-- order_payments
SELECT
    countIf(order_id IS NULL)              AS null_order_id,
    countIf(payment_type IS NULL)          AS null_type,
    countIf(payment_value IS NULL)         AS null_value,
    countIf(payment_installments IS NULL)  AS null_installments,
    count()                                AS total
FROM raw.order_payments;

-- order_reviews
SELECT
    countIf(order_id IS NULL)               AS null_order_id,
    countIf(review_score IS NULL)           AS null_score,
    countIf(review_comment_message IS NULL) AS null_comment,
    count()                                 AS total
FROM raw.order_reviews;

-- order_items
SELECT
    countIf(order_id IS NULL)    AS null_order_id,
    countIf(product_id IS NULL)  AS null_product,
    countIf(seller_id IS NULL)   AS null_seller,
    countIf(price IS NULL)       AS null_price,
    countIf(freight_value IS NULL) AS null_freight,
    count()                      AS total
FROM raw.order_items;

----------------------
-- DUPLICATE CHECKS --
----------------------
SELECT
    'orders' AS tbl,
    count()                        AS total,
    count(DISTINCT order_id)       AS distinct_order_ids,
    count() - count(DISTINCT order_id) AS duplicates
FROM raw.orders

UNION ALL SELECT
    'reviews',
    count(),
    count(DISTINCT order_id),
    count() - count(DISTINCT order_id)
FROM raw.order_reviews;


-------------------------
-- VALUE DISTRIBUTIONS --
-------------------------
-- review score distribution
SELECT review_score, count()
FROM raw.order_reviews
GROUP BY review_score
ORDER BY review_score;

-- order status breakdown
SELECT order_status, count()
FROM raw.orders
GROUP BY order_status
ORDER BY 2 DESC;

-- payment type breakdown
SELECT payment_type, count()
FROM raw.order_payments
GROUP BY payment_type
ORDER BY 2 DESC;


-----------------------
-- DATE RANGE SANITY --
-----------------------
SELECT
    min(order_purchase_timestamp) AS earliest,
    max(order_purchase_timestamp) AS latest
FROM raw.orders;


---------------------------
-- REFERENTIAL INTEGRITY --
---------------------------
-- orders with no corresponding payments
SELECT count()
FROM raw.orders o
LEFT JOIN raw.order_payments p ON o.order_id = p.order_id
WHERE p.order_id IS NULL;

-- orders with no corresponding items
SELECT count()
FROM raw.orders o
LEFT JOIN raw.order_items i ON o.order_id = i.order_id
WHERE i.order_id IS NULL;


------------------------------------------------------------
-- UNEXPECTED VALUES IN review_score (post multiLine fix) --
------------------------------------------------------------
SELECT review_score, count()
FROM raw.order_reviews
WHERE review_score NOT IN ('1', '2', '3', '4', '5')
  AND review_score IS NOT NULL
GROUP BY review_score
ORDER BY count() DESC
LIMIT 20;