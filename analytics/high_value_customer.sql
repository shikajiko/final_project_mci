-- HVC overview KPIs (single-row summary tile)
SELECT
    COUNT(*)                                    AS total_hvc_count,
    ROUND(SUM(total_payment_value), 2)          AS total_hvc_revenue,
    ROUND(AVG(total_payment_value), 2)          AS avg_hvc_spend,
    ROUND(AVG(total_orders), 2)                 AS avg_orders_per_hvc,
    ROUND(AVG(clv_estimate), 2)                 AS avg_clv,
    ROUND(AVG(recency_days), 0)                 AS avg_recency_days
FROM mart.high_value_customers;

-- HVC count and revenue by state
SELECT
    customer_state,
    COUNT(*)                           AS hvc_count,
    ROUND(SUM(total_payment_value), 2) AS total_revenue,
    ROUND(AVG(total_payment_value), 2) AS avg_spend,
    ROUND(AVG(clv_estimate), 2)        AS avg_clv
FROM mart.high_value_customers
GROUP BY customer_state
ORDER BY total_revenue DESC;

-- Preferred payment type distribution among HVCs
SELECT
    preferred_payment_type,
    COUNT(*)                                        AS hvc_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_hvcs,
    ROUND(AVG(total_payment_value), 2)              AS avg_spend,
    ROUND(AVG(avg_installments), 2)                 AS avg_installments
FROM mart.high_value_customers
GROUP BY preferred_payment_type
ORDER BY hvc_count DESC;

-- HVC buyer type segmentation (frequency vs basket size 2x2)
SELECT
    customer_unique_id,
    customer_state,
    total_orders,
    total_payment_value,
    avg_payment_value,
    clv_estimate,
    CASE
        WHEN total_orders >= 2 AND avg_payment_value >= (SELECT AVG(avg_payment_value) FROM mart.high_value_customers)
            THEN 'High Frequency + High Basket'
        WHEN total_orders >= 2 AND avg_payment_value < (SELECT AVG(avg_payment_value) FROM mart.high_value_customers)
            THEN 'High Frequency + Low Basket'
        WHEN total_orders < 2 AND avg_payment_value >= (SELECT AVG(avg_payment_value) FROM mart.high_value_customers)
            THEN 'Low Frequency + High Basket'
        ELSE 'Low Frequency + Low Basket'
    END AS buyer_type
FROM mart.high_value_customers
ORDER BY total_payment_value DESC;

-- HVC recency segmentation (RQ2.3)
SELECT
    multiIf(
        recency_days <= 30,  'Active (≤30d)',
        recency_days <= 90,  'Recent (31-90d)',
        recency_days <= 180, 'Lapsing (91-180d)',
        'Churned (180d+)'
    )                                   AS recency_segment,
    COUNT(*)                            AS customer_count,
    ROUND(AVG(total_payment_value), 2)  AS avg_spend,
    ROUND(AVG(clv_estimate), 2)         AS avg_clv,
    ROUND(AVG(total_orders), 1)         AS avg_orders
FROM mart.high_value_customers
GROUP BY recency_segment
ORDER BY MIN(recency_days);

-- Top 20 HVCs by CLV
SELECT
    customer_unique_id,
    customer_state,
    total_orders,
    ROUND(total_payment_value, 2)   AS total_spend,
    ROUND(avg_payment_value, 2)     AS avg_order_value,
    preferred_payment_type,
    recency_days,
    ROUND(clv_estimate, 2)          AS clv_estimate
FROM mart.high_value_customers
ORDER BY clv_estimate DESC
LIMIT 20;