-- Revenue and HVC count by state (choropleth source)
SELECT
    customer_state,
    SUM(total_transactions)             AS total_transactions,
    ROUND(SUM(total_payment_value), 2)  AS total_revenue,
    ROUND(AVG(avg_payment_value), 2)    AS avg_payment_value,
    SUM(high_value_customer_count)      AS total_hvc_count,
    ROUND(AVG(pct_high_value), 2)       AS avg_pct_high_value,
    ROUND(AVG(avg_lat), 4)              AS avg_lat,
    ROUND(AVG(avg_lng), 4)              AS avg_lng
FROM mart.geo_payment
GROUP BY customer_state
ORDER BY total_revenue DESC;

-- Payment method preference by state (top method per state)
SELECT
    customer_state,
    payment_type,
    total_transactions,
    ROUND(total_payment_value, 2)   AS total_revenue,
    ROUND(avg_payment_value, 2)     AS avg_payment_value
FROM mart.geo_payment
WHERE (customer_state, total_transactions) IN (
    SELECT customer_state, MAX(total_transactions)
    FROM mart.geo_payment
    GROUP BY customer_state
)
ORDER BY total_revenue DESC;

-- Payment type breakdown per state (full matrix)
SELECT
    customer_state,
    payment_type,
    total_transactions,
    ROUND(total_payment_value, 2)   AS total_revenue,
    ROUND(avg_payment_value, 2)     AS avg_payment_value,
    high_value_customer_count,
    ROUND(pct_high_value, 2)        AS pct_high_value
FROM mart.geo_payment
ORDER BY customer_state, total_transactions DESC;

-- High volume but low AOV states (RQ5.4 — underoptimised regions)
SELECT
    customer_state,
    SUM(total_transactions)             AS total_transactions,
    ROUND(SUM(total_payment_value), 2)  AS total_revenue,
    ROUND(AVG(avg_payment_value), 2)    AS avg_payment_value,
    SUM(high_value_customer_count)      AS hvc_count,
    ROUND(AVG(avg_lat), 4)              AS lat,
    ROUND(AVG(avg_lng), 4)              AS lng
FROM mart.geo_payment
GROUP BY customer_state
HAVING avg_payment_value < (
    SELECT AVG(avg_payment_value) FROM mart.geo_payment
)
ORDER BY total_transactions DESC;

-- Credit card penetration by state (RQ5.3)
SELECT
    customer_state,
    SUM(CASE WHEN payment_type = 'credit_card' THEN total_transactions ELSE 0 END) AS cc_transactions,
    SUM(total_transactions) AS all_transactions,
    ROUND(
        SUM(CASE WHEN payment_type = 'credit_card' THEN total_transactions ELSE 0 END)
        * 100.0 / SUM(total_transactions), 2
    ) AS cc_penetration_pct,
    ROUND(
        SUM(CASE WHEN payment_type = 'credit_card' THEN total_payment_value ELSE 0 END), 2
    ) AS cc_revenue
FROM mart.geo_payment
GROUP BY customer_state
ORDER BY cc_penetration_pct DESC;