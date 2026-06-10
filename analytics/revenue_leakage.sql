-- D5-Q1: Pareto summary — revenue share by customer top percentile
SELECT
    customer_segment,
    SUM(customer_count) OVER (ORDER BY min_rank) AS cumulative_customers,
    SUM(segment_revenue) OVER (ORDER BY min_rank) AS cumulative_revenue,
    cumulative_revenue_pct
FROM (
    SELECT
        multiIf(rank_pct <= 10, 'Top 10%',
                rank_pct <= 20, 'Top 20%',
                rank_pct <= 30, 'Top 30%',
                rank_pct <= 50, 'Top 50%',
                'Bottom 50%')                       AS customer_segment,
        COUNT(*)                                    AS customer_count,
        ROUND(SUM(total_payment_value), 2)          AS segment_revenue,
        ROUND(MAX(cumulative_revenue_pct), 2)       AS cumulative_revenue_pct,
        MIN(rank_pct)                               AS min_rank
    FROM mart.revenue_concentration
    GROUP BY customer_segment
    ORDER BY min_rank
)

-- D5-Q2: Lorenz curve data (rank_pct vs cumulative_revenue_pct)
SELECT
    ROUND(rank_pct, 0) AS rank_pct,
    MAX(cumulative_revenue_pct) AS cumulative_revenue_pct,
    ROUND(rank_pct, 0) AS equality_line
FROM mart.revenue_concentration
GROUP BY ROUND(rank_pct, 0)
ORDER BY rank_pct;

-- D5-Q3: Top 50 customers by revenue
SELECT
    revenue_rank,
    customer_unique_id,
    customer_state,
    ROUND(total_payment_value, 2)   AS total_spend,
    total_orders,
    preferred_payment_type,
    ROUND(rank_pct, 2)              AS rank_pct,
    ROUND(cumulative_revenue_pct, 2) AS cumulative_revenue_pct
FROM mart.revenue_concentration
ORDER BY revenue_rank ASC
LIMIT 50;

-- D5-Q4: Revenue concentration by state among top customers
SELECT
    customer_state,
    COUNT(*)                            AS customer_count,
    ROUND(SUM(total_payment_value), 2)  AS total_revenue,
    ROUND(AVG(rank_pct), 2)             AS avg_rank_pct,
    ROUND(MIN(rank_pct), 2)             AS best_rank_pct
FROM mart.revenue_concentration
WHERE rank_pct <= 20
GROUP BY customer_state
ORDER BY total_revenue DESC;

-- D5-Q5: Preferred payment type among top 20% customers
SELECT
    preferred_payment_type,
    COUNT(*)                                            AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct,
    ROUND(SUM(total_payment_value), 2)                 AS total_revenue
FROM mart.revenue_concentration
WHERE rank_pct <= 20
GROUP BY preferred_payment_type
ORDER BY customer_count DESC;

-- D5-Q6: Gini coefficient (single number — inequality index)
SELECT ROUND(
    1 - 2 * SUM((anyLast(total_customers) - revenue_rank + 0.5) * total_payment_value)
        / (anyLast(total_customers) * SUM(total_payment_value)),
2) AS gini_coefficient
FROM mart.revenue_concentration;

-- D5-Q7: Revenue leakage from cancelled orders (RQ6.1, 6.3)
SELECT
    order_status,
    payment_type,
    transaction_count,
    ROUND(total_payment_value, 2)   AS lost_or_at_risk_revenue,
    ROUND(avg_payment_value, 2)     AS avg_order_value,
    installment_count,
    ROUND(pct_installment, 2)       AS pct_installment
FROM mart.revenue_leakage
ORDER BY order_status, lost_or_at_risk_revenue DESC;

-- D5-Q8: Total estimated revenue leakage KPI
SELECT
    SUM(total_payment_value)                            AS total_lost_revenue,
    SUM(transaction_count)                              AS cancelled_transactions,
    ROUND(AVG(avg_payment_value), 2)                    AS avg_cancelled_order_value,
    ROUND(SUM(total_payment_value) * 100.0 / (
        SELECT SUM(total_payment_value) FROM mart.revenue_leakage
    ), 2)                                               AS pct_of_all_revenue
FROM mart.revenue_leakage
WHERE order_status IN ('canceled', 'unavailable');