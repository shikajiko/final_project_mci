-- Installment bucket overview
SELECT
    installment_bucket,
    total_transactions,
    ROUND(avg_payment_value, 2)     AS avg_payment_value,
    ROUND(avg_order_value, 2)       AS avg_order_value,
    ROUND(avg_installments, 2)      AS avg_installments,
    ROUND(total_revenue, 2)         AS total_revenue,
    ROUND(pct_of_total_revenue, 2)  AS pct_of_total_revenue
FROM mart.installment_impact
ORDER BY avg_payment_value DESC;

-- AOV lift from installments vs single payment
SELECT
    installment_bucket,
    ROUND(avg_payment_value, 2) AS avg_payment_value,
    ROUND(
        avg_payment_value - MAX(CASE WHEN installment_bucket = '1' THEN avg_payment_value END) OVER (),
        2
    ) AS lift_vs_single_payment,
    ROUND(
        (avg_payment_value - MAX(CASE WHEN installment_bucket = '1' THEN avg_payment_value END) OVER ())
        / MAX(CASE WHEN installment_bucket = '1' THEN avg_payment_value END) OVER () * 100,
        2
    ) AS lift_pct
FROM mart.installment_impact
ORDER BY avg_payment_value DESC;

-- Payment method distribution statistics (RQ3.2)
SELECT
    payment_type,
    transaction_count,
    ROUND(mean_payment_value, 2)    AS mean_aov,
    ROUND(stddev_payment_value, 2)  AS stddev,
    ROUND(p25, 2)                   AS p25,
    ROUND(p50_median, 2)            AS median,
    ROUND(p75, 2)                   AS p75,
    ROUND(p75 - p25, 2)             AS iqr,
    ROUND(p90, 2)                   AS p90
FROM mart.payment_distribution
ORDER BY mean_payment_value DESC;

-- Revenue uplift simulation (RQ4.4)
-- Simulates uplift if 10%/25% of single-payment credit card users moved to 4-6x installments
WITH single_pay AS (
    SELECT avg_payment_value AS single_aov, total_transactions AS single_count
    FROM mart.installment_impact WHERE installment_bucket = '1'
),
installment_46 AS (
    SELECT avg_payment_value AS install_aov
    FROM mart.installment_impact WHERE installment_bucket = '4-6'
)
SELECT
    ROUND(s.single_aov, 2)                                          AS current_single_aov,
    ROUND(i.install_aov, 2)                                         AS target_installment_aov,
    ROUND(i.install_aov - s.single_aov, 2)                         AS aov_lift_per_customer,
    ROUND(s.single_count * 0.10 * (i.install_aov - s.single_aov), 2) AS uplift_10pct_conversion,
    ROUND(s.single_count * 0.25 * (i.install_aov - s.single_aov), 2) AS uplift_25pct_conversion
FROM single_pay s, installment_46 i;