-- Transaction by payment type
SELECT
    payment_type,
    SUM(total_transactions) AS total_transactions
FROM mart.payment_summary
GROUP BY payment_type
ORDER BY total_transactions DESC;

-- Total revenue by payment type
SELECT
    payment_type,
    SUM(total_payment_value) AS total_revenue
FROM mart.payment_summary
GROUP BY payment_type
ORDER BY total_revenue DESC;

-- Payment method over time
SELECT
    concat(toString(year), '-', lpad(toString(month), 2, '0')) AS period,
    payment_type,
    SUM(total_transactions) AS total_transactions
FROM mart.payment_summary
GROUP BY year, month, payment_type
ORDER BY year, month, payment_type;

-- Payment value by installment buckets
SELECT
    installment_bucket,
    ROUND(
        SUM(total_payment_value) / NULLIF(SUM(total_transactions), 0),
        2
    ) AS avg_payment_value
FROM mart.payment_summary
WHERE payment_type = 'credit_card'
GROUP BY installment_bucket
ORDER BY
    CASE installment_bucket
        WHEN '1' THEN 1
        WHEN '2-3' THEN 2
        WHEN '4-6' THEN 3
        WHEN '7-12' THEN 4
        WHEN '12+' THEN 5
        ELSE 99
    END;

-- Revenue by installment buckets
SELECT
    installment_bucket,
    SUM(total_payment_value) AS total_revenue
FROM mart.payment_summary
WHERE payment_type = 'credit_card'
GROUP BY installment_bucket
ORDER BY
    CASE installment_bucket
        WHEN '1' THEN 1
        WHEN '2-3' THEN 2
        WHEN '4-6' THEN 3
        WHEN '7-12' THEN 4
        WHEN '12+' THEN 5
        ELSE 99
    END;