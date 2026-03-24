SELECT d.year,
       d.month,
       ROUND(SUM(f.amount_spent), 2) AS total_spent
FROM lab01.fact_customer_spending f
JOIN lab01.dim_date d ON f.date_id = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;