SELECT s.segment,
       ROUND(AVG(f.amount_spent), 2) AS avg_spent
FROM lab01.fact_customer_spending f
JOIN lab01.dim_segment s ON f.segment_id = s.segment_id
GROUP BY s.segment
ORDER BY avg_spent DESC;