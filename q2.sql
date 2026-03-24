SELECT l.state_name,
       ROUND(SUM(f.amount_spent), 2) AS total_spent
FROM lab01.fact_customer_spending f
JOIN lab01.dim_location l ON f.location_id = l.location_id
GROUP BY l.state_name
ORDER BY total_spent DESC
LIMIT 10;