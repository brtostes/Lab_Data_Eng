SELECT p.payment_method,
       ROUND(AVG(f.amount_spent), 2) AS avg_ticket
FROM lab01.fact_customer_spending f
JOIN lab01.dim_payment p ON f.payment_id = p.payment_id
GROUP BY p.payment_method
ORDER BY avg_ticket DESC;