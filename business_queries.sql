-- 1. Qual segmento apresenta o maior gasto médio por transação?
SELECT s.segment,
       ROUND(AVG(f.amount_spent)::numeric, 2) AS avg_spent
FROM lab01.fact_customer_spending f
JOIN lab01.dim_segment s ON f.segment_id = s.segment_id
GROUP BY s.segment
ORDER BY avg_spent DESC;

-- 2. Quais estados concentram o maior valor total gasto?
SELECT l.state_name,
       ROUND(SUM(f.amount_spent)::numeric, 2) AS total_spent
FROM lab01.fact_customer_spending f
JOIN lab01.dim_location l ON f.location_id = l.location_id
GROUP BY l.state_name
ORDER BY total_spent DESC
LIMIT 10;

-- 3. Qual método de pagamento possui maior ticket médio?
SELECT p.payment_method,
       ROUND(AVG(f.amount_spent)::numeric, 2) AS avg_ticket
FROM lab01.fact_customer_spending f
JOIN lab01.dim_payment p ON f.payment_id = p.payment_id
GROUP BY p.payment_method
ORDER BY avg_ticket DESC;

-- 4. Como evolui o gasto mensal total entre 2018 e 2025?
SELECT d.year,
       d.month,
       ROUND(SUM(f.amount_spent)::numeric, 2) AS total_spent
FROM lab01.fact_customer_spending f
JOIN lab01.dim_date d ON f.date_id = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- 5. Clientes com referral gastam mais do que clientes sem referral?
SELECT c.referral,
       ROUND(AVG(f.amount_spent)::numeric, 2) AS avg_spent,
       COUNT(*) AS total_transactions
FROM lab01.fact_customer_spending f
JOIN lab01.dim_customer_profile c
  ON f.customer_profile_id = c.customer_profile_id
GROUP BY c.referral
ORDER BY c.referral;
