SELECT c.referral,
       ROUND(AVG(f.amount_spent), 2) AS avg_spent,
       COUNT(*) AS total_transactions
FROM lab01.fact_customer_spending f
JOIN lab01.dim_customer_profile c
  ON f.customer_profile_id = c.customer_profile_id
GROUP BY c.referral
ORDER BY c.referral;
