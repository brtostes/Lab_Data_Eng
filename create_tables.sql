CREATE SCHEMA IF NOT EXISTS lab01;

CREATE TABLE IF NOT EXISTS lab01.dim_date (
    date_id SERIAL PRIMARY KEY,
    full_date TIMESTAMP UNIQUE,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    quarter INT NOT NULL
);

CREATE TABLE IF NOT EXISTS lab01.dim_customer_profile (
    customer_profile_id SERIAL PRIMARY KEY,
    gender VARCHAR(20),
    age INT,
    age_group VARCHAR(20),
    marital_status VARCHAR(20),
    employee_status VARCHAR(50),
    referral INT
);

CREATE TABLE IF NOT EXISTS lab01.dim_location (
    location_id SERIAL PRIMARY KEY,
    state_name VARCHAR(100) UNIQUE
);

CREATE TABLE IF NOT EXISTS lab01.dim_payment (
    payment_id SERIAL PRIMARY KEY,
    payment_method VARCHAR(50) UNIQUE
);

CREATE TABLE IF NOT EXISTS lab01.dim_segment (
    segment_id SERIAL PRIMARY KEY,
    segment VARCHAR(50) UNIQUE
);

CREATE TABLE IF NOT EXISTS lab01.fact_customer_spending (
    transaction_id BIGINT PRIMARY KEY,
    date_id INT REFERENCES lab01.dim_date(date_id),
    customer_profile_id INT REFERENCES lab01.dim_customer_profile(customer_profile_id),
    location_id INT REFERENCES lab01.dim_location(location_id),
    payment_id INT REFERENCES lab01.dim_payment(payment_id),
    segment_id INT REFERENCES lab01.dim_segment(segment_id),
    amount_spent NUMERIC(14,2)
);
