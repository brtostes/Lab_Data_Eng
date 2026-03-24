CREATE SCHEMA IF NOT EXISTS gold;

DROP TABLE IF EXISTS gold.fact_customer_spending CASCADE;
DROP TABLE IF EXISTS gold.dim_date CASCADE;
DROP TABLE IF EXISTS gold.dim_customer_profile CASCADE;
DROP TABLE IF EXISTS gold.dim_location CASCADE;
DROP TABLE IF EXISTS gold.dim_payment_method CASCADE;

CREATE TABLE gold.dim_date (
    date_sk            INTEGER PRIMARY KEY,
    full_date          DATE NOT NULL UNIQUE,
    year               INTEGER NOT NULL,
    quarter            INTEGER NOT NULL,
    month              INTEGER NOT NULL,
    day                INTEGER NOT NULL,
    day_of_week        VARCHAR(20) NOT NULL
);

CREATE TABLE gold.dim_customer_profile (
    customer_profile_sk  BIGSERIAL PRIMARY KEY,
    gender               VARCHAR(50) NOT NULL,
    age                  INTEGER NOT NULL,
    age_band             VARCHAR(20) NOT NULL,
    marital_status       VARCHAR(50) NOT NULL,
    segment              VARCHAR(50) NOT NULL,
    employment_status    VARCHAR(100) NOT NULL,
    CONSTRAINT uq_customer_profile UNIQUE (
        gender, age, age_band, marital_status, segment, employment_status
    )
);

CREATE TABLE gold.dim_location (
    location_sk         BIGSERIAL PRIMARY KEY,
    state_name          VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE gold.dim_payment_method (
    payment_method_sk   BIGSERIAL PRIMARY KEY,
    payment_method      VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE gold.fact_customer_spending (
    transaction_sk        BIGSERIAL PRIMARY KEY,
    transaction_id        BIGINT NOT NULL UNIQUE,
    date_sk               INTEGER NOT NULL,
    customer_profile_sk   BIGINT NOT NULL,
    location_sk           BIGINT NOT NULL,
    payment_method_sk     BIGINT NOT NULL,
    amount_spent          NUMERIC(14,2) NOT NULL,
    referral_flag         SMALLINT,
    CONSTRAINT fk_fact_date
        FOREIGN KEY (date_sk) REFERENCES gold.dim_date(date_sk),
    CONSTRAINT fk_fact_customer_profile
        FOREIGN KEY (customer_profile_sk) REFERENCES gold.dim_customer_profile(customer_profile_sk),
    CONSTRAINT fk_fact_location
        FOREIGN KEY (location_sk) REFERENCES gold.dim_location(location_sk),
    CONSTRAINT fk_fact_payment_method
        FOREIGN KEY (payment_method_sk) REFERENCES gold.dim_payment_method(payment_method_sk)
);

CREATE INDEX idx_fact_date_sk ON gold.fact_customer_spending(date_sk);
CREATE INDEX idx_fact_customer_profile_sk ON gold.fact_customer_spending(customer_profile_sk);
CREATE INDEX idx_fact_location_sk ON gold.fact_customer_spending(location_sk);
CREATE INDEX idx_fact_payment_method_sk ON gold.fact_customer_spending(payment_method_sk);
CREATE INDEX idx_fact_amount_spent ON gold.fact_customer_spending(amount_spent);