from __future__ import annotations

import pandas as pd
from sqlalchemy import text

from utils import SILVER_PARQUET_PATH, add_age_group, get_engine


def insert_dimensions(df: pd.DataFrame, engine) -> None:
    dim_date = (
        df[['transaction_date', 'year', 'month', 'day', 'quarter']]
        .drop_duplicates()
        .rename(columns={'transaction_date': 'full_date'})
    )

    dim_customer = (
        add_age_group(df)[['gender', 'age', 'age_group', 'marital_status', 'employee_status', 'referral']]
        .drop_duplicates()
    )

    dim_location = df[['state_name']].drop_duplicates()
    dim_payment = df[['payment_method']].drop_duplicates()
    dim_segment = df[['segment']].drop_duplicates()

    with engine.begin() as conn:
        dim_date.to_sql('dim_date', conn, schema='lab01', if_exists='append', index=False, method='multi', chunksize=5000)
        dim_customer.to_sql('dim_customer_profile', conn, schema='lab01', if_exists='append', index=False, method='multi', chunksize=5000)
        dim_location.to_sql('dim_location', conn, schema='lab01', if_exists='append', index=False, method='multi', chunksize=5000)
        dim_payment.to_sql('dim_payment', conn, schema='lab01', if_exists='append', index=False, method='multi', chunksize=5000)
        dim_segment.to_sql('dim_segment', conn, schema='lab01', if_exists='append', index=False, method='multi', chunksize=5000)


def load_fact(df: pd.DataFrame, engine) -> None:
    df = add_age_group(df)

    with engine.begin() as conn:
        d_date = pd.read_sql('SELECT * FROM lab01.dim_date', conn)
        d_customer = pd.read_sql('SELECT * FROM lab01.dim_customer_profile', conn)
        d_location = pd.read_sql('SELECT * FROM lab01.dim_location', conn)
        d_payment = pd.read_sql('SELECT * FROM lab01.dim_payment', conn)
        d_segment = pd.read_sql('SELECT * FROM lab01.dim_segment', conn)

        fact = (
            df.merge(
                d_date,
                left_on=['transaction_date', 'year', 'month', 'day', 'quarter'],
                right_on=['full_date', 'year', 'month', 'day', 'quarter'],
                how='left',
            )
            .merge(
                d_customer,
                on=['gender', 'age', 'age_group', 'marital_status', 'employee_status', 'referral'],
                how='left',
            )
            .merge(d_location, on='state_name', how='left')
            .merge(d_payment, on='payment_method', how='left')
            .merge(d_segment, on='segment', how='left')
        )

        fact_final = fact[[
            'transaction_id', 'date_id', 'customer_profile_id', 'location_id',
            'payment_id', 'segment_id', 'amount_spent'
        ]].drop_duplicates()

        fact_final.to_sql(
            'fact_customer_spending',
            conn,
            schema='lab01',
            if_exists='append',
            index=False,
            method='multi',
            chunksize=5000,
        )


def truncate_tables(engine) -> None:
    stmts = [
        'TRUNCATE TABLE lab01.fact_customer_spending RESTART IDENTITY CASCADE;',
        'TRUNCATE TABLE lab01.dim_date RESTART IDENTITY CASCADE;',
        'TRUNCATE TABLE lab01.dim_customer_profile RESTART IDENTITY CASCADE;',
        'TRUNCATE TABLE lab01.dim_location RESTART IDENTITY CASCADE;',
        'TRUNCATE TABLE lab01.dim_payment RESTART IDENTITY CASCADE;',
        'TRUNCATE TABLE lab01.dim_segment RESTART IDENTITY CASCADE;',
    ]
    with engine.begin() as conn:
        for stmt in stmts:
            conn.execute(text(stmt))


def main() -> None:
    engine = get_engine()
    df = pd.read_parquet(SILVER_PARQUET_PATH)

    truncate_tables(engine)
    insert_dimensions(df, engine)
    load_fact(df, engine)

    print('Carga GOLD concluída com sucesso.')


if __name__ == '__main__':
    main()
