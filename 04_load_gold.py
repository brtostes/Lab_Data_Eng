from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text

SILVER_FILE = Path("data/silver/customer_spending_silver.parquet")

DB_USER = "postgres"
DB_PASS = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "postgres"

ENGINE = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

def add_age_group(df):
    bins = [15, 25, 35, 45, 55, 65, 79]
    labels = ["15-24", "25-34", "35-44", "45-54", "55-64", "65-78"]
    df = df.copy()
    df["age_group"] = pd.cut(df["age"], bins=bins, labels=labels, right=False)
    return df

def insert_only_new(conn, df_new, table_name, key_columns, schema="lab01"):
    existing = pd.read_sql(f"SELECT * FROM {schema}.{table_name}", conn)

    if existing.empty:
        df_to_insert = df_new.copy()
    else:
        merged = df_new.merge(
            existing[key_columns].drop_duplicates(),
            on=key_columns,
            how="left",
            indicator=True
        )
        df_to_insert = merged[merged["_merge"] == "left_only"][df_new.columns]

    if not df_to_insert.empty:
        df_to_insert.to_sql(table_name, conn, schema=schema, if_exists="append", index=False)

def main():
    df = pd.read_parquet(SILVER_FILE)
    df = add_age_group(df)

    with ENGINE.begin() as conn:
        # Dimensão data
        dim_date = df[["transaction_date", "year", "month", "day", "quarter"]].drop_duplicates().copy()
        dim_date.columns = ["full_date", "year", "month", "day", "quarter"]
        insert_only_new(conn, dim_date, "dim_date", ["full_date"])

        # Dimensão perfil
        dim_profile = df[[
            "gender", "age", "age_group", "marital_status", "employee_status", "referral"
        ]].drop_duplicates().copy()
        insert_only_new(
            conn,
            dim_profile,
            "dim_customer_profile",
            ["gender", "age", "age_group", "marital_status", "employee_status", "referral"]
        )

        # Dimensão localização
        dim_location = df[["state_name"]].drop_duplicates().copy()
        insert_only_new(conn, dim_location, "dim_location", ["state_name"])

        # Dimensão pagamento
        dim_payment = df[["payment_method"]].drop_duplicates().copy()
        insert_only_new(conn, dim_payment, "dim_payment", ["payment_method"])

        # Dimensão segmento
        dim_segment = df[["segment"]].drop_duplicates().copy()
        insert_only_new(conn, dim_segment, "dim_segment", ["segment"])

        # Releitura das dimensões para mapear chaves
        d_date = pd.read_sql("SELECT * FROM lab01.dim_date", conn)
        d_profile = pd.read_sql("SELECT * FROM lab01.dim_customer_profile", conn)
        d_location = pd.read_sql("SELECT * FROM lab01.dim_location", conn)
        d_payment = pd.read_sql("SELECT * FROM lab01.dim_payment", conn)
        d_segment = pd.read_sql("SELECT * FROM lab01.dim_segment", conn)

        fact = df.copy()

        fact = fact.merge(
            d_date,
            left_on=["transaction_date", "year", "month", "day", "quarter"],
            right_on=["full_date", "year", "month", "day", "quarter"],
            how="left"
        )

        fact = fact.merge(
            d_profile,
            on=["gender", "age", "age_group", "marital_status", "employee_status", "referral"],
            how="left"
        )

        fact = fact.merge(d_location, on="state_name", how="left")
        fact = fact.merge(d_payment, on="payment_method", how="left")
        fact = fact.merge(d_segment, on="segment", how="left")

        fact_final = fact[[
            "transaction_id", "date_id", "customer_profile_id",
            "location_id", "payment_id", "segment_id", "amount_spent"
        ]].drop_duplicates()

        # Evitar duplicidade na fato, se transaction_id for único
        existing_fact = pd.read_sql("SELECT transaction_id FROM lab01.fact_customer_spending", conn)
        fact_final = fact_final[~fact_final["transaction_id"].isin(existing_fact["transaction_id"])]

        if not fact_final.empty:
            fact_final.to_sql("fact_customer_spending", conn, schema="lab01", if_exists="append", index=False)

    print("Carga GOLD concluída.")

if __name__ == "__main__":
    main()