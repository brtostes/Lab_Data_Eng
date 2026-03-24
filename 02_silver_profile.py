from pathlib import Path
import pandas as pd

RAW_FILE = Path("data/raw/customer_spending_1M_2018_2025.csv")
SILVER_DIR = Path("data/silver")
SILVER_FILE = SILVER_DIR / "customer_spending_silver.parquet"

def standardize_columns(df):
    df = df.rename(columns={
        "Transaction_ID": "transaction_id",
        "Transaction_date": "transaction_date",
        "Gender": "gender",
        "Age": "age",
        "Marital_status": "marital_status",
        "State_names": "state_name",
        "Segment": "segment",
        "Employees_status": "employee_status",
        "Payment_method": "payment_method",
        "Referral": "referral",
        "Amount_spent": "amount_spent"
    })
    return df

def main():
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(RAW_FILE)

    # Padronização de colunas
    df = standardize_columns(df)

    # Conversão de tipos
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")

    # Padronização textual
    df["segment"] = df["segment"].replace({"Missing": "unknown"})
    df["gender"] = df["gender"].fillna("unknown")
    df["employee_status"] = df["employee_status"].fillna("unknown")
    df["referral"] = df["referral"].fillna(0)

    # Tratamento de idade
    median_age = df["age"].median()
    df["age"] = df["age"].fillna(median_age)

    # Remoção de linhas sem valor de gasto
    df = df.dropna(subset=["amount_spent"])

    # Remoção de duplicatas
    df = df.drop_duplicates()

    # Tipagem final
    df["transaction_id"] = df["transaction_id"].astype("int64")
    df["age"] = df["age"].round().astype("int64")
    df["referral"] = df["referral"].astype("int64")
    df["amount_spent"] = df["amount_spent"].astype("float64")

    # Colunas derivadas úteis para a Gold
    df["year"] = df["transaction_date"].dt.year
    df["month"] = df["transaction_date"].dt.month
    df["day"] = df["transaction_date"].dt.day
    df["quarter"] = df["transaction_date"].dt.quarter

    # Persistência em Parquet
    df.to_parquet(SILVER_FILE, index=False)

    print("Processamento SILVER concluído.")
    print(f"Linhas finais: {df.shape[0]}")
    print(f"Arquivo salvo em: {SILVER_FILE}")

if __name__ == "__main__":
    main()