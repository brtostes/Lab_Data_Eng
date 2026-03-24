from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

SILVER_FILE = Path("data/silver/customer_spending_silver.parquet")
REPORT_DIR = Path("reports")
IMG_DIR = REPORT_DIR / "img"

def main():
    REPORT_DIR.mkdir(exist_ok=True)
    IMG_DIR.mkdir(exist_ok=True)

    df = pd.read_parquet(SILVER_FILE)

    # 1. Histograma amount_spent
    plt.figure(figsize=(8,5))
    df["amount_spent"].hist(bins=50)
    plt.title("Distribuição de Amount Spent")
    plt.xlabel("Amount Spent")
    plt.ylabel("Frequência")
    plt.tight_layout()
    plt.savefig(IMG_DIR / "grafico_1_amount_spent.png")
    plt.close()

    # 2. Média por segmento
    plt.figure(figsize=(8,5))
    df.groupby("segment")["amount_spent"].mean().sort_values().plot(kind="bar")
    plt.title("Gasto Médio por Segmento")
    plt.xlabel("Segmento")
    plt.ylabel("Gasto médio")
    plt.tight_layout()
    plt.savefig(IMG_DIR / "grafico_2_segmento.png")
    plt.close()

    # 3. Total por método de pagamento
    plt.figure(figsize=(8,5))
    df.groupby("payment_method")["amount_spent"].sum().sort_values().plot(kind="bar")
    plt.title("Gasto Total por Método de Pagamento")
    plt.xlabel("Método de pagamento")
    plt.ylabel("Gasto total")
    plt.tight_layout()
    plt.savefig(IMG_DIR / "grafico_3_pagamento.png")
    plt.close()

    # 4. Série temporal mensal
    monthly = df.set_index("transaction_date").resample("ME")["amount_spent"].sum()
    plt.figure(figsize=(10,5))
    monthly.plot()
    plt.title("Gasto Total Mensal")
    plt.xlabel("Data")
    plt.ylabel("Gasto total")
    plt.tight_layout()
    plt.savefig(IMG_DIR / "grafico_4_serie_temporal.png")
    plt.close()

    # 5. Gasto médio por faixa etária
    bins = [15, 25, 35, 45, 55, 65, 79]
    labels = ["15-24", "25-34", "35-44", "45-54", "55-64", "65-78"]
    df["age_group"] = pd.cut(df["age"], bins=bins, labels=labels, right=False)

    plt.figure(figsize=(8,5))
    df.groupby("age_group")["amount_spent"].mean().plot(kind="bar")
    plt.title("Gasto Médio por Faixa Etária")
    plt.xlabel("Faixa etária")
    plt.ylabel("Gasto médio")
    plt.tight_layout()
    plt.savefig(IMG_DIR / "grafico_5_faixa_etaria.png")
    plt.close()

if __name__ == "__main__":
    main()