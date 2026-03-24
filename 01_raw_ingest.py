from pathlib import Path
import shutil
import pandas as pd

RAW_DIR = Path("data/raw")
SOURCE_FILE = Path("customer_spending_1M_2018_2025.csv")
TARGET_FILE = RAW_DIR / SOURCE_FILE.name

def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    if not SOURCE_FILE.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {SOURCE_FILE}")

    shutil.copy2(SOURCE_FILE, TARGET_FILE)

    df = pd.read_csv(TARGET_FILE)
    print("Ingestão RAW concluída.")
    print(f"Linhas: {df.shape[0]}")
    print(f"Colunas: {df.shape[1]}")
    print("Colunas:")
    print(df.columns.tolist())

if __name__ == "__main__":
    main()