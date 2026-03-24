from __future__ import annotations

import json
import pandas as pd

from utils import ensure_directories, RAW_CSV_PATH, SILVER_PARQUET_PATH, standardize_columns


QUALITY_JSON = SILVER_PARQUET_PATH.parent / 'data_quality_summary.json'


def build_quality_report(df_before: pd.DataFrame, df_after: pd.DataFrame) -> dict:
    null_counts = df_before.isna().sum().sort_values(ascending=False)
    null_pct = (df_before.isna().mean() * 100).sort_values(ascending=False)
    return {
        'raw_shape': {'rows': int(df_before.shape[0]), 'cols': int(df_before.shape[1])},
        'silver_shape': {'rows': int(df_after.shape[0]), 'cols': int(df_after.shape[1])},
        'duplicate_rows_raw': int(df_before.duplicated().sum()),
        'duplicate_rows_silver': int(df_after.duplicated().sum()),
        'nulls_raw': {
            col: {
                'count': int(null_counts[col]),
                'pct': round(float(null_pct[col]), 4),
            }
            for col in df_before.columns
            if int(null_counts[col]) > 0
        },
        'dtypes_silver': {col: str(dtype) for col, dtype in df_after.dtypes.items()},
        'numeric_summary_silver': df_after.describe(include='all', datetime_is_numeric=True).fillna('').astype(str).to_dict(),
    }


def main() -> None:
    ensure_directories()

    df = pd.read_csv(RAW_CSV_PATH)
    df_raw = df.copy()

    df = standardize_columns(df)

    df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
    df['gender'] = df['gender'].fillna('unknown').str.strip().str.lower()
    df['marital_status'] = df['marital_status'].fillna('unknown').str.strip().str.lower()
    df['state_name'] = df['state_name'].fillna('unknown').str.strip().str.title()
    df['segment'] = df['segment'].fillna('unknown').replace({'Missing': 'unknown'}).astype(str).str.strip().str.lower()
    df['employee_status'] = df['employee_status'].fillna('unknown').astype(str).str.strip().str.lower()
    df['payment_method'] = df['payment_method'].fillna('unknown').astype(str).str.strip()

    df['age'] = pd.to_numeric(df['age'], errors='coerce')
    df['referral'] = pd.to_numeric(df['referral'], errors='coerce').fillna(0)
    df['amount_spent'] = pd.to_numeric(df['amount_spent'], errors='coerce')

    median_age = df['age'].median()
    df['age'] = df['age'].fillna(median_age)

    # Remoções justificadas: data inválida e gasto ausente inviabilizam análises temporais e de receita.
    df = df.dropna(subset=['transaction_date', 'amount_spent'])
    df = df.drop_duplicates()

    df['transaction_id'] = df['transaction_id'].astype('int64')
    df['age'] = df['age'].round().astype('int64')
    df['referral'] = df['referral'].round().astype('int64')
    df['amount_spent'] = df['amount_spent'].astype('float64')

    df['year'] = df['transaction_date'].dt.year.astype('int64')
    df['month'] = df['transaction_date'].dt.month.astype('int64')
    df['day'] = df['transaction_date'].dt.day.astype('int64')
    df['quarter'] = df['transaction_date'].dt.quarter.astype('int64')

    df.to_parquet(SILVER_PARQUET_PATH, index=False)

    quality_report = build_quality_report(df_raw, df)
    with open(QUALITY_JSON, 'w', encoding='utf-8') as f:
        json.dump(quality_report, f, ensure_ascii=False, indent=2)

    print('Processamento SILVER concluído.')
    print(f'Arquivo salvo em: {SILVER_PARQUET_PATH}')
    print(f'Linhas finais: {df.shape[0]}')
    print(f'Relatório JSON salvo em: {QUALITY_JSON}')


if __name__ == '__main__':
    main()
