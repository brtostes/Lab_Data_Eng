from __future__ import annotations

from pathlib import Path
import os
import re
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_RAW = PROJECT_ROOT / 'data' / 'raw'
DATA_SILVER = PROJECT_ROOT / 'data' / 'silver'
REPORTS_DIR = PROJECT_ROOT / 'reports'
SQL_DIR = PROJECT_ROOT / 'sql'

RAW_CSV_NAME = 'customer_spending_1M_2018_2025.csv'
RAW_CSV_PATH = DATA_RAW / RAW_CSV_NAME
SILVER_PARQUET_PATH = DATA_SILVER / 'customer_spending_silver.parquet'

COLUMN_MAPPING = {
    'Transaction_ID': 'transaction_id',
    'Transaction_date': 'transaction_date',
    'Gender': 'gender',
    'Age': 'age',
    'Marital_status': 'marital_status',
    'State_names': 'state_name',
    'Segment': 'segment',
    'Employees_status': 'employee_status',
    'Payment_method': 'payment_method',
    'Referral': 'referral',
    'Amount_spent': 'amount_spent',
}


def ensure_directories() -> None:
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    DATA_SILVER.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    (REPORTS_DIR / 'img').mkdir(parents=True, exist_ok=True)


def to_snake_case(name: str) -> str:
    name = re.sub(r'[^a-zA-Z0-9]+', '_', name)
    name = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
    return name.strip('_').lower()


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    mapped = {col: COLUMN_MAPPING.get(col, to_snake_case(col)) for col in df.columns}
    return df.rename(columns=mapped)


def add_age_group(df: pd.DataFrame) -> pd.DataFrame:
    bins = [15, 25, 35, 45, 55, 65, 79]
    labels = ['15-24', '25-34', '35-44', '45-54', '55-64', '65-78']
    df = df.copy()
    df['age_group'] = pd.cut(df['age'], bins=bins, labels=labels, right=False)
    return df


def get_engine():
    from sqlalchemy import create_engine

    db_user = os.getenv('DB_USER', 'postgres')
    db_pass = os.getenv('DB_PASS', 'postgres')
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'lab01')
    conn_str = f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'
    return create_engine(conn_str)
