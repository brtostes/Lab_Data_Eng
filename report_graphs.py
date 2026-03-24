from __future__ import annotations

import json
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

from utils import SILVER_PARQUET_PATH, REPORTS_DIR

IMG_DIR = REPORTS_DIR / 'img'
QUALITY_JSON = SILVER_PARQUET_PATH.parent / 'data_quality_summary.json'
REPORT_MD = REPORTS_DIR / 'data_quality_and_graphs.md'


def save_plot(path: Path) -> None:
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close()


def main() -> None:
    IMG_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(SILVER_PARQUET_PATH)
    with open(QUALITY_JSON, 'r', encoding='utf-8') as f:
        quality = json.load(f)

    # 1. Distribuição de gasto
    plt.figure(figsize=(8, 5))
    df['amount_spent'].hist(bins=50)
    plt.title('Distribuição de amount_spent')
    plt.xlabel('amount_spent')
    plt.ylabel('Frequência')
    save_plot(IMG_DIR / 'grafico_1_amount_spent.png')

    # 2. Gasto médio por segmento
    plt.figure(figsize=(8, 5))
    df.groupby('segment')['amount_spent'].mean().sort_values().plot(kind='bar')
    plt.title('Gasto médio por segmento')
    plt.xlabel('segment')
    plt.ylabel('Gasto médio')
    save_plot(IMG_DIR / 'grafico_2_segmento.png')

    # 3. Gasto total por método de pagamento
    plt.figure(figsize=(8, 5))
    df.groupby('payment_method')['amount_spent'].sum().sort_values().plot(kind='bar')
    plt.title('Gasto total por método de pagamento')
    plt.xlabel('payment_method')
    plt.ylabel('Gasto total')
    save_plot(IMG_DIR / 'grafico_3_pagamento.png')

    # 4. Série temporal mensal
    monthly = df.set_index('transaction_date').resample('ME')['amount_spent'].sum()
    plt.figure(figsize=(10, 5))
    monthly.plot()
    plt.title('Gasto total mensal (2018-2025)')
    plt.xlabel('Data')
    plt.ylabel('Gasto total')
    save_plot(IMG_DIR / 'grafico_4_serie_temporal.png')

    # 5. Gasto médio por faixa etária
    bins = [15, 25, 35, 45, 55, 65, 79]
    labels = ['15-24', '25-34', '35-44', '45-54', '55-64', '65-78']
    age_group = pd.cut(df['age'], bins=bins, labels=labels, right=False)
    plt.figure(figsize=(8, 5))
    df.assign(age_group=age_group).groupby('age_group', observed=False)['amount_spent'].mean().plot(kind='bar')
    plt.title('Gasto médio por faixa etária')
    plt.xlabel('Faixa etária')
    plt.ylabel('Gasto médio')
    save_plot(IMG_DIR / 'grafico_5_faixa_etaria.png')

    # 6. Gasto total por estado (extra)
    plt.figure(figsize=(10, 5))
    df.groupby('state_name')['amount_spent'].sum().sort_values(ascending=False).head(10).sort_values().plot(kind='barh')
    plt.title('Top 10 estados por gasto total')
    plt.xlabel('Gasto total')
    plt.ylabel('Estado')
    save_plot(IMG_DIR / 'grafico_6_estados_top10.png')

    lines = [
        '# Relatório de Qualidade de Dados e Análise Exploratória',
        '',
        '## 1. Qualidade dos Dados',
        f"- Base bruta: {quality['raw_shape']['rows']:,} linhas e {quality['raw_shape']['cols']} colunas.",
        f"- Base silver: {quality['silver_shape']['rows']:,} linhas e {quality['silver_shape']['cols']} colunas.",
        f"- Duplicatas na base bruta: {quality['duplicate_rows_raw']}.",
        f"- Duplicatas após tratamento: {quality['duplicate_rows_silver']}.",
        '',
        '### Nulos detectados na base bruta',
        '',
        '| Coluna | Nulos | Percentual |',
        '|---|---:|---:|',
    ]

    for col, stats in quality['nulls_raw'].items():
        lines.append(f"| {col} | {stats['count']} | {stats['pct']:.2f}% |")

    lines.extend([
        '',
        '## 2. Gráficos',
        '',
        '### Gráfico 1 - Distribuição de `amount_spent`',
        '![Gráfico 1](img/grafico_1_amount_spent.png)',
        '',
        '### Gráfico 2 - Gasto médio por `segment`',
        '![Gráfico 2](img/grafico_2_segmento.png)',
        '',
        '### Gráfico 3 - Gasto total por `payment_method`',
        '![Gráfico 3](img/grafico_3_pagamento.png)',
        '',
        '### Gráfico 4 - Série temporal mensal do gasto total',
        '![Gráfico 4](img/grafico_4_serie_temporal.png)',
        '',
        '### Gráfico 5 - Gasto médio por faixa etária',
        '![Gráfico 5](img/grafico_5_faixa_etaria.png)',
        '',
        '### Gráfico 6 - Top 10 estados por gasto total (extra)',
        '![Gráfico 6](img/grafico_6_estados_top10.png)',
    ])

    REPORT_MD.write_text('\n'.join(lines), encoding='utf-8')
    print(f'Relatório salvo em: {REPORT_MD}')


if __name__ == '__main__':
    main()
