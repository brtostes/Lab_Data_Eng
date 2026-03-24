Lab01_PART1_
Ingestão de Dados End-to-End (Local) — Online Store Customer Transactions
Breno Tostes Garcia

Disciplina: Fundamentos de Engenharia de Dados
Aluno: Hercules — NUSP 18105650

Fonte de dados: Kraggle
Período coletado: 2018 - 2025
Total de registros: 1 milhão de registros


------------------------------------------------------------------------------------------------------------

# 1. Objetivo do projeto

Este projeto implementa um fluxo completo de engenharia de dados a partir de uma base transacional de gastos de consumidores. A solução foi estruturada para atender aos requisitos do laboratório, contemplando:

- ingestão do dado bruto;
- tratamento e padronização dos dados;
- análise de qualidade;
- geração de gráficos exploratórios;
- persistência em formato otimizado;
- modelagem analítica em esquema estrela;
- execução de consultas de negócio em PostgreSQL.

A base utilizada contém 1 milhão de registros, cobrindo transações entre **2018 e 2025**.


# 2. Arquitetura

Fonte CSV
   ↓
Python - Ingestão Raw
   ↓
data/raw
   ↓
Python - Limpeza e Padronização
   ↓
data/silver/customer_spending_silver.parquet
   ↓
Python - Carga analítica
   ↓
PostgreSQL (dimensões + fato)



## 2.1. Bronze

Armazena o arquivo original sem transformações, preservando o dado bruto para rastreabilidade e reprodutibilidade.

## 2.2. Silver

Realiza a padronização dos nomes das colunas, conversão de tipos, tratamento de valores ausentes, remoção de duplicatas, criação de atributos temporais e persistência em Parquet.

## 2.3. Gold

Organiza os dados em um modelo dimensional do tipo Star Schema, permitindo análises analíticas e consultas de negócio de forma mais eficiente.



# 3. Estrutura do Repositório

Lab01_PART1_NUSP/
│
├── data/
│   ├── raw/
│   └── silver/
│
├── notebooks/
│
├── reports/
│   └── img/
│
├── sql/
│   ├── create_tables.sql
│   ├── q1.sql
│   ├── q2.sql
│   ├── q3.sql
│   ├── q4.sql
│   └── q5.sql
│
├── src/
│   ├── 01_ingest_raw.py
│   ├── 02_process_silver.py
│   ├── 03_load_gold.py
│   ├── report_graphs.py
│
├── requirements.txt
└── README.md


# 4. Base de Dados Utilizada
_customer_spending_1M_2018_2025.csv_

## 4.1. Características gerais
* volume: 1.000.000 de linhas;
* 11 colunas;
* dados de transações de consumo;
* presença de variáveis categóricas, numéricas e temporais;
* presença de valores ausentes, o que permite demonstrar tratamento na camada Silver.


## 4.2. Colunas originais da base
* Transaction_ID
* Transaction_date
* Gender
* Age
* Marital_status
* State_names
* Segment
* Employees_status
* Payment_method
* Referral
* Amount_spent


# 5. Dicionário de Dados Padronizado
Após a padronização, as colunas passam a ter os nomes abaixo:

| Coluna padronizada | Descrição                               |
| ------------------ | --------------------------------------- |
| `transaction_id`   | Identificador da transação              |
| `transaction_date` | Data e hora da transação                |
| `gender`           | Gênero do consumidor                    |
| `age`              | Idade do consumidor                     |
| `marital_status`   | Estado civil                            |
| `state_name`       | Estado associado à transação            |
| `segment`          | Segmento do cliente                     |
| `employee_status`  | Situação ocupacional do cliente         |
| `payment_method`   | Método de pagamento utilizado           |
| `referral`         | Indicador de referência                 |
| `amount_spent`     | Valor gasto na transação                |
| `year`             | Ano derivado da data da transação       |
| `month`            | Mês derivado da data da transação       |
| `day`              | Dia derivado da data da transação       |
| `quarter`          | Trimestre derivado da data da transação |


# 6. Implementação por camada
## 6.1. Camada Bronze
### Finalidade

Preservar o arquivo bruto exatamente como foi recebido, sem qualquer transformação.

### Script
_src/01_ingest_raw.py_

### O que o script faz
* cria a estrutura de diretórios necessária;
* localiza o arquivo CSV na raiz do projeto ou em data/raw/;
* copia o arquivo bruto para data/raw/;
* exibe informações básicas da ingestão:
    * caminho salvo;
    * número de linhas;
    * número de colunas;
    * lista de colunas.

### Saída esperada
data/raw/customer_spending_1M_2018_2025.csv


## 6.2. Camada Silver
### Finalidade
Transformar o dado bruto em um conjunto limpo, consistente e pronto para análises.

### Script
_src/02_process_silver.py_

### Transformações aplicadas
### a) Padronização dos nomes de colunas
Conversão para snake_case e ajuste semântico:

| Original           | Padronizado        |
| ------------------ | ------------------ |
| `Transaction_ID`   | `transaction_id`   |
| `Transaction_date` | `transaction_date` |
| `Gender`           | `gender`           |
| `Age`              | `age`              |
| `Marital_status`   | `marital_status`   |
| `State_names`      | `state_name`       |
| `Segment`          | `segment`          |
| `Employees_status` | `employee_status`  |
| `Payment_method`   | `payment_method`   |
| `Referral`         | `referral`         |
| `Amount_spent`     | `amount_spent`     |


### b) Conversão de tipos
* transaction_date → datetime
* age → numérico inteiro
* referral → inteiro
* amount_spent → float

### c) Padronização de texto
* gender, marital_status, segment, employee_status padronizados com limpeza textual;
* state_name padronizado com capitalização apropriada;
* payment_method normalizado;
* valor textual "Missing" em segment convertido para "unknown".

### d) Tratamento de valores ausentes
As decisões adotadas foram:
* gender → preenchido com "unknown";
* marital_status → preenchido com "unknown" quando necessário;
* state_name → preenchido com "unknown" quando necessário;
* segment → preenchido com "unknown";
* employee_status → preenchido com "unknown";
* referral → preenchido com 0;
* age → imputado com a mediana;
* amount_spent → linhas removidas quando nulo;
* transaction_date → linhas removidas quando inválida ou ausente.

### e) Remoção de duplicatas
* remoção de linhas duplicadas exatas.

### f) Criação de atributos temporais
Derivação das colunas:
* year
* month
* day
* quarter

### Persistência
A camada Silver é salva em formato Parquet:
_data/silver/customer_spending_silver.parquet_

### Relatório auxiliar
O script também gera um resumo de qualidade em JSON:
_data/silver/data_quality_summary.json_

## 6.3. Relatório exploratório e gráficos
### Script
_src/report_graphs.py_

### Saídas geradas
relatório em Markdown:
_reports/data_quality_and_graphs.md_

### gráficos em:
_reports/img/_

### Gráficos produzidos
* Distribuição de amount_spent;
* Gasto médio por segment;
* Gasto total por payment_method;
* Série temporal mensal do gasto total;
* Gasto médio por faixa etária;
* Top 10 estados por gasto total (gráfico extra).

## 6.4. Camada Gold
### Finalidade
Estruturar os dados em modelo analítico relacional no PostgreSQL.
* Script
_src/03_load_gold.py_

* Banco de dados
O projeto foi preparado para uso com PostgreSQL.

### Estratégia de modelagem
Foi adotado um esquema estrela (Star Schema), composto por:
* Tabelas dimensão
* lab01.dim_date
* lab01.dim_customer_profile
* lab01.dim_location
* lab01.dim_payment
* lab01.dim_segment
* Tabela fato
* lab01.fact_customer_spending
* Descrição das tabelas
* dim_date

### Armazena atributos temporais:
* date_id
* full_date
* year
* month
* day
* quarter
* dim_customer_profile

### Armazena atributos do perfil do cliente:
* customer_profile_id
* gender
* age
* age_group
* marital_status
* employee_status
* referral
* dim_location

### Armazena localização:
* location_id
* state_name
* dim_payment

### Armazena o método de pagamento:
* payment_id
* payment_method
* dim_segment

### Armazena o segmento do cliente:
* segment_id
* segment
* fact_customer_spending

### Armazena o evento transacional:
* transaction_id
* date_id
* customer_profile_id
* location_id
* payment_id
* segment_id
* amount_spent

### Scripts SQL
* criação do schema:
_sql/create_schema.sql_

* criação das tabelas:
_sql/create_tables.sql_


## 7. Perguntas de negócio
O projeto responde às seguintes perguntas analíticas:

1. Qual segmento apresenta o maior gasto médio por transação?
2. Quais estados concentram o maior valor total gasto?
3. Qual método de pagamento possui maior ticket médio?
4. Como evolui o gasto mensal total entre 2018 e 2025?
5. Clientes com referral gastam mais do que clientes sem referral?

### Scripts correspondentes
* SQL:
_q1.sql
q2.sql
q3.sql
q4.sql
q5.sql_


* execução via Python:
_03_report_graphs.py_


## 8. Qualidade dos dados
A análise inicial identificou a presença de valores ausentes em colunas relevantes, especialmente em variáveis de perfil e valor monetário.

### Estratégia adotada
As decisões de qualidade buscaram preservar o máximo possível da base sem comprometer a consistência analítica:

* variáveis categóricas com baixa proporção de nulos foram preenchidas com "unknown";
* age foi imputada pela mediana;
* referral foi preenchida com 0;
* linhas com amount_spent ausente foram removidas, pois inviabilizam análises de receita;
* linhas com transaction_date inválida também foram removidas, pois impedem análises temporais;
* duplicatas exatas foram eliminadas.


### Justificativa metodológica
Essa estratégia é adequada porque:
* evita imputações arbitrárias sobre a variável monetária principal;
* mantém a interpretabilidade dos dados;
* preserva a maior parte dos registros válidos;
* melhora a consistência da modelagem dimensional.


# 9. Como executar o projeto
## 9.1. Pré-requisitos
* Python 3.10+;
* PostgreSQL instalado e ativo;
* psql disponível no terminal.


## 9.2. Instalação das dependências
_pip install -r requirements.txt_


## 9.3. Posicionamento do arquivo de entrada
Coloque o arquivo:
_customer_spending_1M_2018_2025.csv_

na raiz do projeto ou diretamente em:
_data/raw/_


## 9.4. Executar a camada Bronze
_python src/01_ingest_raw.py_


## 9.5. Executar a camada Silver
_python src/02_process_silver.py_


## 9.6. Gerar o relatório exploratório e os gráficos
_python src/report_graphs.py_


## 9.7. Criar o banco no PostgreSQL
Exemplo:

_CREATE DATABASE lab01;_


## 9.8. Criar schema e tabelas
_psql -U postgres -d lab01 -f sql/create_tables.sql_


## 9.9. Ajustar conexão do banco
Caso necessário, altere as credenciais no arquivo src/utils.py de acordo com seu ambiente local.


## 9.10. Carregar a camada Gold
_python src/03_load_gold.py_


## 9.11. Executar as consultas de negócio
Via Python
_python src/03_report_graphs.py_



# 10. Ordem recomendada de execução
_pip install -r requirements.txt
python src/01_ingest_raw.py
python src/02_process_silver.py
python src/report_graphs.py
psql -U postgres -d lab01 -f sql/create_tables.sql
python src/03_load_gold.py



# 11. Produtos gerados no projeto
Ao final da execução, os principais artefatos gerados serão:

## Camada Bronze
_data/raw/customer_spending_1M_2018_2025.csv_

## Camada Silver
_data/silver/customer_spending_silver.parquet
data/silver/data_quality_summary.json_

## Relatórios
_reports/data_quality_and_graphs.md
reports/img/grafico_1_amount_spent.png
reports/img/grafico_2_segmento.png
reports/img/grafico_3_pagamento.png
reports/img/grafico_4_serie_temporal.png
reports/img/grafico_5_faixa_etaria.png
reports/img/grafico_6_estados_top10.png_

## Camada Gold
* schema lab01 no PostgreSQL;
* dimensões e fato populadas.


# 12. Considerações finais
A solução proposta atende aos requisitos centrais do laboratório, demonstrando um pipeline completo com:
* ingestão bruta rastreável;
* tratamento e qualificação dos dados;
* armazenamento em formato colunar eficiente;
* modelagem analítica em banco relacional;
* consultas de negócio orientadas à análise de consumo.

Além de cumprir os requisitos técnicos, o projeto foi estruturado de forma reprodutível, modular e extensível, permitindo futuras melhorias, como:
* particionamento do Parquet;
* orquestração automática do pipeline;
* testes de qualidade de dados;
* dashboard analítico;
* uso de ferramentas de workflow como Airflow ou Prefect.


# 13. Autor
Projeto desenvolvido para fins acadêmicos no contexto da disciplina Introdução à Modelagem de Dados, do curso de especialização em Engenharia de Dados & Big Data, do Programa de Educação Continuada da Escola Politécnica da Universidade de São Paulo (PECE/Poli/USP). 

