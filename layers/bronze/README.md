# Camada Bronze — Raw Data

Os dados brutos **não ficam neste diretorio**. A Bronze e materializada
no Cloud Storage, em `gs://<bucket>/bronze/<tabela>/`, em formato Parquet
particionado por ano quando aplicavel.

## Por que no Storage e nao no BigQuery

Dado bruto nao e consultado diretamente. Manter a Bronze em object storage
custa uma fracao do preco do data warehouse e preserva o historico completo
sem custo de armazenamento estruturado — decisao de FinOps.

## O que roda aqui

A ingestao e feita por `ingestion/batch/ingest_bronze.py`.

## Caracteristicas

- Sem transformacoes significativas
- Historico completo preservado
- Metadados de rastreabilidade: `_ingestao_timestamp` e `_fonte`
