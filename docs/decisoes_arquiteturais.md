# Decisoes Arquiteturais

Registro das escolhas tecnicas do projeto e suas justificativas.

---

## 1. Cloud provider: GCP

**Decisao:** Google Cloud Platform.

**Motivo:** a Base dos Dados hospeda seus datasets nativamente no BigQuery
publico. Usar GCP elimina a etapa de download e reupload — a ingestao vira
uma query. Em AWS ou Azure seria necessario baixar os dados localmente e
depois enviar, adicionando latencia e um ponto de falha.

**Trade-off:** menor portabilidade entre nuvens. Aceitavel dado o escopo e
o prazo do projeto.

---

## 2. Arquitetura Medalhao: Storage + BigQuery

**Decisao:** Bronze em Cloud Storage (Parquet), Silver e Gold no BigQuery.

**Motivo:** dado bruto nao e consultado diretamente. Manter a Bronze em
object storage custa uma fracao do preco do data warehouse. Silver e Gold,
que recebem queries analiticas, ganham com o formato colunar e o motor
distribuido do BigQuery.

Esse desenho e o padrao **Lakehouse**: storage barato para o dado bruto,
warehouse para o dado consultavel.

**Trade-off:** duas tecnologias de armazenamento em vez de uma. Compensado
pela reducao de custo e pela separacao clara de responsabilidades.

---

## 3. Particionamento por ano

**Decisao:** Parquet particionado por `ano` nas tabelas que possuem a coluna.

**Motivo:** FinOps aplicado na escrita. A camada Silver le apenas as
particoes necessarias, reduzindo o volume escaneado.

**Armadilha encontrada:** particionar por uma coluna que nao existe na
origem replica o dataset inteiro em cada diretorio de particao. As tabelas
de metas foram ingeridas com `partition_cols=["ano"]` sem possuir a coluna,
gerando `meta_alfabetizacao_uf` com 81 linhas em vez de 27. A leitura na
Silver passou a distinguir tabelas com particao real das replicadas, e a
configuracao de ingestao foi corrigida em `config/settings.py`.

---

## 4. Compressao Snappy

**Decisao:** Snappy em vez de gzip ou zstd.

**Motivo:** equilibrio entre taxa de compressao e custo de CPU na
leitura/escrita. E o padrao de fato em pipelines Spark e BigQuery.

---

## 5. Indicador oficial em vez de recalculo

**Decisao:** usar a `taxa_alfabetizacao` ja agregada pelo INEP nas tabelas
`municipio` e `uf`, em vez de recalcular a partir da tabela `alunos`
(3,9 milhoes de linhas).

**Motivo:** o indicador oficial e o do INEP. Recalcular introduz risco de
divergencia com o numero publicado, sem ganho analitico claro para o
objetivo do desafio.

**Consequencia:** a tabela `alunos` permanece disponivel na Bronze como
base de granularidade fina, destinada ao treino de modelos de ML na Gold.

---

## 6. Unpivot das metas (wide -> long)

**Decisao:** converter `meta_alfabetizacao_2024` ... `meta_alfabetizacao_2030`
de colunas para linhas, com a coluna `ano_meta`.

**Motivo:** o formato wide impede o join temporal com os resultados, que
sao organizados por linha/ano. O formato long tambem simplifica a
comparacao meta x resultado e a evolucao temporal na Gold.

---

## 7. Padronizacao de `id_municipio` com zfill(7)

**Decisao:** normalizar o codigo IBGE como string de 7 digitos.

**Motivo:** o codigo IBGE tem zeros a esquerda em alguns municipios. Se
tratado como inteiro, esses zeros se perdem e o join falha silenciosamente
para os municipios afetados.

---

## 8. Regiao southamerica-east1

**Decisao:** todos os recursos em Sao Paulo.

**Motivo:** reduz latencia e evita custo de egress entre regioes. Manter
Storage e BigQuery na mesma regiao e requisito para leitura direta.

---

## 9. Validacao bloqueia promocao entre camadas

**Decisao:** duplicatas ou nulos em chave reprovam a tabela e podem
interromper o pipeline (`quality/bloquear_se_reprovado`).

**Motivo:** dado invalido propagado para a Gold contamina dashboards e
modelos. E mais barato falhar cedo.

---

## 10. Falha isolada por tabela na ingestao

**Decisao:** cada tabela e ingerida em bloco try/except independente.

**Motivo:** uma tabela indisponivel na origem nao deve impedir a ingestao
das demais. O relatorio final consolida sucessos e falhas.

---

## Achados de qualidade de dados

| Achado | Interpretacao |
|---|---|
| `DF` e `RR` aparecem em `meta_alfabetizacao_uf` mas nao em `uf_resultado` | O DF nao possui municipios; RR provavelmente ficou fora da avaliacao estadual. Nao e erro de pipeline. |
| 5.550 municipios em `municipio` contra 5.352 nas metas | 198 municipios sem meta definida. Investigar antes de calcular cobertura nacional. |
| `proporcao_aluno_nivel_*` com ~48% de nulos | Decidir na Gold entre imputacao, exclusao ou manutencao com flag. |
| Metas vao ate 2030, resultados so ate 2024 | O join meta x resultado so cruza em 2024. Anos futuros terao meta sem resultado. |
