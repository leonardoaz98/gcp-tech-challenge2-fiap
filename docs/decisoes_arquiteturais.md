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
gerando `meta_alfabetizacao_uf` com 81 linhas em vez de 27.

**Correcao definitiva:** `gravar_local` passou a validar a existencia da
coluna antes de particionar (`particoes_validas`), eliminando o bug na
origem. O workaround que existia na leitura da Silver (`TABELAS_SEM_ANO`)
foi removido por descrever um estado que nao existe mais, substituido por
uma deduplicacao defensiva que protege contra dado antigo remanescente no
bucket.

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

---

## 11. Ingestao streaming via Cloud Pub/Sub

**Decisao:** Pub/Sub com consumer em micro-batch, gravando em zona
separada da Bronze (`bronze/streaming/`).

**Motivo:** o Saeb e anual, mas o dado nao para de se mover entre as
ondas. Secretarias enviam correcoes de cadastro, revisoes de meta e novas
medicoes ao longo do ano. Tratar isso como batch significaria esperar o
proximo ciclo para refletir uma correcao — inaceitavel para uma politica
com meta anual ate 2030.

**Trade-off:** um componente a mais para operar e monitorar. Compensado
pelo fato de o Pub/Sub ser serverless, sem cluster para gerenciar.

---

## 12. Micro-batch em vez de um arquivo por evento

**Decisao:** o consumer acumula ate 100 eventos ou 30 segundos antes de
gravar um unico Parquet.

**Motivo:** object storage cobra por operacao, e milhares de objetos
minusculos degradam a leitura analitica — o small files problem. O buffer
equilibra latencia e custo.

**Trade-off:** ate 30 segundos de latencia adicional. Irrelevante para um
indicador com meta anual.

---

## 13. At-least-once no broker, exactly-once na Silver

**Decisao:** o ack so acontece apos a gravacao confirmada no bucket; a
deduplicacao por `id_evento` fica na promocao para a Silver.

**Motivo:** se o processo morrer no meio de um lote, o Pub/Sub reentrega
a mensagem. Perder evento e pior que processar duas vezes. A chave de
deduplicacao converte a garantia do broker em semantica efetivamente
exactly-once na camada analitica, sem exigir transacao distribuida.

---

## 14. Views sem filtro e com colunas de reagregacao

**Decisao:** nenhuma view da Gold filtra linha estruturalmente valida, e
toda view agregada carrega `soma_*` e `qtd_*` alem da media.

**Motivo:** duas licoes aprendidas na pratica.

A primeira: as views filtravam por `WHERE gap_meta IS NOT NULL`. Como
`gap_meta` so existe quando ha resultado e meta, o filtro reduzia
silenciosamente todo o historico a 2024 — anulando o FULL OUTER JOIN
construido na fato. Nenhum erro, nenhum alerta.

A segunda: media de media nao e reagregavel. A media nacional calculada
a partir das medias por UF diverge da media sobre os municipios, porque
cada UF tem um numero diferente deles. Com soma e contagem, o consumidor
reagrega em qualquer nivel sem perder precisao.

**Criterio oficial:** a media e sempre municipal. O municipio e a unidade
de gestao do Compromisso Nacional, entao UF e regiao sao agregacoes de
municipios.

---

## 15. Diagnostico de cobertura como alarme

**Decisao:** `build_views.py` reporta a distribuicao de linhas por ano
apos criar cada view.

**Motivo:** silent-drop e a classe de bug mais cara deste projeto porque
nao gera excecao. Um filtro mal colocado produz um dashboard que parece
funcionar. Logar a cobertura torna a perda visivel na hora.

## Achados de qualidade de dados

| Achado | Interpretacao |
|---|---|
| `DF` e `RR` aparecem em `meta_alfabetizacao_uf` mas nao em `uf_resultado` | O DF nao possui municipios; RR provavelmente ficou fora da avaliacao estadual. Nao e erro de pipeline. |
| 5.550 municipios em `municipio` contra 5.352 nas metas | 198 municipios sem meta definida. Investigar antes de calcular cobertura nacional. |
| `proporcao_aluno_nivel_*` com ~48% de nulos | Decidir na Gold entre imputacao, exclusao ou manutencao com flag. |
| Metas vao ate 2030, resultados so ate 2024 | O join meta x resultado so cruza em 2024. Anos futuros terao meta sem resultado. |
