# Ingestão Streaming

Ingestão de eventos em tempo quase real via **Cloud Pub/Sub**, complementando
a carga batch anual do Saeb.

## Por que streaming aqui

O Saeb é anual, mas o dado não para de se mover entre as ondas: secretarias
municipais e estaduais enviam correções de cadastro, revisões de meta e novas
medições ao longo do ano. Modelar isso como batch significaria esperar o
próximo ciclo para refletir uma correção — inaceitável para uma política com
meta anual até 2030.

## Componentes

| Arquivo | Papel |
|---|---|
| `setup_pubsub.py` | Provisiona tópico e subscription (idempotente) |
| `publisher.py` | Simula eventos das secretarias e publica no tópico |
| `consumer.py` | Consome em micro-batches e grava na zona de streaming da Bronze |

A promoção para a Silver fica em `layers/silver/build_streaming.py`.

## Esquema do evento

```json
{
  "id_evento": "uuid",
  "tipo_evento": "atualizacao_indicador | nova_medicao | revisao_meta",
  "origem": "secretaria_municipal | secretaria_estadual | inep",
  "id_municipio": "3550308",
  "sigla_uf": "SP",
  "ano_referencia": 2024,
  "taxa_alfabetizacao": 76.4,
  "timestamp_evento": "2025-03-11T14:02:31+00:00"
}
```

Os municípios sorteados pelo publisher vêm da própria `gold.dim_municipio`.
Um gerador com IDs sintéticos produziria 100% de órfãs na validação de
integridade referencial e mascararia o comportamento real do pipeline.

## Decisões de design

**Micro-batch em vez de um arquivo por evento.** Object storage cobra por
operação, e milhares de objetos minúsculos degradam a leitura analítica — o
clássico *small files problem*. O buffer descarrega a cada 100 eventos ou 30
segundos, o que vier primeiro.

**Ack depois da gravação confirmada.** Se o processo morrer no meio do lote,
o Pub/Sub reentrega a mensagem. É uma garantia *at-least-once* deliberada:
perder evento é pior que processar duas vezes.

**Deduplicação por `id_evento` na Silver.** É o que converte o at-least-once
do broker em semântica efetivamente *exactly-once* na camada analítica.

**Zona separada da Bronze.** Os eventos vivem em
`gs://<bucket>/bronze/streaming/dt=YYYY-MM-DD/`, isolados do batch. Origens
com contratos e cadências diferentes não devem compartilhar o mesmo prefixo.

## Observabilidade

O consumer emite ao final de cada janela: eventos recebidos, gravados,
inválidos, número de micro-batches e **latência média fim a fim** (da emissão
ao consumo). Evento sem os campos obrigatórios é descartado com alerta em vez
de contaminar a Silver.

## Execução

```bash
# 1. Provisiona tópico e subscription (uma vez)
python -m ingestion.streaming.setup_pubsub

# 2. Em um terminal: consumer escutando por 2 minutos
python -m ingestion.streaming.consumer --duracao 120

# 3. Em outro terminal: publica 200 eventos
python -m ingestion.streaming.publisher --eventos 200 --intervalo 0.1

# 4. Promove os eventos para a Silver
python -m layers.silver.build_streaming
```

## FinOps

O Pub/Sub tem franquia mensal de 10 GB. O volume simulado aqui fica em alguns
megabytes, dentro do gratuito. A retenção de mensagem é limitada a 7 dias
para evitar acúmulo silencioso de backlog e o custo de armazenamento
associado.
