# Ambiente local: Kafka em modo KRaft e Kafka UI

## O que é

Um Docker Compose (`infra/compose/compose.yaml`) com:

- **Kafka em modo KRaft:** um único nó que é broker e controller ao mesmo tempo. KRaft é o modo em que o próprio Kafka guarda os metadados do cluster (tópicos, partições, líderes) num quorum Raft, sem ZooKeeper. A partir do Kafka 4.0 é o único modo que existe.
- **Kafka UI (kafbat):** interface web para ver brokers, tópicos, mensagens e consumer groups.

## Quando usar e quando não usar

- **Usar** para desenvolver e explorar os exemplos na sua máquina.
- **Não usar** como referência de produção: há um broker só, sem réplicas e sem autenticação. Os testes de integração também não dependem dele; eles sobem o próprio Kafka com Testcontainers.

## Como ativar e configurar

```bash
docker compose -f infra/compose/compose.yaml up -d
```

```bash
docker compose -f infra/compose/compose.yaml down
```

Para apagar também os dados do Kafka, acrescente `-v` ao `down`.

| Serviço | Endereço |
| --- | --- |
| Kafka, para aplicações rodando no host | `localhost:9092` |
| Kafka, para outros contêineres do Compose | `kafka:29092` |
| Kafka UI | http://localhost:8090 |

Variáveis opcionais (ambiente ou arquivo `.env` em `infra/compose/`):

| Variável | Default | Para que serve |
| --- | --- | --- |
| `KAFKA_VERSION` | `4.2.2` | Tag da imagem `apache/kafka` |
| `KAFKA_PORT` | `9092` | Porta do Kafka no host |
| `KAFKA_UI_VERSION` | `v1.5.0` | Tag da imagem `kafbat/kafka-ui` |
| `KAFKA_UI_PORT` | `8090` | Porta da Kafka UI no host |

## Armadilhas comuns

- **Listeners anunciados.** O cliente se conecta ao bootstrap, recebe do broker o endereço anunciado (`advertised.listeners`) e reconecta nele. Por isso há dois listeners: `EXTERNAL` anuncia `localhost:9092` (vale para o host) e `INTERNAL` anuncia `kafka:29092` (vale dentro da rede do Compose). Usar o endereço errado dá conexão que abre e depois falha.
- **Fator de replicação dos tópicos internos.** Com um broker só, `__consumer_offsets` e `__transaction_state` precisam de fator de replicação 1. Sem isso, consumer groups e transações não funcionam.
- **Criação automática de tópicos desligada.** `auto.create.topics.enable=false` é proposital: um nome de tópico digitado errado gera erro em vez de criar um tópico novo com configuração padrão. Os tópicos serão declarados por propriedades (Etapa 1).
- **Porta 8080.** A Kafka UI fica na 8090 para deixar a 8080 livre para as aplicações Spring.

## Como foi testado

- Manualmente: `docker compose up -d`, o Kafka fica `healthy` e a Kafka UI mostra o cluster `local` como online, com 1 broker e controller KRaft.
- Automaticamente: [`KafkaEnvironmentSmokeTest`](../../kafka-core/src/test/java/io/github/lucascinigalia/kafkaboilerplate/core/KafkaEnvironmentSmokeTest.java) sobe um Kafka real com Testcontainers e confere que o broker responde.
