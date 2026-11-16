# Producer: publicação de eventos, `acks` e idempotência

## O que é

- **`EventPublisher`:** a porta que o código de negócio usa para publicar. Ela não tem nenhuma classe do Kafka. A implementação, `KafkaEventPublisher`, fica no `kafka-core`.
- **Envio assíncrono:** `send()` só coloca o registro num buffer. Uma thread do producer junta os registros em lotes e envia. A confirmação chega depois, por callback.
- **`acks`:** quantas réplicas precisam confirmar a escrita.
  - `all`: o líder e todas as réplicas em sincronia. Não perde mensagem enquanto sobrar uma réplica em sincronia.
  - `leader` (`acks=1`): só o líder. Se ele cair antes de replicar, a mensagem some.
  - `none` (`acks=0`): sem confirmação. Perde mensagens sem avisar.
- **Producer idempotente:** o producer recebe um id e numera cada registro por partição. Se uma retentativa reenviar algo que o broker já gravou, o broker reconhece o número e descarta. Sem isso, um timeout de rede seguido de retentativa gera duplicata.
- **Batching (`linger.ms`, `batch.size`) e compressão:** o producer espera um pouco para juntar mais registros num lote e comprime o lote inteiro. Troca alguns milissegundos de latência por vazão.

## Quando usar e quando não usar

- **`publishAndWait`:** quando o chamador precisa saber se o evento foi gravado antes de responder. Bloqueia a thread até o `send-timeout`.
- **`publish`:** quando dá para seguir sem esperar. Devolve um `CompletableFuture`; trate a falha nele.
- **Manter `acks=all` com idempotência** em tudo que não pode ser perdido nem duplicado. É o padrão.
- **Enfraquecer `acks`** só para dados descartáveis de altíssimo volume, como métricas, em que perder alguns registros é aceitável.
- **O producer idempotente não resolve tudo.** Ele evita duplicatas das retentativas internas do producer. Se a aplicação chamar `publish` duas vezes para o mesmo fato, são dois registros. Gravar no banco e publicar de forma atômica é assunto do outbox (Etapa 4).

## Como ativar e configurar no boilerplate

A aplicação importa `KafkaCoreConfiguration` e injeta a porta:

```java
PublishResult result = publisher.publishAndWait(event, key);

publisher.publish(event, key).whenComplete((result, failure) -> { ... });
```

O tópico é resolvido pelo tipo do evento ([ADR 0003](../adr/0003-roteamento-por-tipo-de-evento.md)).

| Propriedade (`boilerplate.kafka.`) | Padrão | Para que serve |
| --- | --- | --- |
| `bootstrap-servers` | obrigatória | Endereço dos brokers |
| `source` | obrigatória | Nome da aplicação; vai no envelope e no `client.id` |
| `producer.acks` | `all` | `all`, `leader` ou `none` |
| `producer.idempotence` | `true` | Producer idempotente |
| `producer.compression` | `lz4` | `none`, `gzip`, `snappy`, `lz4` ou `zstd` |
| `producer.linger` | `5ms` | Espera para encher o lote |
| `producer.batch-size` | `16KB` | Tamanho máximo do lote por partição |
| `producer.delivery-timeout` | `2m` | Tempo total para confirmar um registro, com retentativas |
| `producer.send-timeout` | `10s` | Quanto o `publishAndWait` bloqueia |
| `producer.properties.*` | vazio | Qualquer propriedade do producer Kafka; vence as de cima |

Usar `acks` mais fraco exige desligar a idempotência de forma explícita. Com `acks: leader` e idempotência ligada, a aplicação não sobe:

```yaml
boilerplate:
  kafka:
    producer:
      acks: leader
      idempotence: false
```

## Armadilhas comuns

- **Ignorar o futuro do `publish`.** A falha chega só no callback. Sem tratar, o evento se perde em silêncio.
- **Falha não significa que não gravou.** Um timeout pode acontecer depois de o broker gravar. `EventPublishException` quer dizer "sem confirmação". Por isso o consumidor precisa ser idempotente.
- **`acks=all` com `min.insync.replicas=1`.** Se só o líder estiver em sincronia, `all` vira "só o líder". Em produção use fator de replicação 3 e `min.insync.replicas=2`.
- **`max.block.ms`.** O `send()` bloqueia até 60 segundos por padrão quando não consegue os metadados do tópico (broker fora do ar). Reduza em `producer.properties` se isso for bloquear threads HTTP.
- **Publicar dentro de uma transação de banco.** O banco pode desfazer a transação depois de o evento já ter saído. A solução é o outbox.

## Como foi testado

- [`KafkaEventPublisherIntegrationTest`](../../kafka-core/src/test/java/io/github/lucascinigalia/kafkaboilerplate/core/producer/KafkaEventPublisherIntegrationTest.java): envio síncrono e assíncrono contra um Kafka real, e a configuração efetiva do producer (idempotência, `acks=all`, compressão).
- [`KafkaEventPublisherFailureTest`](../../kafka-core/src/test/java/io/github/lucascinigalia/kafkaboilerplate/core/producer/KafkaEventPublisherFailureTest.java): sem broker, o `publish` falha o futuro e o `publishAndWait` lança `EventPublishException`.
- [`BoilerplateKafkaPropertiesTest`](../../kafka-core/src/test/java/io/github/lucascinigalia/kafkaboilerplate/core/config/BoilerplateKafkaPropertiesTest.java): os padrões e a regra que impede `acks` fraco com idempotência ligada.
- [`ProducerAppIntegrationTest`](../../examples/producer-app/src/test/java/io/github/lucascinigalia/kafkaboilerplate/examples/producer/ProducerAppIntegrationTest.java): de uma requisição HTTP até o registro no tópico.
- [`HexagonalArchitectureTest`](../../examples/producer-app/src/test/java/io/github/lucascinigalia/kafkaboilerplate/examples/producer/HexagonalArchitectureTest.java): domínio e aplicação não dependem de classes do Kafka.
