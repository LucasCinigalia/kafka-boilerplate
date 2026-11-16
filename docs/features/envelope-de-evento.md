# Envelope de evento

## O que é

Um formato padrão em volta de todo evento, definido em `EventEnvelope`:

| Campo | Para que serve |
| --- | --- |
| `eventId` | Identificador único. É por ele que o consumidor detecta duplicatas (Etapa 4). |
| `type` | Nome lógico do evento, por exemplo `OrderCreated`. Define também o tópico de destino. |
| `version` | Versão do contrato do payload. |
| `occurredAt` | Quando o fato aconteceu. |
| `correlationId` | Igual em todos os eventos do mesmo fluxo de negócio. Serve para seguir o fluxo nos logs. |
| `causationId` | `eventId` do evento que causou este. Vazio quando o evento inicia o fluxo. |
| `source` | Aplicação que produziu o evento. |
| `payload` | Os dados do evento. |

No Kafka, o envelope inteiro vai como JSON no valor do registro, e os metadados são repetidos nos headers. O motivo está na [ADR 0002](../adr/0002-envelope-no-valor-e-metadados-nos-headers.md).

```json
{
  "eventId": "58dc465f-10bb-44b5-a868-4e8f9a16bd40",
  "type": "OrderCreated",
  "version": 1,
  "occurredAt": "2026-11-16T22:07:28.650Z",
  "correlationId": "request-42",
  "causationId": null,
  "source": "producer-app",
  "payload": { "orderId": "133f2acd-...", "customerId": "c1", "totalAmount": 10 }
}
```

## Quando usar e quando não usar

- **Usar** em todo evento publicado pelo boilerplate. Os recursos das próximas etapas (deduplicação, DLT, tracing) dependem desses campos.
- **Não usar** para consumir tópicos de terceiros que têm outro formato. Nesse caso o adaptador de entrada converte o formato deles.

## Como ativar e configurar no boilerplate

Crie os eventos pela `EventFactory`, que preenche `eventId`, `occurredAt` e `source`:

```java
// Evento que inicia um fluxo: correlationId novo.
events.create("OrderCreated", 1, payload);

// Evento que continua um fluxo vindo de fora (por exemplo, de um header HTTP).
events.create("OrderCreated", 1, payload, correlationId);

// Evento causado por outro: mesmo correlationId, causationId apontando para a causa.
events.createCausedBy(cause, "OrderApproved", 1, payload);
```

O `source` vem de `boilerplate.kafka.source`.

## Armadilhas comuns

- **Confundir `correlationId` com `causationId`.** O primeiro é o mesmo no fluxo inteiro; o segundo aponta só para o evento imediatamente anterior.
- **Gerar um `eventId` novo ao reenviar o mesmo evento.** O consumidor passa a tratar a retentativa como evento novo e processa duas vezes.
- **Mudar o payload sem subir a `version`.** Consumidores antigos quebram sem aviso. A Etapa 5 trata de evolução de schema.
- **Colocar o nome da classe Java na mensagem.** O serializador não grava o header `__TypeId__` de propósito.

## Como foi testado

- [`EventFactoryTest`](../../kafka-core/src/test/java/io/github/lucascinigalia/kafkaboilerplate/core/event/EventFactoryTest.java): ids, correlação, causação e campos obrigatórios.
- [`KafkaEventPublisherIntegrationTest`](../../kafka-core/src/test/java/io/github/lucascinigalia/kafkaboilerplate/core/producer/KafkaEventPublisherIntegrationTest.java), teste `publishAndWaitWritesTheEnvelopeAsJsonWithKeyAndHeaders`: lê o registro com um consumidor puro e confere o JSON, a chave e os headers.
