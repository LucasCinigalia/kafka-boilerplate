# ADR 0002: envelope completo no valor, metadados repetidos nos headers

- **Status:** aceita
- **Data:** 2026-11-16

## Contexto

Todo evento carrega metadados (`eventId`, `type`, `version`, `occurredAt`, `correlationId`, `causationId`, `source`) além do payload. Era preciso decidir onde eles viajam dentro do registro Kafka.

| Opção | Vantagem | Desvantagem |
| --- | --- | --- |
| Só no valor (JSON do envelope) | Mensagem autocontida; sobrevive a qualquer ferramenta que copie só chave e valor | Para filtrar ou rotear é preciso desserializar o valor inteiro |
| Só nos headers (valor é o payload puro) | Valor enxuto; roteamento sem desserializar | Ferramentas e conectores que ignoram headers perdem os metadados |
| Nos dois | Autocontida e roteável sem desserializar | Dado duplicado; alguns bytes a mais por mensagem |

## Decisão

O valor do registro é o **envelope completo em JSON**. Os metadados são **repetidos nos headers** (`event-id`, `event-type`, `event-version`, `correlation-id`, `causation-id`, `event-source`).

O valor é a fonte da verdade. Os headers são uma cópia para filtros, roteamento, logs e, na Etapa 3, para lidar com mensagens cujo valor não desserializa.

O serializador não grava o header `__TypeId__` do Spring Kafka: o nome da classe Java do payload não faz parte do contrato. O consumidor decide o tipo pelo `type` e pela `version`.

O timestamp do registro Kafka é o `occurredAt` do evento.

## Consequências

- Um consumidor em outra linguagem lê o evento só com um parser JSON.
- Renomear ou mover a classe do payload não quebra consumidores.
- Há duplicação: quem alterar o envelope precisa manter os headers em sincronia. Isso acontece num lugar só, o `KafkaEventPublisher`.
