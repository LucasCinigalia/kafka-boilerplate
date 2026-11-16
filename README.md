# kafka-boilerplate

Template de Kafka com Spring Boot, feito para ser copiado para projetos novos. Não tem regra de negócio: os exemplos usam eventos genéricos só para demonstrar cada recurso do Kafka, com documentação e testes.

O projeto está em construção, por etapas. O andamento fica na seção "Status" do [CLAUDE.md](CLAUDE.md).

## Requisitos

- Java 21
- Docker (ambiente local e testes de integração com Testcontainers)

## Como rodar

Subir o Kafka e a Kafka UI (http://localhost:8090):

```bash
docker compose -f infra/compose/compose.yaml up -d
```

Compilar e rodar os testes:

```bash
./mvnw verify
```

Rodar o exemplo de producer (porta 8081) e publicar um pedido:

```bash
./mvnw -pl examples/producer-app -am -DskipTests package
```

```bash
java -jar examples/producer-app/target/producer-app-0.1.0-SNAPSHOT.jar
```

```bash
curl -X POST localhost:8081/orders -H 'Content-Type: application/json' -d '{"customerId": "c1", "totalAmount": 10}'
```

A resposta traz o tópico, a partição e o offset do evento. O registro aparece na Kafka UI, no tópico `orders.v1`.

## Estrutura

| Caminho | Conteúdo |
| --- | --- |
| `kafka-core/` | Código de infraestrutura Kafka compartilhado pelos exemplos |
| `examples/producer-app/` | Exemplo de produção de eventos por uma API REST |
| `infra/compose/` | Ambiente local com Docker Compose |
| `docs/adr/` | Decisões de arquitetura |
| `docs/features/` | Um arquivo por recurso do Kafka |

## Documentação

- [ADR 0001: repositório template em vez de starter](docs/adr/0001-template-em-vez-de-starter.md)
- [ADR 0002: envelope completo no valor, metadados repetidos nos headers](docs/adr/0002-envelope-no-valor-e-metadados-nos-headers.md)
- [ADR 0003: tópico de destino resolvido pelo tipo do evento](docs/adr/0003-roteamento-por-tipo-de-evento.md)
- [Ambiente local: Kafka em modo KRaft e Kafka UI](docs/features/ambiente-local.md)
- [Fundamentos: tópico, partição, offset e chave](docs/features/fundamentos.md)
- [Envelope de evento](docs/features/envelope-de-evento.md)
- [Producer: publicação de eventos, `acks` e idempotência](docs/features/producer.md)
- [Tópicos declarativos](docs/features/topicos-declarativos.md)
