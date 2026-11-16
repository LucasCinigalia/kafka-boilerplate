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

## Estrutura

| Caminho | Conteúdo |
| --- | --- |
| `kafka-core/` | Código de infraestrutura Kafka compartilhado pelos exemplos |
| `examples/` | Aplicações de exemplo, uma por grupo de recursos (a partir da Etapa 1) |
| `infra/compose/` | Ambiente local com Docker Compose |
| `docs/adr/` | Decisões de arquitetura |
| `docs/features/` | Um arquivo por recurso do Kafka |

## Documentação

- [ADR 0001: repositório template em vez de starter](docs/adr/0001-template-em-vez-de-starter.md)
- [Ambiente local: Kafka em modo KRaft e Kafka UI](docs/features/ambiente-local.md)
