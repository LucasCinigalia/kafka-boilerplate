# kafka-boilerplate: contexto do projeto

Este arquivo dá o contexto completo do projeto. Leia tudo antes de começar e siga a seção "Como trabalhar comigo".

## Quem sou e por que este projeto existe

Sou Lucas Cinigalia Francisco, desenvolvedor backend em Florianópolis (SC).

- **Trabalho atual:** Stratos Consultoria desde jan/2026, com integrações REST, SQL entre vários sistemas e IA aplicada.
- **Formação:** Análise e Desenvolvimento de Sistemas no Senac, com conclusão prevista para jun/2027.
- **Stack:** Java/Spring é a mais forte; também uso Python, React e SQL.
- **GitHub:** github.com/LucasCinigalia. O projeto mais completo hoje é o LimpAção (Java 21, Spring Boot, Spring Security com JWT, JPA, MySQL, WebSocket/STOMP).

**Objetivo:** aprender, na prática, Kafka e Kubernetes, que aparecem na maioria das vagas de backend Java. Numa análise de 8 vagas de ago a nov/2026, mensageria aparece em 7 e Kubernetes em 6. A vaga que motivou o estudo é a de Desenvolvedor(a) Back-end Java da VAAS (Florianópolis). Ela pede Kafka ou Pub/Sub, idempotência, resiliência (retry, circuit breaker), observabilidade, testes no CI, Kubernetes (GKE), GitHub Actions e Argo CD.

## O que é o projeto

Um **boilerplate de Kafka com Spring Boot**, feito para ser reaproveitado. Quando eu precisar de Kafka num projeto novo, quero usar este em vez de começar do zero, mesmo que o projeto tenha requisitos bem diferentes.

Por isso o projeto:

- **Não tem regra de negócio.** Os exemplos usam eventos genéricos (por exemplo, `PedidoCriado`) só para demonstrar cada recurso.
- **Cobre o Kafka de forma ampla.** Cada recurso fica isolado, documentado e testado.
- **Tem defaults seguros e configuráveis.** Tudo pode ser ligado, desligado ou ajustado por propriedades.
- **Prioriza código limpo.** O código de negócio de quem usa o boilerplate não deve depender de classes do Kafka.

## Arquitetura do repositório

**Decisão (ADR 0001):** o projeto é um **repositório template**, copiado inteiro para cada projeto novo. Não é um starter publicado como dependência. Detalhes em `docs/adr/0001-template-em-vez-de-starter.md`.

Projeto Maven multi-módulo: um módulo com o código Kafka compartilhado e aplicações de exemplo que o usam.

```
kafka-boilerplate/
  CLAUDE.md
  README.md
  pom.xml                       pom pai: versões e plugins
  kafka-core/                   código de infraestrutura Kafka compartilhado (copiado junto, não publicado)
  kafka-streams-core/           código compartilhado de Kafka Streams (Etapa 7)
  examples/
    producer-app/               produção de eventos
    consumer-app/               consumo, erros, retry e DLT
    transactional-app/          exactly-once (ler, processar, escrever)
    outbox-app/                 padrão outbox com PostgreSQL
    request-reply-app/          request-reply sobre Kafka
    streams-app/                Kafka Streams
  infra/
    compose/                    Kafka (KRaft), Schema Registry, Kafka UI, PostgreSQL, observabilidade
    k8s/                        kind, Strimzi, chart Helm dos exemplos, KEDA
  docs/
    adr/                        decisões de arquitetura
    features/                   um arquivo por recurso do Kafka (ver "Padrão de documentação")
    guia-de-uso.md              como usar o boilerplate num projeto novo
  .github/workflows/            CI
```

Os módulos e pastas são criados na etapa em que passam a ser usados.

### O que o kafka-core oferece

- **Envelope de evento padrão:** `eventId`, `type`, `version`, `occurredAt`, `correlationId`, `causationId`, `source` e `payload`. Vai inteiro em JSON no valor, com os metadados repetidos nos headers (ADR 0002).
- **Porta de publicação:** uma interface `EventPublisher` que o código de negócio usa sem conhecer Kafka. A implementação Kafka fica no `kafka-core`. O tópico é resolvido pelo tipo do evento (ADR 0003).
- **Producer com defaults seguros:** idempotência ligada, `acks=all`, compressão e batching configuráveis.
- **Consumer com tratamento de erro pronto:** `ErrorHandlingDeserializer`, `DefaultErrorHandler` com backoff exponencial, exceções não-retentáveis configuráveis e envio para DLT.
- **Consumo idempotente opcional:** interface de deduplicação por `eventId`, com implementação em memória e em banco.
- **Tópicos declarativos:** criação por propriedades (partições, réplicas, retenção, compactação).
- **Serialização plugável:** JSON por padrão e Avro com Schema Registry como opção.
- **Propagação de contexto:** correlation id e trace nos headers.
- **Observabilidade:** métricas Micrometer e health check.
- **Configuração tipada e validada:** tudo sob `boilerplate.kafka.*`, com `@ConfigurationProperties` e Bean Validation.

## Recursos do Kafka que o projeto deve cobrir

| Área | Recursos |
| --- | --- |
| Fundamentos | tópicos, partições, offsets, chaves e ordenação, consumer groups, rebalance |
| Producer | envio síncrono e assíncrono, callbacks, headers, particionador customizado, idempotência, `acks`, `linger.ms`, `batch.size`, compressão |
| Consumer | commit automático e manual (`AckMode`), concorrência, batch listener, filtros, seek e replay, pause e resume, listener de rebalance |
| Erros | poison pill, retry bloqueante com backoff, retry não-bloqueante (`@RetryableTopic`), DLT e reprocessamento da DLT |
| Confiabilidade | consumidor idempotente, transações e exactly-once, padrão outbox |
| Schemas | Avro, Schema Registry, regras de compatibilidade e evolução de schema |
| Padrões | request-reply, tópico compactado como tabela de estado |
| Kafka Streams | operações sem estado, agregações, janelas, joins, state stores |
| Observabilidade | métricas, tracing de ponta a ponta, consumer lag, health |
| Segurança | SASL/SCRAM e TLS (opcional, via Strimzi no Kubernetes) |
| Kubernetes | Kafka no cluster com Strimzi, tópicos e usuários como CRDs, deploy dos exemplos com Helm, graceful shutdown, escala por lag com KEDA |

## Stack

| Parte | Tecnologia |
| --- | --- |
| Linguagem e framework | Java 21, Spring Boot 4.1.2 e Spring for Apache Kafka 4.1.2 (versão gerenciada pelo Spring Boot; kafka-clients 4.2.2) |
| Kafka local | Apache Kafka 4.2.2 em modo KRaft (sem ZooKeeper), Schema Registry, Kafka UI (kafbat) |
| Banco (só no outbox e na deduplicação) | PostgreSQL + Flyway |
| Testes | JUnit 5, AssertJ, Awaitility, Testcontainers (Kafka e PostgreSQL) |
| Observabilidade | Micrometer, Prometheus, Grafana, OpenTelemetry |
| Kubernetes | kind, Strimzi, Helm, KEDA |
| Entrega | Docker multi-stage, GitHub Actions, GHCR; Argo CD opcional |

Ao atualizar versões, use as estáveis mais recentes e deixe o parent do Spring Boot gerenciar as versões de Kafka e Testcontainers.

## Comandos

- Ambiente local: `docker compose -f infra/compose/compose.yaml up -d` (Kafka em `localhost:9092`, Kafka UI em http://localhost:8090)
- Build e testes: `./mvnw verify` (precisa de Docker por causa do Testcontainers)

## Etapas (fazer em ordem, uma de cada vez)

Cada etapa termina com testes passando, a documentação do recurso em `docs/features/` e um commit.

0. **Estrutura e ambiente.** Discutir e registrar a ADR 0001 (starter + exemplos, ou template). Criar o Maven multi-módulo e o Docker Compose com Kafka KRaft e Kafka UI.
   - *Pronto quando:* `docker compose up` sobe o Kafka e a Kafka UI mostra o cluster.
1. **Fundamentos e producer.** Envelope de evento, `EventPublisher`, tópicos declarativos, envio com chave, headers e callbacks, defaults de idempotência.
   - *Aprender:* tópico, partição, offset, chave e ordenação, `acks`, producer idempotente.
2. **Consumer.** Consumer group, concorrência, commit manual, listener de rebalance, filtros e batch listener. Testes com Testcontainers.
   - *Aprender:* como o rebalance funciona e por que o commit manual muda as garantias.
3. **Tratamento de erros.** Poison pill com `ErrorHandlingDeserializer`, retry bloqueante, retry não-bloqueante com `@RetryableTopic`, DLT e reprocessamento.
   - *Aprender:* quando usar retry bloqueante ou não-bloqueante, e o efeito de cada um na ordem das mensagens.
4. **Confiabilidade.** Consumidor idempotente, transações com exactly-once no fluxo ler-processar-escrever, e o módulo de outbox com PostgreSQL.
   - *Pronto quando:* testes provarem que uma mensagem duplicada é processada uma vez só, e que um erro no meio da transação não publica nada.
5. **Schemas.** Avro com Schema Registry, uma evolução compatível e uma incompatível, e teste que detecta a quebra.
6. **Padrões avançados.** Request-reply, tópico compactado, seek e replay a partir de um timestamp, pause e resume.
7. **Kafka Streams.** Módulo `kafka-streams-core` e um exemplo com agregação em janela, join e consulta ao state store.
8. **Observabilidade.** Métricas de producer e consumer, consumer lag, trace atravessando producer e consumer, e um painel no Grafana.
9. **Kubernetes.** Cluster local com kind e Kafka pelo operador Strimzi (cluster, `KafkaTopic` e `KafkaUser` como CRDs). Deploy dos exemplos com um chart Helm, probes, graceful shutdown dos consumers, PodDisruptionBudget e escala automática por consumer lag com KEDA.
   - *Aprender:* Pod, Deployment, Service, ConfigMap, Secret, operadores e CRDs, e o que acontece com um consumer quando o pod é reiniciado.
10. **CI/CD.** GitHub Actions com build, testes e publicação das imagens no GHCR. Opcional: Argo CD sincronizando o cluster.
11. **Segurança (opcional).** SASL/SCRAM e TLS configurados pelo Strimzi, com um profile Spring para ligar e desligar.

Ao fim, o `docs/guia-de-uso.md` deve permitir usar o boilerplate num projeto novo em poucos passos: copiar o template, manter o exemplo mais próximo, configurar as propriedades e implementar o handler.

## Padrão de documentação de cada recurso

Cada arquivo em `docs/features/` deve ter:

1. **O que é:** o conceito.
2. **Quando usar e quando não usar.**
3. **Como ativar e configurar no boilerplate:** propriedades e exemplo.
4. **Armadilhas comuns.**
5. **Como foi testado:** link para o teste.

## Padrões de código

- **Arquitetura hexagonal nos exemplos.** O domínio não importa nada de `org.apache.kafka` nem de `org.springframework.kafka`. Kafka fica nos adaptadores.
- **Nenhum valor fixo no código.** Nomes de tópicos, grupos e tempos de retry vêm de propriedades tipadas e validadas.
- **Defaults seguros.** Tudo que pode perder ou duplicar mensagem precisa ser uma escolha explícita.
- **Estilo:** records para eventos e DTOs, injeção por construtor, classes pequenas, nomes claros e logs estruturados com `correlationId`.
- **Testes de integração reais.** Usar Testcontainers com Kafka de verdade, não mocks do Kafka. Usar Awaitility em vez de `Thread.sleep`.
- **Configuração explícita no `kafka-core`.** Classes `@Configuration` comuns, importadas pelos exemplos, sem auto-configuration. Recursos opcionais são ligados por propriedade (`@ConditionalOnProperty`).
- **Pacote base:** `io.github.lucascinigalia.kafkaboilerplate`.
- **Idioma:** código em inglês; documentação, ADRs e commits em português.

## Como trabalhar comigo

- **Quero aprender, não só ter o código pronto.** Antes de cada etapa, explique em poucas linhas os conceitos novos e o que vamos construir.
- **Passos pequenos.** Uma etapa por vez, com commits pequenos e mensagens claras. Não pule etapas sem eu pedir.
- **Explique as decisões.** Quando houver escolha de design, apresente as opções e registre a decisão em `docs/adr/`.
- **Testes fazem parte da etapa.** Rode a suíte antes de dizer que terminou.
- **Ao fim de cada etapa:** resuma o que foi feito, o que aprendi e o que vem a seguir, e marque a etapa como concluída neste arquivo.
- **Commits só com o meu usuário.** Autor `Lucas Cinigalia Francisco`, sem linhas de coautoria (`Co-Authored-By`) nem qualquer menção a ferramentas de IA em commits, títulos ou descrições de pull request.
- **Nunca fazer push direto na `main`.** Cada entrega vai para uma branch e vira um pull request. Eu faço o merge pelo site com "Squash and merge", para que o commit da `main` seja criado pelo GitHub, comigo como autor e marcado como Verified. O título do pull request vira a mensagem desse commit, então deve seguir o padrão dos commits (em português, por exemplo `feat: ...`).

## Status

- [x] Etapa 0: Estrutura e ambiente
- [x] Etapa 1: Fundamentos e producer
- [ ] Etapa 2: Consumer
- [ ] Etapa 3: Tratamento de erros
- [ ] Etapa 4: Confiabilidade
- [ ] Etapa 5: Schemas
- [ ] Etapa 6: Padrões avançados
- [ ] Etapa 7: Kafka Streams
- [ ] Etapa 8: Observabilidade
- [ ] Etapa 9: Kubernetes
- [ ] Etapa 10: CI/CD
- [ ] Etapa 11: Segurança (opcional)

**Próximo passo:** começar a Etapa 2 (consumer).
