# Tópicos declarativos

## O que é

Os tópicos da aplicação são descritos em propriedades e criados na subida, pelo `KafkaAdmin` do Spring. O broker do ambiente local não cria tópicos sozinho (`auto.create.topics.enable=false`), então um tópico só existe se foi declarado.

## Quando usar e quando não usar

- **Usar** em desenvolvimento, em testes e em ambientes em que a aplicação é dona dos tópicos.
- **Não usar** quando os tópicos são gerenciados por fora, por exemplo como CRDs `KafkaTopic` do Strimzi (Etapa 9) ou por Terraform. Nesse caso desligue com `boilerplate.kafka.admin.create-topics=false` e mantenha a declaração só para o roteamento dos eventos.

## Como ativar e configurar no boilerplate

```yaml
boilerplate:
  kafka:
    topics:
      orders:                       # apelido, usado só na configuração
        name: orders.v1             # nome real no broker
        partitions: 3
        replicas: 3
        retention: 7d
        event-types: [OrderCreated]
      customer-state:
        name: customer-state.v1
        partitions: 3
        replicas: 3
        cleanup-policy: compact
```

| Propriedade (`boilerplate.kafka.topics.<apelido>.`) | Padrão | Para que serve |
| --- | --- | --- |
| `name` | obrigatória | Nome do tópico no broker |
| `partitions` | obrigatória | Número de partições |
| `replicas` | obrigatória | Fator de replicação |
| `retention` | padrão do broker (7 dias) | Por quanto tempo os registros ficam guardados |
| `cleanup-policy` | `delete` | `delete` apaga registros antigos; `compact` mantém o último de cada chave |
| `event-types` | vazio | Tipos de evento publicados neste tópico |
| `config.*` | vazio | Qualquer configuração de tópico do Kafka |

| Propriedade (`boilerplate.kafka.admin.`) | Padrão | Para que serve |
| --- | --- | --- |
| `create-topics` | `true` | Cria os tópicos declarados na subida |
| `fail-fast` | `true` | Impede a aplicação de subir se o broker não responder |
| `operation-timeout` | `30s` | Tempo limite das operações administrativas |

`partitions` e `replicas` não têm valor padrão de propósito: o primeiro é difícil de mudar depois, e o segundo decide quantos brokers podem cair sem perda.

## Armadilhas comuns

- **Alterar a declaração não altera um tópico que já existe.** O `KafkaAdmin` cria o que falta e consegue aumentar partições, mas não muda retenção nem outras configurações de um tópico existente.
- **Aumentar `partitions` é aplicado e quebra a ordenação por chave.** Veja [fundamentos](fundamentos.md).
- **`replicas` maior que o número de brokers** faz a criação falhar. No ambiente local, com um broker, só funciona 1.
- **Várias instâncias subindo juntas** tentam criar o mesmo tópico. Não há problema: quem perde a corrida recebe "já existe" e segue.

## Como foi testado

[`KafkaEventPublisherIntegrationTest`](../../kafka-core/src/test/java/io/github/lucascinigalia/kafkaboilerplate/core/producer/KafkaEventPublisherIntegrationTest.java), teste `declaredTopicsAreCreatedWithTheConfiguredSettings`: confere no broker as partições, a retenção e a política de limpeza dos tópicos declarados. [`BoilerplateKafkaPropertiesTest`](../../kafka-core/src/test/java/io/github/lucascinigalia/kafkaboilerplate/core/config/BoilerplateKafkaPropertiesTest.java) confere que um tópico sem `partitions` e `replicas` é rejeitado.
