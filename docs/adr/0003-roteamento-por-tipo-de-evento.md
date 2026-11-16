# ADR 0003: tópico de destino resolvido pelo tipo do evento

- **Status:** aceita
- **Data:** 2026-11-16

## Contexto

O código de negócio publica eventos pela porta `EventPublisher` e não deve conhecer Kafka. Era preciso decidir como ele indica o destino.

| Opção | Vantagem | Desvantagem |
| --- | --- | --- |
| Passar o nome do tópico (`publish(topic, key, event)`) | Simples e explícito | Nome de tópico espalhado pelo código de negócio; trocar o tópico exige mudar código |
| Passar um apelido lógico do tópico | Nome real fica na configuração | O código de negócio ainda pensa em tópicos |
| Resolver pelo tipo do evento | O código de negócio só diz o que aconteceu; o destino é configuração | Um nível a mais de indireção; cada tipo vai para um tópico só |

## Decisão

Resolver o tópico **pelo tipo do evento**. Cada tópico declarado lista os tipos que recebe:

```yaml
boilerplate:
  kafka:
    topics:
      orders:
        name: orders.v1
        partitions: 3
        replicas: 3
        event-types: [OrderCreated, OrderCancelled]
```

A porta fica `publish(event, key)`. A chave é obrigatória, porque é ela que define a ordenação.

## Consequências

- Publicar um tipo que nenhum tópico declara falha na hora, com mensagem clara, em vez de ir para um tópico errado.
- Um tipo declarado em dois tópicos impede a aplicação de subir.
- Publicar o mesmo evento em dois tópicos não é suportado. Se um projeto precisar disso, o lugar para mudar é o `TopicResolver`.
- Colocar vários tipos no mesmo tópico é o que permite manter a ordem entre eles (por exemplo, `OrderCreated` antes de `OrderCancelled` do mesmo pedido).
