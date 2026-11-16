# Fundamentos: tópico, partição, offset e chave

## O que é

- **Tópico:** um log nomeado onde os eventos são gravados em sequência e nunca alterados. Ler não remove nada; cada consumidor guarda a própria posição.
- **Partição:** o tópico é dividido em partições, e cada uma é um log independente. É a partição que permite escalar: partições diferentes ficam em brokers diferentes e são lidas em paralelo.
- **Offset:** a posição de um registro dentro de uma partição (0, 1, 2...). O par partição + offset identifica um registro.
- **Chave:** o producer calcula o hash da chave para escolher a partição. A mesma chave sempre cai na mesma partição enquanto o número de partições não mudar.
- **Ordenação:** o Kafka só garante ordem **dentro de uma partição**. Não há ordem entre partições.

Juntando tudo: eventos com a mesma chave ficam na mesma partição e, por isso, em ordem.

## Quando usar e quando não usar

- **Escolha como chave o identificador da entidade cuja ordem importa.** No exemplo, a chave é o `customerId`: todos os pedidos de um cliente ficam em ordem.
- **Não use uma chave com poucos valores ou muito desbalanceada** (um status, um país dominante). Quase tudo cai na mesma partição e ela vira gargalo.
- **Não conte com ordem global.** Se o sistema precisa de ordem total entre todos os eventos, só um tópico com uma partição resolve, e ele não escala.

## Como ativar e configurar no boilerplate

A chave é o segundo argumento da porta de publicação:

```java
publisher.publish(event, customerId);
```

O número de partições é declarado por tópico e não tem valor padrão (veja [tópicos declarativos](topicos-declarativos.md)).

## Armadilhas comuns

- **Aumentar partições quebra a ordenação por chave.** O hash passa a ser dividido por outro número e a mesma chave vai para outra partição. Eventos antigos e novos da mesma entidade ficam em partições diferentes. Defina o número pensando no futuro.
- **Não dá para diminuir partições.** Só recriando o tópico.
- **Partição quente.** Uma chave muito mais frequente que as outras sobrecarrega uma partição.
- **Offset não é contínuo para quem lê.** Compactação e transações deixam buracos; não use offset como contador.

## Como foi testado

[`KafkaEventPublisherIntegrationTest`](../../kafka-core/src/test/java/io/github/lucascinigalia/kafkaboilerplate/core/producer/KafkaEventPublisherIntegrationTest.java), teste `eventsWithTheSameKeyGoToTheSamePartitionInPublishOrder`: publica 20 eventos com duas chaves num tópico de 3 partições e confere que cada chave ficou numa partição só, com offsets e conteúdo em ordem.
