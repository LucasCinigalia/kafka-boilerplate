# ADR 0001: repositório template em vez de starter

- **Status:** aceita
- **Data:** 2026-11-16

## Contexto

O objetivo do projeto é reaproveitar uma base de Kafka com Spring Boot em projetos novos, mesmo com requisitos bem diferentes. Havia duas formas de fazer isso:

1. **Starter + exemplos:** uma biblioteca com auto-configuration, usada por dependência Maven.
2. **Repositório template:** um projeto comum, copiado inteiro para cada projeto novo.

| | Starter + exemplos | Repositório template |
| --- | --- | --- |
| Reuso | Adicionar a dependência e configurar propriedades | Copiar o repositório e apagar o que não usa |
| Evolução | Corrige num lugar só; os projetos sobem a versão | Cada cópia diverge; correções não se propagam |
| Customização | Por propriedades e substituição de beans | Total: basta editar o código copiado |
| Custo inicial | Maior: auto-configuration, publicação do artefato | Baixo: é uma aplicação Spring comum |
| Distribuição | Precisa publicar o artefato | Nenhuma |
| Visibilidade do Kafka | Parte da configuração fica escondida na biblioteca | Toda a configuração fica à vista no projeto |

## Decisão

Usar o **repositório template**.

O repositório continua sendo um Maven multi-módulo, com esta divisão:

- `kafka-core/`: código de infraestrutura Kafka compartilhado (envelope de evento, `EventPublisher`, configuração de producer e consumer, tratamento de erros). É código-fonte copiado junto com o template, não um artefato publicado.
- `examples/*`: uma aplicação por grupo de recursos. Cada uma depende de `kafka-core` e serve de ponto de partida para o projeto novo.

Para usar num projeto novo: copiar o repositório, manter `kafka-core` e o exemplo mais próximo do caso de uso, apagar os outros exemplos e renomear o pacote base.

## Consequências

- **Positivas:** começo mais simples; todo o código Kafka fica visível e editável, o que ajuda no aprendizado; não há artefato para publicar nem versão para gerenciar.
- **Negativas:** cada projeto criado a partir do template vira uma cópia independente. Uma correção feita aqui precisa ser levada à mão para os projetos que já copiaram.
- **O que muda no plano original:**
  - Não há `kafka-starter` nem `kafka-streams-starter`; o código compartilhado fica em `kafka-core` (e em `kafka-streams-core`, na Etapa 7).
  - Não há auto-configuration (`AutoConfiguration.imports`). As classes de configuração são `@Configuration` comuns, importadas pelos exemplos.
  - Continuam valendo: propriedades tipadas e validadas sob `boilerplate.kafka.*`, recursos ligados e desligados por propriedade, e domínio sem dependência de classes do Kafka.
- **Caminho de volta:** como o código compartilhado já fica isolado em `kafka-core`, transformá-lo num starter depois exige só adicionar a auto-configuration e publicar o módulo.
