package io.github.lucascinigalia.kafkaboilerplate.core.config;

import io.github.lucascinigalia.kafkaboilerplate.core.config.BoilerplateKafkaProperties.Producer;
import io.github.lucascinigalia.kafkaboilerplate.core.event.EventFactory;
import io.github.lucascinigalia.kafkaboilerplate.core.event.EventPublisher;
import io.github.lucascinigalia.kafkaboilerplate.core.producer.KafkaEventPublisher;
import io.github.lucascinigalia.kafkaboilerplate.core.producer.TopicResolver;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import tools.jackson.databind.json.JsonMapper;

/** Producer side: factory with safe defaults, template and the {@link EventPublisher} adapter. */
@Configuration(proxyBeanMethods = false)
public class KafkaProducerConfiguration {

    @Bean
    ProducerFactory<String, Object> eventProducerFactory(BoilerplateKafkaProperties properties, JsonMapper jsonMapper) {
        Producer producer = properties.producer();
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers());
        config.put(ProducerConfig.CLIENT_ID_CONFIG, properties.source());
        config.put(ProducerConfig.ACKS_CONFIG, producer.acks().kafkaValue());
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, producer.idempotence());
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, producer.compression().kafkaValue());
        config.put(ProducerConfig.LINGER_MS_CONFIG, producer.linger().toMillis());
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, Math.toIntExact(producer.batchSize().toBytes()));
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Math.toIntExact(producer.deliveryTimeout().toMillis()));
        config.putAll(producer.properties());

        // No type headers: the Java class name of the payload is not part of the contract.
        JacksonJsonSerializer<Object> valueSerializer = new JacksonJsonSerializer<>(jsonMapper);
        valueSerializer.setAddTypeInfo(false);
        return new DefaultKafkaProducerFactory<>(config, new StringSerializer(), valueSerializer);
    }

    @Bean
    KafkaTemplate<String, Object> eventKafkaTemplate(ProducerFactory<String, Object> eventProducerFactory) {
        return new KafkaTemplate<>(eventProducerFactory);
    }

    @Bean
    TopicResolver topicResolver(BoilerplateKafkaProperties properties) {
        return new TopicResolver(properties.topics().values());
    }

    @Bean
    EventPublisher eventPublisher(
            KafkaTemplate<String, Object> eventKafkaTemplate,
            TopicResolver topicResolver,
            BoilerplateKafkaProperties properties) {
        return new KafkaEventPublisher(
                eventKafkaTemplate, topicResolver, properties.producer().sendTimeout());
    }

    @Bean
    EventFactory eventFactory(BoilerplateKafkaProperties properties) {
        return new EventFactory(properties.source(), Clock.systemUTC());
    }
}
