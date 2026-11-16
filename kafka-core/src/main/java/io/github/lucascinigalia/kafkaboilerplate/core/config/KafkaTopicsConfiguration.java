package io.github.lucascinigalia.kafkaboilerplate.core.config;

import io.github.lucascinigalia.kafkaboilerplate.core.config.BoilerplateKafkaProperties.Topic;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/** Declarative topics: what is listed under {@code boilerplate.kafka.topics} is created at startup. */
@Configuration(proxyBeanMethods = false)
public class KafkaTopicsConfiguration {

    @Bean
    KafkaAdmin kafkaAdmin(BoilerplateKafkaProperties properties) {
        KafkaAdmin admin = new KafkaAdmin(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers()));
        admin.setAutoCreate(properties.admin().createTopics());
        admin.setFatalIfBrokerNotAvailable(properties.admin().failFast());
        admin.setOperationTimeout(Math.toIntExact(properties.admin().operationTimeout().toSeconds()));
        return admin;
    }

    @Bean
    KafkaAdmin.NewTopics declaredTopics(BoilerplateKafkaProperties properties) {
        return new KafkaAdmin.NewTopics(properties.topics().values().stream()
                .map(KafkaTopicsConfiguration::toNewTopic)
                .toArray(NewTopic[]::new));
    }

    private static NewTopic toNewTopic(Topic topic) {
        TopicBuilder builder = TopicBuilder.name(topic.name())
                .partitions(topic.partitions())
                .replicas(topic.replicas())
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, topic.cleanupPolicy().kafkaValue());
        if (topic.retention() != null) {
            builder.config(TopicConfig.RETENTION_MS_CONFIG, Long.toString(topic.retention().toMillis()));
        }
        topic.config().forEach(builder::config);
        return builder.build();
    }
}
