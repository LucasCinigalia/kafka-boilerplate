package io.github.lucascinigalia.kafkaboilerplate.core.config;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.lucascinigalia.kafkaboilerplate.core.config.BoilerplateKafkaProperties.Acks;
import io.github.lucascinigalia.kafkaboilerplate.core.config.BoilerplateKafkaProperties.Compression;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.validation.autoconfigure.ValidationAutoConfiguration;
import org.springframework.context.annotation.Configuration;

/** Binding and validation only; no broker involved. */
class BoilerplateKafkaPropertiesTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ValidationAutoConfiguration.class))
            .withUserConfiguration(PropertiesOnly.class)
            .withPropertyValues("boilerplate.kafka.bootstrap-servers=localhost:9092", "boilerplate.kafka.source=test-app");

    @Test
    void defaultsAreTheSafeOnes() {
        runner.run(context -> {
            BoilerplateKafkaProperties properties = context.getBean(BoilerplateKafkaProperties.class);

            assertThat(properties.producer().acks()).isEqualTo(Acks.ALL);
            assertThat(properties.producer().idempotence()).isTrue();
            assertThat(properties.producer().compression()).isEqualTo(Compression.LZ4);
            assertThat(properties.producer().linger()).isEqualTo(Duration.ofMillis(5));
            assertThat(properties.admin().createTopics()).isTrue();
            assertThat(properties.topics()).isEmpty();
        });
    }

    @Test
    void weakerAcksWithIdempotenceStillOnIsRejected() {
        runner.withPropertyValues("boilerplate.kafka.producer.acks=leader")
                .run(context -> assertThat(context).hasFailed().getFailure().rootCause().hasMessageContaining("idempotence"));
    }

    @Test
    void weakerAcksIsAcceptedWhenIdempotenceIsTurnedOffExplicitly() {
        runner.withPropertyValues("boilerplate.kafka.producer.acks=leader", "boilerplate.kafka.producer.idempotence=false")
                .run(context -> assertThat(context.getBean(BoilerplateKafkaProperties.class).producer().acks())
                        .isEqualTo(Acks.LEADER));
    }

    @Test
    void topicWithoutPartitionsAndReplicasIsRejected() {
        runner.withPropertyValues("boilerplate.kafka.topics.orders.name=orders.v1")
                .run(context -> assertThat(context)
                        .hasFailed()
                        .getFailure()
                        .rootCause()
                        .hasMessageContaining("partitions")
                        .hasMessageContaining("replicas"));
    }

    @Test
    void missingBootstrapServersIsRejected() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ValidationAutoConfiguration.class))
                .withUserConfiguration(PropertiesOnly.class)
                .withPropertyValues("boilerplate.kafka.source=test-app")
                .run(context -> assertThat(context).hasFailed().getFailure().rootCause().hasMessageContaining("bootstrapServers"));
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(BoilerplateKafkaProperties.class)
    static class PropertiesOnly {}
}
