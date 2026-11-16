package io.github.lucascinigalia.kafkaboilerplate.core.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.lucascinigalia.kafkaboilerplate.core.config.KafkaCoreConfiguration;
import io.github.lucascinigalia.kafkaboilerplate.core.event.EventEnvelope;
import io.github.lucascinigalia.kafkaboilerplate.core.event.EventFactory;
import io.github.lucascinigalia.kafkaboilerplate.core.event.EventPublishException;
import io.github.lucascinigalia.kafkaboilerplate.core.event.EventPublisher;
import io.github.lucascinigalia.kafkaboilerplate.core.support.SamplePayload;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.validation.autoconfigure.ValidationAutoConfiguration;

/** What the caller sees when no broker answers. Points to a port where nothing listens. */
class KafkaEventPublisherFailureTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(JacksonAutoConfiguration.class, ValidationAutoConfiguration.class))
            .withUserConfiguration(KafkaCoreConfiguration.class)
            .withPropertyValues(
                    "boilerplate.kafka.bootstrap-servers=localhost:1",
                    "boilerplate.kafka.source=test-app",
                    "boilerplate.kafka.admin.create-topics=false",
                    "boilerplate.kafka.producer.properties[max.block.ms]=500",
                    "boilerplate.kafka.producer.send-timeout=5s",
                    "boilerplate.kafka.topics.samples.name=samples.v1",
                    "boilerplate.kafka.topics.samples.partitions=1",
                    "boilerplate.kafka.topics.samples.replicas=1",
                    "boilerplate.kafka.topics.samples.event-types=SampleCreated");

    @Test
    void asyncPublishFailsTheFutureInsteadOfThrowing() {
        runner.run(context -> {
            EventEnvelope<SamplePayload> event =
                    context.getBean(EventFactory.class).create("SampleCreated", 1, new SamplePayload("x", 1));

            assertThat(context.getBean(EventPublisher.class).publish(event, "key"))
                    .failsWithin(Duration.ofSeconds(10))
                    .withThrowableOfType(java.util.concurrent.ExecutionException.class)
                    .withCauseInstanceOf(EventPublishException.class);
        });
    }

    @Test
    void publishAndWaitThrowsEventPublishException() {
        runner.run(context -> {
            EventEnvelope<SamplePayload> event =
                    context.getBean(EventFactory.class).create("SampleCreated", 1, new SamplePayload("x", 1));

            assertThatThrownBy(() -> context.getBean(EventPublisher.class).publishAndWait(event, "key"))
                    .isInstanceOf(EventPublishException.class)
                    .hasMessageContaining(event.eventId().toString());
        });
    }
}
