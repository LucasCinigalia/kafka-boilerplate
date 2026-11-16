package io.github.lucascinigalia.kafkaboilerplate.core.support;

import org.testcontainers.kafka.KafkaContainer;

/** One real Kafka broker (KRaft) shared by every integration test of the module. */
public final class KafkaTestContainer {

    private static final String IMAGE = "apache/kafka:4.2.2";

    private static final KafkaContainer KAFKA = new KafkaContainer(IMAGE);

    static {
        KAFKA.start();
    }

    private KafkaTestContainer() {}

    public static String bootstrapServers() {
        return KAFKA.getBootstrapServers();
    }
}
