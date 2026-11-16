package io.github.lucascinigalia.kafkaboilerplate.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Proves the test infrastructure works: a real Kafka broker (KRaft) starts in a container
 * and accepts admin operations. Later stages build their integration tests on the same setup.
 */
@Testcontainers
class KafkaEnvironmentSmokeTest {

    private static final String KAFKA_IMAGE = "apache/kafka:4.2.2";
    private static final String TOPIC = "smoke-test";
    private static final int PARTITIONS = 3;

    @Container
    static final KafkaContainer kafka = new KafkaContainer(KAFKA_IMAGE);

    @Test
    void brokerStartsAndCreatesTopicWithRequestedPartitions() throws Exception {
        Map<String, Object> config = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

        try (Admin admin = Admin.create(config)) {
            admin.createTopics(List.of(new NewTopic(TOPIC, PARTITIONS, (short) 1))).all().get(30, TimeUnit.SECONDS);

            TopicDescription description =
                    admin.describeTopics(List.of(TOPIC)).allTopicNames().get(30, TimeUnit.SECONDS).get(TOPIC);

            assertThat(admin.describeCluster().nodes().get(30, TimeUnit.SECONDS)).hasSize(1);
            assertThat(description.partitions()).hasSize(PARTITIONS);
        }
    }
}
