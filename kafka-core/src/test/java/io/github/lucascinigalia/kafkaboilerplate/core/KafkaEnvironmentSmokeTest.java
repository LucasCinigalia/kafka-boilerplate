package io.github.lucascinigalia.kafkaboilerplate.core;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.lucascinigalia.kafkaboilerplate.core.support.KafkaTestContainer;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.Test;

/** Proves the test infrastructure works: a real Kafka broker (KRaft) starts in a container. */
class KafkaEnvironmentSmokeTest {

    @Test
    void brokerStartsAndAnswersAdminRequests() throws Exception {
        Map<String, Object> config =
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaTestContainer.bootstrapServers());

        try (Admin admin = Admin.create(config)) {
            assertThat(admin.describeCluster().nodes().get(30, TimeUnit.SECONDS)).hasSize(1);
        }
    }
}
