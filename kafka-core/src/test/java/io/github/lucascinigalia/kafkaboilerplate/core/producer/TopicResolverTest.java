package io.github.lucascinigalia.kafkaboilerplate.core.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import io.github.lucascinigalia.kafkaboilerplate.core.config.BoilerplateKafkaProperties.CleanupPolicy;
import io.github.lucascinigalia.kafkaboilerplate.core.config.BoilerplateKafkaProperties.Topic;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TopicResolverTest {

    @Test
    void resolvesTopicFromEventType() {
        TopicResolver resolver = new TopicResolver(
                List.of(topic("orders.v1", "OrderCreated", "OrderCancelled"), topic("payments.v1", "PaymentTaken")));

        assertThat(resolver.topicFor("OrderCancelled")).isEqualTo("orders.v1");
        assertThat(resolver.topicFor("PaymentTaken")).isEqualTo("payments.v1");
    }

    @Test
    void rejectsEventTypeThatNoTopicDeclares() {
        TopicResolver resolver = new TopicResolver(List.of(topic("orders.v1", "OrderCreated")));

        assertThatIllegalArgumentException()
                .isThrownBy(() -> resolver.topicFor("Unknown"))
                .withMessageContaining("Unknown");
    }

    @Test
    void rejectsEventTypeDeclaredByTwoTopics() {
        assertThatIllegalStateException()
                .isThrownBy(() ->
                        new TopicResolver(List.of(topic("orders.v1", "OrderCreated"), topic("audit.v1", "OrderCreated"))))
                .withMessageContaining("OrderCreated");
    }

    private static Topic topic(String name, String... eventTypes) {
        return new Topic(name, 1, (short) 1, null, CleanupPolicy.DELETE, Set.of(eventTypes), Map.of());
    }
}
