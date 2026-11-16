package io.github.lucascinigalia.kafkaboilerplate.core.producer;

import io.github.lucascinigalia.kafkaboilerplate.core.config.BoilerplateKafkaProperties.Topic;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/** Finds the destination topic of an event type, from the declared topics. */
public class TopicResolver {

    private final Map<String, String> topicByEventType;

    public TopicResolver(Collection<Topic> topics) {
        Map<String, String> routes = new HashMap<>();
        for (Topic topic : topics) {
            for (String eventType : topic.eventTypes()) {
                String previous = routes.put(eventType, topic.name());
                if (previous != null && !previous.equals(topic.name())) {
                    throw new IllegalStateException("Event type '%s' is mapped to more than one topic: %s and %s"
                            .formatted(eventType, previous, topic.name()));
                }
            }
        }
        this.topicByEventType = Map.copyOf(routes);
    }

    public String topicFor(String eventType) {
        String topic = topicByEventType.get(eventType);
        if (topic == null) {
            throw new IllegalArgumentException(
                    "No topic declares event type '%s' under boilerplate.kafka.topics.*.event-types"
                            .formatted(eventType));
        }
        return topic;
    }
}
