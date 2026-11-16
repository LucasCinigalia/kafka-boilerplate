package io.github.lucascinigalia.kafkaboilerplate.core.producer;

import io.github.lucascinigalia.kafkaboilerplate.core.event.EventEnvelope;
import io.github.lucascinigalia.kafkaboilerplate.core.event.EventHeaders;
import io.github.lucascinigalia.kafkaboilerplate.core.event.EventPublishException;
import io.github.lucascinigalia.kafkaboilerplate.core.event.EventPublisher;
import io.github.lucascinigalia.kafkaboilerplate.core.event.PublishResult;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

/** Kafka adapter of {@link EventPublisher}. */
public class KafkaEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TopicResolver topicResolver;
    private final Duration sendTimeout;

    public KafkaEventPublisher(
            KafkaTemplate<String, Object> kafkaTemplate, TopicResolver topicResolver, Duration sendTimeout) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicResolver = topicResolver;
        this.sendTimeout = sendTimeout;
    }

    @Override
    public CompletableFuture<PublishResult> publish(EventEnvelope<?> event, String key) {
        Objects.requireNonNull(event, "event is required");
        Objects.requireNonNull(key, "key is required");
        ProducerRecord<String, Object> record = toRecord(event, key);

        CompletableFuture<PublishResult> result = new CompletableFuture<>();
        try {
            // send() only puts the record in the producer buffer; the callback runs when the broker answers.
            kafkaTemplate.send(record).whenComplete((sendResult, failure) -> {
                if (failure != null) {
                    result.completeExceptionally(failed(event, record.topic(), failure));
                } else {
                    result.complete(confirmed(event, sendResult.getRecordMetadata()));
                }
            });
        } catch (RuntimeException failure) {
            // Serialization errors and metadata timeouts are thrown by send() itself.
            result.completeExceptionally(failed(event, record.topic(), failure));
        }
        return result;
    }

    @Override
    public PublishResult publishAndWait(EventEnvelope<?> event, String key) {
        try {
            return publish(event, key).get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof EventPublishException publishException) {
                throw publishException;
            }
            throw new EventPublishException("Failed to publish event " + event.eventId(), e.getCause());
        } catch (TimeoutException e) {
            throw new EventPublishException(
                    "Event %s was not confirmed within %s".formatted(event.eventId(), sendTimeout), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EventPublishException("Interrupted while publishing event " + event.eventId(), e);
        }
    }

    private ProducerRecord<String, Object> toRecord(EventEnvelope<?> event, String key) {
        String topic = topicResolver.topicFor(event.type());
        // partition = null: the partitioner chooses it from the hash of the key.
        ProducerRecord<String, Object> record =
                new ProducerRecord<>(topic, null, event.occurredAt().toEpochMilli(), key, event);
        Headers headers = record.headers();
        addHeader(headers, EventHeaders.EVENT_ID, event.eventId().toString());
        addHeader(headers, EventHeaders.EVENT_TYPE, event.type());
        addHeader(headers, EventHeaders.EVENT_VERSION, Integer.toString(event.version()));
        addHeader(headers, EventHeaders.CORRELATION_ID, event.correlationId());
        addHeader(headers, EventHeaders.SOURCE, event.source());
        if (event.causationId() != null) {
            addHeader(headers, EventHeaders.CAUSATION_ID, event.causationId().toString());
        }
        return record;
    }

    private static void addHeader(Headers headers, String name, String value) {
        headers.add(name, value.getBytes(StandardCharsets.UTF_8));
    }

    private PublishResult confirmed(EventEnvelope<?> event, RecordMetadata metadata) {
        log.atInfo()
                .addKeyValue("correlationId", event.correlationId())
                .addKeyValue("eventId", event.eventId())
                .addKeyValue("eventType", event.type())
                .addKeyValue("topic", metadata.topic())
                .addKeyValue("partition", metadata.partition())
                .addKeyValue("offset", metadata.offset())
                .log("Event published");
        return new PublishResult(event.eventId(), metadata.topic(), metadata.partition(), metadata.offset());
    }

    private EventPublishException failed(EventEnvelope<?> event, String topic, Throwable failure) {
        log.atError()
                .addKeyValue("correlationId", event.correlationId())
                .addKeyValue("eventId", event.eventId())
                .addKeyValue("eventType", event.type())
                .addKeyValue("topic", topic)
                .setCause(failure)
                .log("Event not published");
        return new EventPublishException(
                "Failed to publish event %s to topic %s".formatted(event.eventId(), topic), failure);
    }
}
