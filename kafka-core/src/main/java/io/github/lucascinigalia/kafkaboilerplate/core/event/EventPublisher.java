package io.github.lucascinigalia.kafkaboilerplate.core.event;

import java.util.concurrent.CompletableFuture;

/**
 * Port used by business code to publish events. It knows nothing about the broker:
 * the destination is resolved from the event type by the adapter.
 */
public interface EventPublisher {

    /**
     * Publishes without blocking. The future completes when the broker confirms the write,
     * or fails with {@link EventPublishException}.
     *
     * @param key events with the same key keep their relative order
     */
    CompletableFuture<PublishResult> publish(EventEnvelope<?> event, String key);

    /**
     * Publishes and waits for the broker confirmation, up to the configured timeout.
     *
     * @throws EventPublishException if the event could not be confirmed in time
     */
    PublishResult publishAndWait(EventEnvelope<?> event, String key);
}
