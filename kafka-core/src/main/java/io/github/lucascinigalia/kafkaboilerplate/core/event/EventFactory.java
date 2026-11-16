package io.github.lucascinigalia.kafkaboilerplate.core.event;

import java.time.Clock;
import java.util.Objects;
import java.util.UUID;

/** Creates envelopes, filling in the fields that business code should not have to care about. */
public class EventFactory {

    private final String source;
    private final Clock clock;

    public EventFactory(String source, Clock clock) {
        this.source = Objects.requireNonNull(source, "source is required");
        this.clock = Objects.requireNonNull(clock, "clock is required");
    }

    /** Creates an event that starts a new flow, with a fresh correlation id. */
    public <T> EventEnvelope<T> create(String type, int version, T payload) {
        return create(type, version, payload, UUID.randomUUID().toString());
    }

    /** Creates an event that joins an existing flow, e.g. one started by an HTTP request. */
    public <T> EventEnvelope<T> create(String type, int version, T payload, String correlationId) {
        return new EventEnvelope<>(
                UUID.randomUUID(), type, version, clock.instant(), correlationId, null, source, payload);
    }

    /** Creates an event caused by another one: same correlation id, causation id pointing to the cause. */
    public <T> EventEnvelope<T> createCausedBy(EventEnvelope<?> cause, String type, int version, T payload) {
        return new EventEnvelope<>(
                UUID.randomUUID(), type, version, clock.instant(), cause.correlationId(), cause.eventId(), source,
                payload);
    }
}
