package io.github.lucascinigalia.kafkaboilerplate.core.event;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Standard envelope wrapped around every event.
 *
 * @param eventId unique id of this event; consumers use it to detect duplicates
 * @param type logical event name, e.g. {@code OrderCreated}; also decides the destination topic
 * @param version version of the payload contract
 * @param occurredAt when the fact happened
 * @param correlationId id shared by every event that belongs to the same business flow
 * @param causationId id of the event that caused this one; {@code null} when it starts a flow
 * @param source name of the application that produced the event
 * @param payload event data
 */
public record EventEnvelope<T>(
        UUID eventId,
        String type,
        int version,
        Instant occurredAt,
        String correlationId,
        UUID causationId,
        String source,
        T payload) {

    public EventEnvelope {
        Objects.requireNonNull(eventId, "eventId is required");
        requireText(type, "type");
        if (version < 1) {
            throw new IllegalArgumentException("version must be >= 1");
        }
        Objects.requireNonNull(occurredAt, "occurredAt is required");
        requireText(correlationId, "correlationId");
        requireText(source, "source");
        Objects.requireNonNull(payload, "payload is required");
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
    }
}
