package io.github.lucascinigalia.kafkaboilerplate.core.event;

import java.util.UUID;

/** Where the broker stored a published event. */
public record PublishResult(UUID eventId, String topic, int partition, long offset) {}
