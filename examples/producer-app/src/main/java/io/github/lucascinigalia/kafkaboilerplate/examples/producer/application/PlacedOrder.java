package io.github.lucascinigalia.kafkaboilerplate.examples.producer.application;

import io.github.lucascinigalia.kafkaboilerplate.core.event.PublishResult;
import java.util.UUID;

public record PlacedOrder(UUID orderId, String correlationId, PublishResult publication) {}
