package io.github.lucascinigalia.kafkaboilerplate.examples.producer.domain;

import java.math.BigDecimal;
import java.util.UUID;

/** Payload of the event published when an order is placed. */
public record OrderCreated(UUID orderId, String customerId, BigDecimal totalAmount) {

    public static final String TYPE = "OrderCreated";
    public static final int VERSION = 1;
}
