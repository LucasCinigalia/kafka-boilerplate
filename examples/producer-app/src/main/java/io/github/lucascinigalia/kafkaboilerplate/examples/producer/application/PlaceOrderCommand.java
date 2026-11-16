package io.github.lucascinigalia.kafkaboilerplate.examples.producer.application;

import java.math.BigDecimal;

/**
 * @param correlationId id of the flow this order belongs to; {@code null} starts a new flow
 */
public record PlaceOrderCommand(String customerId, BigDecimal totalAmount, String correlationId) {}
