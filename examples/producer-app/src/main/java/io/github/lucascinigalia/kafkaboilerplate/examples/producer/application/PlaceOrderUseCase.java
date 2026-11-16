package io.github.lucascinigalia.kafkaboilerplate.examples.producer.application;

import io.github.lucascinigalia.kafkaboilerplate.core.event.EventEnvelope;
import io.github.lucascinigalia.kafkaboilerplate.core.event.EventFactory;
import io.github.lucascinigalia.kafkaboilerplate.core.event.EventPublisher;
import io.github.lucascinigalia.kafkaboilerplate.core.event.PublishResult;
import io.github.lucascinigalia.kafkaboilerplate.examples.producer.domain.OrderCreated;
import java.util.UUID;
import org.springframework.stereotype.Service;

/** Business code: it depends on the {@link EventPublisher} port and never sees Kafka. */
@Service
public class PlaceOrderUseCase {

    private final EventFactory events;
    private final EventPublisher publisher;

    public PlaceOrderUseCase(EventFactory events, EventPublisher publisher) {
        this.events = events;
        this.publisher = publisher;
    }

    public PlacedOrder place(PlaceOrderCommand command) {
        OrderCreated payload = new OrderCreated(UUID.randomUUID(), command.customerId(), command.totalAmount());
        EventEnvelope<OrderCreated> event = command.correlationId() == null
                ? events.create(OrderCreated.TYPE, OrderCreated.VERSION, payload)
                : events.create(OrderCreated.TYPE, OrderCreated.VERSION, payload, command.correlationId());

        // The customer id is the key: all orders of one customer stay in order.
        PublishResult publication = publisher.publishAndWait(event, command.customerId());
        return new PlacedOrder(payload.orderId(), event.correlationId(), publication);
    }
}
