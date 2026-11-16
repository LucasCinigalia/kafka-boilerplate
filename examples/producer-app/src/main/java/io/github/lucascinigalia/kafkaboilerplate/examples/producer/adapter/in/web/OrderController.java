package io.github.lucascinigalia.kafkaboilerplate.examples.producer.adapter.in.web;

import io.github.lucascinigalia.kafkaboilerplate.examples.producer.application.PlaceOrderCommand;
import io.github.lucascinigalia.kafkaboilerplate.examples.producer.application.PlaceOrderUseCase;
import io.github.lucascinigalia.kafkaboilerplate.examples.producer.application.PlacedOrder;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

    static final String CORRELATION_ID_HEADER = "X-Correlation-Id";

    private final PlaceOrderUseCase placeOrder;

    public OrderController(PlaceOrderUseCase placeOrder) {
        this.placeOrder = placeOrder;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public OrderResponse place(
            @Valid @RequestBody OrderRequest request,
            @RequestHeader(name = CORRELATION_ID_HEADER, required = false) String correlationId) {
        PlacedOrder placed =
                placeOrder.place(new PlaceOrderCommand(request.customerId(), request.totalAmount(), correlationId));
        return new OrderResponse(
                placed.orderId(),
                placed.publication().eventId(),
                placed.correlationId(),
                placed.publication().topic(),
                placed.publication().partition(),
                placed.publication().offset());
    }

    public record OrderRequest(@NotBlank String customerId, @NotNull @Positive BigDecimal totalAmount) {}

    public record OrderResponse(
            UUID orderId, UUID eventId, String correlationId, String topic, int partition, long offset) {}
}
