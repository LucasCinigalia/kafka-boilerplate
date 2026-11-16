package io.github.lucascinigalia.kafkaboilerplate.core.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import io.github.lucascinigalia.kafkaboilerplate.core.support.SamplePayload;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class EventFactoryTest {

    private static final Instant NOW = Instant.parse("2026-11-16T12:00:00Z");
    private static final SamplePayload PAYLOAD = new SamplePayload("item", 1);

    private final EventFactory factory = new EventFactory("test-app", Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    void newFlowGetsFreshIdsAndNoCausation() {
        EventEnvelope<SamplePayload> first = factory.create("SampleCreated", 1, PAYLOAD);
        EventEnvelope<SamplePayload> second = factory.create("SampleCreated", 1, PAYLOAD);

        assertThat(first.source()).isEqualTo("test-app");
        assertThat(first.occurredAt()).isEqualTo(NOW);
        assertThat(first.causationId()).isNull();
        assertThat(first.eventId()).isNotEqualTo(second.eventId());
        assertThat(first.correlationId()).isNotEqualTo(second.correlationId());
    }

    @Test
    void keepsCorrelationIdReceivedFromOutside() {
        EventEnvelope<SamplePayload> event = factory.create("SampleCreated", 1, PAYLOAD, "request-42");

        assertThat(event.correlationId()).isEqualTo("request-42");
    }

    @Test
    void causedEventKeepsCorrelationAndPointsToItsCause() {
        EventEnvelope<SamplePayload> cause = factory.create("SampleCreated", 1, PAYLOAD);

        EventEnvelope<SamplePayload> effect = factory.createCausedBy(cause, "SampleApproved", 1, PAYLOAD);

        assertThat(effect.correlationId()).isEqualTo(cause.correlationId());
        assertThat(effect.causationId()).isEqualTo(cause.eventId());
        assertThat(effect.eventId()).isNotEqualTo(cause.eventId());
    }

    @Test
    void envelopeRejectsMissingRequiredFields() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new EventEnvelope<>(UUID.randomUUID(), " ", 1, NOW, "c", null, "s", PAYLOAD))
                .withMessageContaining("type");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new EventEnvelope<>(UUID.randomUUID(), "T", 0, NOW, "c", null, "s", PAYLOAD))
                .withMessageContaining("version");
    }
}
