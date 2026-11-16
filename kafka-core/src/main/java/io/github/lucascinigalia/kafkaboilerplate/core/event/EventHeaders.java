package io.github.lucascinigalia.kafkaboilerplate.core.event;

/**
 * Names of the record headers that mirror the envelope metadata, so consumers can route,
 * filter and log without deserializing the value.
 */
public final class EventHeaders {

    public static final String EVENT_ID = "event-id";
    public static final String EVENT_TYPE = "event-type";
    public static final String EVENT_VERSION = "event-version";
    public static final String CORRELATION_ID = "correlation-id";
    public static final String CAUSATION_ID = "causation-id";
    public static final String SOURCE = "event-source";

    private EventHeaders() {}
}
