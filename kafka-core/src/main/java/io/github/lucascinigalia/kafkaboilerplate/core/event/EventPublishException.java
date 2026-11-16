package io.github.lucascinigalia.kafkaboilerplate.core.event;

/** The event was not confirmed by the broker. It may or may not have been written. */
public class EventPublishException extends RuntimeException {

    public EventPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
