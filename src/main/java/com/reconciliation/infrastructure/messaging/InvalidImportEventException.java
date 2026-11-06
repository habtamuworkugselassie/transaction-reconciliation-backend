package com.reconciliation.infrastructure.messaging;

/**
 * A record on the imports topic that deserialised but cannot be acted on (no import id).
 * Never retried; the listener container sends it to the dead-letter topic.
 */
public class InvalidImportEventException extends RuntimeException {
    public InvalidImportEventException(String message) {
        super(message);
    }
}
