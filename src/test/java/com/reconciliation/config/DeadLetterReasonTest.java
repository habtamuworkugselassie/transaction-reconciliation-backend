package com.reconciliation.config;

import com.reconciliation.infrastructure.messaging.InvalidImportEventException;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.serializer.DeserializationException;

import static org.assertj.core.api.Assertions.assertThat;

/** The dead-letter counter's label must stay low-cardinality and see through Spring's wrappers. */
class DeadLetterReasonTest {

    @Test
    void classifiesDeadRecordsIntoAFixedSetOfReasons() {
        assertThat(MessagingConfig.deadLetterReason(
                new DeserializationException("bad json", new byte[] {1}, false, new RuntimeException())))
                .isEqualTo("deserialization");
        assertThat(MessagingConfig.deadLetterReason(
                new ListenerExecutionFailedException("listener failed", new InvalidImportEventException("no id"))))
                .isEqualTo("invalid_event");
        assertThat(MessagingConfig.deadLetterReason(new IllegalStateException("anything else"))).isEqualTo("other");
    }
}
