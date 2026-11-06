package com.reconciliation.domain.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static com.reconciliation.domain.model.ImportStatus.CANCELLED;
import static com.reconciliation.domain.model.ImportStatus.CANCELLING;
import static com.reconciliation.domain.model.ImportStatus.COMPLETED;
import static com.reconciliation.domain.model.ImportStatus.FAILED;
import static com.reconciliation.domain.model.ImportStatus.PENDING;
import static com.reconciliation.domain.model.ImportStatus.PROCESSING;
import static com.reconciliation.domain.model.ImportStatus.UPLOADING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The state machine every conditional UPDATE in the repository implements. */
class ImportStatusTest {

    @Test
    void happyPath() {
        assertThat(UPLOADING.canTransitionTo(PENDING)).isTrue();
        assertThat(PENDING.canTransitionTo(PROCESSING)).isTrue();
        assertThat(PROCESSING.canTransitionTo(COMPLETED)).isTrue();
    }

    @Test
    void failurePaths() {
        assertThat(UPLOADING.canTransitionTo(FAILED)).isTrue();
        assertThat(PENDING.canTransitionTo(FAILED)).isTrue();
        assertThat(PROCESSING.canTransitionTo(FAILED)).isTrue();
        assertThat(CANCELLING.canTransitionTo(FAILED)).isTrue();
    }

    @Test
    void cancellationPaths() {
        assertThat(PENDING.canTransitionTo(CANCELLED)).as("nobody holds a pending import: cancel at once").isTrue();
        assertThat(PROCESSING.canTransitionTo(CANCELLING)).isTrue();
        assertThat(CANCELLING.canTransitionTo(CANCELLED)).isTrue();
        assertThat(PROCESSING.canTransitionTo(CANCELLED)).as("worker finalises a cancel it observed itself").isTrue();
    }

    @Test
    void releaseForRetryGoesBackToPending() {
        assertThat(PROCESSING.canTransitionTo(PENDING)).isTrue();
        assertThat(CANCELLING.canTransitionTo(PENDING)).as("a cancel in progress is never retried").isFalse();
    }

    @Test
    void nothingSkipsStates() {
        assertThat(UPLOADING.canTransitionTo(PROCESSING)).isFalse();
        assertThat(UPLOADING.canTransitionTo(COMPLETED)).isFalse();
        assertThat(PENDING.canTransitionTo(COMPLETED)).isFalse();
        assertThat(PENDING.canTransitionTo(CANCELLING)).isFalse();
    }

    @ParameterizedTest
    @EnumSource(value = ImportStatus.class, names = {"COMPLETED", "FAILED", "CANCELLED"})
    void terminalStatesAreFinalAndNotCancellable(ImportStatus terminal) {
        assertThat(terminal.isTerminal()).isTrue();
        assertThat(terminal.isCancellable()).isFalse();
        for (ImportStatus target : ImportStatus.values()) {
            boolean manualRetry = terminal == FAILED && target == PENDING;
            assertThat(terminal.canTransitionTo(target)).as(terminal + " -> " + target).isEqualTo(manualRetry);
        }
    }

    @Test
    void onlyAFailedImportCanBeRequeuedAndOnlyToPending() {
        assertThat(FAILED.canTransitionTo(PENDING)).isTrue();
        assertThat(FAILED.canTransitionTo(PROCESSING)).isFalse();
        assertThat(COMPLETED.canTransitionTo(PENDING)).isFalse();
        assertThat(CANCELLED.canTransitionTo(PENDING)).isFalse();
    }

    @ParameterizedTest
    @EnumSource(value = ImportStatus.class, names = {"UPLOADING", "PENDING", "PROCESSING", "CANCELLING"})
    void nonTerminalStates(ImportStatus status) {
        assertThat(status.isTerminal()).isFalse();
    }

    @Test
    void onlyPendingAndProcessingAcceptACancelRequest() {
        assertThat(PENDING.isCancellable()).isTrue();
        assertThat(PROCESSING.isCancellable()).isTrue();
        assertThat(UPLOADING.isCancellable()).isFalse();
        assertThat(CANCELLING.isCancellable()).as("already cancelling; idempotent at the use-case level").isFalse();
    }

    @Test
    void wireNamesRoundTrip() {
        for (ImportStatus s : ImportStatus.values()) {
            assertThat(s.wireName()).isEqualTo(s.name().toLowerCase());
            assertThat(ImportStatus.fromWireName(s.wireName())).isEqualTo(s);
        }
        assertThatThrownBy(() -> ImportStatus.fromWireName("done")).isInstanceOf(IllegalArgumentException.class);
    }
}
