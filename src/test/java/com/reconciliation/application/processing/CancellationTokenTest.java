package com.reconciliation.application.processing;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CancellationTokenTest {

    @Test
    void startsUncancelled() {
        CancellationToken token = new CancellationToken();
        assertThat(token.isCancelled()).isFalse();
        assertThat(token.reason()).isNull();
    }

    @Test
    void firstReasonWins() {
        CancellationToken token = new CancellationToken();
        token.cancel(CancellationToken.Reason.CANCEL_REQUESTED);
        token.cancel(CancellationToken.Reason.SHUTDOWN);

        assertThat(token.isCancelled()).isTrue();
        assertThat(token.reason()).isEqualTo(CancellationToken.Reason.CANCEL_REQUESTED);
    }
}
