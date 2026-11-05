package com.reconciliation.testing;

import com.reconciliation.application.port.MetricsRecorder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Records every metric call so a test can assert on what the use case reported. */
public final class RecordingMetrics implements MetricsRecorder {

    public final List<String> events = Collections.synchronizedList(new ArrayList<>());

    @Override public void importCreated(long bytes) { events.add("importCreated:" + bytes); }
    @Override public void importReplayed() { events.add("importReplayed"); }
    @Override public void uploadRejected(String reason) { events.add("uploadRejected:" + reason); }
    @Override public void recordsParsed(int count) { events.add("recordsParsed:" + count); }
    @Override public void batchWritten(int accepted, int rejected, int duplicates, long durationNanos) {
        events.add("batchWritten:" + accepted + "/" + rejected + "/" + duplicates);
    }
    @Override public void importFinished(String outcome, long durationNanos) { events.add("importFinished:" + outcome); }
    @Override public void retryAttempt(String operation, int attempt) { events.add("retryAttempt:" + operation + ":" + attempt); }
    @Override public void retryExhausted(String operation) { events.add("retryExhausted:" + operation); }

    public long count(String prefix) {
        synchronized (events) {
            return events.stream().filter(e -> e.startsWith(prefix)).count();
        }
    }
}
