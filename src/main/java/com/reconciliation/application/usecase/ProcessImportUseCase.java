package com.reconciliation.application.usecase;

import com.reconciliation.application.port.BatchWriter;
import com.reconciliation.application.port.BatchWriter.BatchOutcome;
import com.reconciliation.application.port.BatchWriter.BatchWrite;
import com.reconciliation.application.port.Clock;
import com.reconciliation.application.port.FileStorage;
import com.reconciliation.application.port.ImportJobQueue;
import com.reconciliation.application.port.ImportRepository;
import com.reconciliation.application.port.LeaseLostException;
import com.reconciliation.application.port.MetricsRecorder;
import com.reconciliation.application.port.RetryPolicy;
import com.reconciliation.application.port.RiskScorer;
import com.reconciliation.application.port.TransientFailureClassifier;
import com.reconciliation.application.processing.CancellationToken;
import com.reconciliation.application.processing.NdjsonLineReader;
import com.reconciliation.application.processing.ProcessingOutcome;
import com.reconciliation.application.processing.RecordParser;
import com.reconciliation.domain.error.AppException;
import com.reconciliation.domain.error.ErrorCategory;
import com.reconciliation.domain.error.ErrorCode;
import com.reconciliation.domain.model.ImportJob;
import com.reconciliation.domain.model.NormalizedTransaction;
import com.reconciliation.domain.model.Rejection;
import com.reconciliation.domain.model.ScoredTransaction;
import com.reconciliation.domain.processing.TransactionValidator;
import com.reconciliation.domain.processing.ValidationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Processes one claimed import end to end.
 *
 * <p>Pipeline, per import:
 * <pre>
 *   reader (stream, bounded line) -> parse + validate + normalise (this thread)
 *       -> batch of N -> RiskScorer pool (CPU, bounded)        \
 *       -> writer thread: wait for scores, persist atomically   | at most maxInFlightBatches
 *          with checkpoint, release permit                     /
 * </pre>
 * Back-pressure: the parsing loop acquires a permit per batch and the writer releases it
 * after the batch is committed, so at most {@code maxInFlightBatches * batchSize} records
 * are ever materialised for an import. The scorer's own bounded queue blocks submission
 * when the CPU pool is saturated.
 *
 * <p>Resumption: lines up to the persisted checkpoint are skipped without parsing, so a
 * second attempt after a crash continues where the last committed batch ended. Because
 * checkpoint and batch commit atomically, no record is ever counted twice; the unique
 * constraint on transactions is the second line of defence.
 */
public class ProcessImportUseCase {

    private static final Logger log = LoggerFactory.getLogger(ProcessImportUseCase.class);

    /** Bounds and collaborators that do not change per import. */
    public record Settings(int batchSize, int maxInFlightBatches, int maxLineLength, int maxRawValueLength,
                           int maxAttempts) {}

    private final ImportRepository imports;
    private final ImportJobQueue queue;
    private final FileStorage storage;
    private final RiskScorer scorer;
    private final BatchWriter batchWriter;
    private final RetryPolicy retry;
    private final TransientFailureClassifier transientFailures;
    private final Clock clock;
    private final MetricsRecorder metrics;
    private final Settings settings;

    public ProcessImportUseCase(ImportRepository imports, ImportJobQueue queue, FileStorage storage,
                                RiskScorer scorer, BatchWriter batchWriter, RetryPolicy retry,
                                TransientFailureClassifier transientFailures, Clock clock,
                                MetricsRecorder metrics, Settings settings) {
        this.imports = imports;
        this.queue = queue;
        this.storage = storage;
        this.scorer = scorer;
        this.batchWriter = batchWriter;
        this.retry = retry;
        this.transientFailures = transientFailures;
        this.clock = clock;
        this.metrics = metrics;
        this.settings = settings;
    }

    public ProcessingOutcome execute(ImportJob job, String workerId, CancellationToken token) {
        MDC.put("importId", job.id().toString());
        MDC.put("attempt", String.valueOf(job.attemptCount()));
        long startedAt = System.nanoTime();
        try {
            if (job.cancelRequested()) {
                // Claimed a 'cancelling' import abandoned by a dead worker: just finalise it.
                return finish(job, workerId, ProcessingOutcome.CANCELLED, null, startedAt);
            }
            Pipeline pipeline = new Pipeline(job, workerId, token);
            Throwable failure = pipeline.run();
            if (failure != null) {
                return handleFailure(job, workerId, failure, startedAt);
            }
            if (token.isCancelled()) {
                return switch (token.reason()) {
                    case CANCEL_REQUESTED -> finish(job, workerId, ProcessingOutcome.CANCELLED, null, startedAt);
                    case SHUTDOWN -> finish(job, workerId, ProcessingOutcome.RELEASED, "Worker shutting down", startedAt);
                    case LEASE_LOST -> finish(job, workerId, ProcessingOutcome.ABANDONED, null, startedAt);
                };
            }
            return finish(job, workerId, ProcessingOutcome.COMPLETED, null, startedAt);
        } finally {
            MDC.remove("importId");
            MDC.remove("attempt");
            MDC.remove("batch");
        }
    }

    private ProcessingOutcome handleFailure(ImportJob job, String workerId, Throwable failure, long startedAt) {
        if (failure instanceof LeaseLostException) {
            log.warn("Lease lost; abandoning attempt");
            return finish(job, workerId, ProcessingOutcome.ABANDONED, null, startedAt);
        }
        // The classifier knows what is worth another attempt (lost connections, deadlocks,
        // storage I/O, retryable AppExceptions). Programming errors, constraint violations,
        // validation and cancellation are never transient, so they fail the import at once.
        boolean programming = failure instanceof AppException app && app.category() == ErrorCategory.PROGRAMMING;
        // The budget counts attempts since the last manual retry (attempt_base), so a client
        // retry of a failed import gets maxAttempts fresh attempts, and no more.
        if (!programming && transientFailures.isTransient(failure) && job.attemptsSinceRetry() < settings.maxAttempts()) {
            log.warn("Transient failure on attempt {}/{}; releasing import for retry: {}",
                    job.attemptsSinceRetry(), settings.maxAttempts(), failure.toString());
            return finish(job, workerId, ProcessingOutcome.RELEASED, failure.getMessage(), startedAt);
        }
        log.error("Import processing failed permanently (attempt {}/{})", job.attemptsSinceRetry(), settings.maxAttempts(),
                failure);
        return finish(job, workerId, ProcessingOutcome.FAILED, safeMessage(failure), startedAt);
    }

    private ProcessingOutcome finish(ImportJob job, String workerId, ProcessingOutcome outcome, String message, long startedAt) {
        boolean applied = switch (outcome) {
            case COMPLETED -> imports.complete(job.id(), workerId, clock.now());
            case CANCELLED -> imports.markCancelled(job.id(), workerId, clock.now());
            case FAILED -> imports.fail(job.id(), workerId, ErrorCode.PROCESSING_FAILED.name(), message, clock.now());
            case RELEASED -> queue.release(job.id(), workerId);
            case ABANDONED -> true;
        };
        if (!applied) {
            log.warn("Could not record outcome {}: lease no longer held", outcome);
            outcome = ProcessingOutcome.ABANDONED;
        }
        metrics.importFinished(outcome.wireName(), System.nanoTime() - startedAt);
        log.info("Import attempt finished outcome={} durationMs={}", outcome, (System.nanoTime() - startedAt) / 1_000_000);
        return outcome;
    }

    private static String safeMessage(Throwable failure) {
        if (failure instanceof AppException app) {
            return app.code().name() + ": " + app.getMessage();
        }
        return failure.getClass().getSimpleName();
    }

    // ------------------------------------------------------------------------

    /** State of one attempt. */
    private final class Pipeline {

        private record Pending(int batchNumber, long lastLine, CompletableFuture<List<ScoredTransaction>> scoring,
                               List<Rejection> rejections, int candidateCount) {}

        private static final Pending POISON = new Pending(-1, -1, null, List.of(), 0);

        private final ImportJob job;
        private final String workerId;
        private final CancellationToken token;
        private final Semaphore inFlight;
        private final BlockingQueue<Pending> pending;
        private final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        private final RecordParser parser;

        Pipeline(ImportJob job, String workerId, CancellationToken token) {
            this.job = job;
            this.workerId = workerId;
            this.token = token;
            this.inFlight = new Semaphore(settings.maxInFlightBatches());
            this.pending = new ArrayBlockingQueue<>(settings.maxInFlightBatches() + 1);
            this.parser = new RecordParser(new TransactionValidator(job.providerId()), settings.maxRawValueLength());
        }

        /** Returns the failure that stopped the pipeline, or null. */
        Throwable run() {
            Thread writer = Thread.ofVirtual().name("import-writer-" + job.id()).start(this::writerLoop);
            Throwable readerFailure = null;
            try {
                readAndSubmit();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                token.cancel(CancellationToken.Reason.SHUTDOWN);
            } catch (Throwable t) {
                readerFailure = t;
            } finally {
                try {
                    pending.put(POISON);
                    writer.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            Throwable failure = writerFailure.get();
            return failure != null ? failure : readerFailure;
        }

        private void readAndSubmit() throws Exception {
            long resumeAfter = job.lastLineCommitted();
            if (resumeAfter > 0) {
                log.info("Resuming after committed line {}", resumeAfter);
            }
            int batchNumber = 0;
            List<NormalizedTransaction> candidates = new ArrayList<>(settings.batchSize());
            List<Rejection> rejections = new ArrayList<>();
            long lastLine = resumeAfter;
            try (InputStream in = storage.open(job.storageKey());
                 NdjsonLineReader reader = new NdjsonLineReader(in, settings.maxLineLength())) {
                NdjsonLineReader.Line line;
                while ((line = reader.next()) != null) {
                    if (line.number() <= resumeAfter || line.isBlank()) {
                        continue;
                    }
                    if (writerFailure.get() != null || token.isCancelled()) {
                        return;
                    }
                    switch (parser.parse(line)) {
                        case ValidationResult.Valid v -> candidates.add(v.transaction());
                        case ValidationResult.Invalid i -> rejections.add(i.rejection());
                    }
                    lastLine = line.number();
                    if (candidates.size() + rejections.size() >= settings.batchSize()) {
                        submit(++batchNumber, lastLine, candidates, rejections);
                        candidates = new ArrayList<>(settings.batchSize());
                        rejections = new ArrayList<>();
                        pollCancellation();
                    }
                }
                if (!candidates.isEmpty() || !rejections.isEmpty()) {
                    if (writerFailure.get() == null && !token.isCancelled()) {
                        submit(++batchNumber, lastLine, candidates, rejections);
                    }
                }
            } catch (IOException e) {
                throw new AppException(ErrorCode.STORAGE_FAILURE, "Could not read the uploaded file", e);
            }
        }

        private void submit(int batchNumber, long lastLine, List<NormalizedTransaction> candidates,
                            List<Rejection> rejections) throws InterruptedException {
            inFlight.acquire(); // back-pressure: wait until the writer has committed an earlier batch
            metrics.recordsParsed(candidates.size() + rejections.size());
            CompletableFuture<List<ScoredTransaction>> scoring = candidates.isEmpty()
                    ? CompletableFuture.completedFuture(List.of())
                    : scorer.scoreAsync(candidates);
            pending.put(new Pending(batchNumber, lastLine, scoring, rejections, candidates.size()));
        }

        private void pollCancellation() {
            if (!token.isCancelled() && imports.isCancelRequested(job.id())) {
                token.cancel(CancellationToken.Reason.CANCEL_REQUESTED);
                log.info("Cancellation observed; stopping after in-flight batches");
            }
        }

        private void writerLoop() {
            MDC.put("importId", job.id().toString());
            MDC.put("workerId", workerId);
            try {
                while (true) {
                    Pending p = pending.take();
                    if (p == POISON) {
                        return;
                    }
                    try {
                        if (writerFailure.get() == null) {
                            persist(p);
                        }
                    } catch (Throwable t) {
                        writerFailure.compareAndSet(null, t);
                        p.scoring().cancel(true);
                    } finally {
                        inFlight.release();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                writerFailure.compareAndSet(null, e);
            } finally {
                MDC.clear();
            }
        }

        private void persist(Pending p) throws Exception {
            MDC.put("batch", String.valueOf(p.batchNumber()));
            long start = System.nanoTime();
            List<ScoredTransaction> scored;
            try {
                scored = p.scoring().join();
            } catch (CompletionException e) {
                throw e.getCause() instanceof Exception ex ? ex : e;
            }
            BatchWrite write = new BatchWrite(job.id(), workerId, p.batchNumber(), p.lastLine(), scored, p.rejections());
            BatchOutcome outcome = retry.execute("batch-write", token, () -> batchWriter.write(write));
            metrics.batchWritten(outcome.accepted(), outcome.rejected(), outcome.duplicates(), System.nanoTime() - start);
            if (log.isDebugEnabled()) {
                log.debug("Batch committed accepted={} rejected={} duplicates={} lastLine={} durationMs={}",
                        outcome.accepted(), outcome.rejected(), outcome.duplicates(), p.lastLine(),
                        (System.nanoTime() - start) / 1_000_000);
            }
        }
    }
}
