package com.reconciliation.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.reconciliation.application.port.BatchWriter;
import com.reconciliation.application.port.Clock;
import com.reconciliation.application.port.FileStorage;
import com.reconciliation.application.port.IdempotencyCache;
import com.reconciliation.application.port.ImportJobQueue;
import com.reconciliation.application.port.ImportRepository;
import com.reconciliation.application.port.LeaseLostException;
import com.reconciliation.application.port.MetricsRecorder;
import com.reconciliation.application.port.RetryPolicy;
import com.reconciliation.application.port.SummaryCache;
import com.reconciliation.application.port.TransientFailureClassifier;
import com.reconciliation.application.processing.CancellationToken;
import com.reconciliation.application.processing.ProcessingOutcome;
import com.reconciliation.application.usecase.ProcessImportUseCase;
import com.reconciliation.config.AppProperties;
import com.reconciliation.domain.model.ImportJob;
import com.reconciliation.infrastructure.persistence.JooqImportJobStore;
import com.reconciliation.infrastructure.persistence.PostgresSupport;
import com.reconciliation.testing.DirectRiskScorer;
import com.reconciliation.testing.FixedClock;
import com.reconciliation.testing.Fixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.reconciliation.infrastructure.persistence.jooq.Tables.IMPORTS;
import static com.reconciliation.infrastructure.persistence.jooq.Tables.TRANSACTIONS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The service over real HTTP: the full Spring context, Tomcat on a random port, the
 * streaming multipart reader, the error mapping and the jOOQ adapters on PostgreSQL.
 *
 * <p>What is replaced, and why:
 * <ul>
 *   <li>{@link ImportJobQueue}: a database-only implementation (same claims and leases,
 *       no broker), so the test needs no Kafka. Dispatch through Kafka is covered by the
 *       lease semantics it relies on, which are exercised here.</li>
 *   <li>{@link SummaryCache} / {@link IdempotencyCache}: no-ops, so every read hits the
 *       database and the test needs no Redis. The caches fail soft in production anyway.</li>
 *   <li>{@link Clock}: a {@link FixedClock} the test moves to expire leases.</li>
 *   <li>The worker is disabled ({@code app.processing.enabled=false}); the test drives
 *       {@link ProcessImportUseCase} itself, built from the context's own adapters, so it
 *       decides exactly when a "crash" happens.</li>
 * </ul>
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "app.processing.enabled=false",
                "spring.kafka.admin.auto-create=false",
                "management.endpoint.health.group.readiness.include=readinessState,db,storage,worker",
                "app.upload.max-file-size-bytes=" + ImportApiIT.MAX_FILE_BYTES,
                "app.upload.max-concurrent-uploads=32",
                "app.retry.initial-backoff=10ms",
                "app.retry.max-backoff=50ms"
        })
class ImportApiIT {

    static final int MAX_FILE_BYTES = 64 * 1024;
    private static final int BATCH_SIZE = 10;
    private static final Duration LEASE = Duration.ofSeconds(30);
    private static final Path STORAGE_ROOT = createStorageRoot();

    private static final PostgresSupport PG = PostgresSupport.get();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", PG::jdbcUrl);
        registry.add("spring.datasource.username", PG::username);
        registry.add("spring.datasource.password", PG::password);
        registry.add("app.storage.root", STORAGE_ROOT::toString);
        registry.add("app.processing.lease-duration", LEASE::toString);
    }

    @TestConfiguration
    static class TestBeans {

        @Bean
        @Primary
        FixedClock testClock() {
            return new FixedClock();
        }

        @Bean
        @Primary
        ImportJobQueue databaseOnlyImportJobQueue(JooqImportJobStore store, ImportRepository imports, Clock clock,
                                                  AppProperties props) {
            return new DatabaseOnlyImportJobQueue(store, imports, clock, props.processing().leaseDuration());
        }

        @Bean
        @Primary
        SummaryCache noOpSummaryCache() {
            return SummaryCache.NONE;
        }

        @Bean
        @Primary
        IdempotencyCache noOpIdempotencyCache() {
            return IdempotencyCache.NONE;
        }
    }

    @Autowired TestRestTemplate http;
    @Autowired ObjectMapper json;
    @Autowired FixedClock clock;
    @Autowired ImportJobQueue queue;
    @Autowired ImportRepository imports;
    @Autowired FileStorage storage;
    @Autowired BatchWriter batchWriter;
    @Autowired RetryPolicy retryPolicy;
    @Autowired TransientFailureClassifier classifier;
    @Autowired MetricsRecorder metrics;

    @BeforeEach
    void reset() throws IOException {
        PG.truncateAll();
        clock.set(FixedClock.START);
        Path uploads = STORAGE_ROOT.resolve("imports");
        if (Files.exists(uploads)) {
            try (Stream<Path> files = Files.walk(uploads)) {
                files.sorted(Comparator.reverseOrder()).filter(p -> !p.equals(uploads)).forEach(ImportApiIT::delete);
            }
        }
    }

    // ---- upload -------------------------------------------------------------

    @Test
    void uploadIsAcceptedAndQueued() throws Exception {
        ResponseEntity<String> response = upload("create-1", Fixtures.validLines(5));

        assertThat(response.getStatusCode().value()).isEqualTo(202);
        JsonNode body = json.readTree(response.getBody());
        UUID id = UUID.fromString(body.get("id").asText());
        assertThat(body.get("status").asText()).isEqualTo("pending");
        assertThat(Instant.parse(body.get("createdAt").asText())).isEqualTo(FixedClock.START);

        JsonNode status = getJson("/v1/imports/" + id);
        assertThat(status.get("status").asText()).isEqualTo("pending");
        assertThat(status.at("/progress/processed").asLong()).isZero();
        assertThat(storedFiles()).hasSize(1);
        assertThat(PG.db().fetchCount(IMPORTS)).isEqualTo(1);
    }

    @Test
    void replayWithTheSameKeyReturnsTheOriginalImportAndIgnoresTheNewBody() throws Exception {
        UUID first = idOf(upload("replay-1", Fixtures.validLines(3)));

        ResponseEntity<String> replay = upload("replay-1", Fixtures.validLines(7));

        assertThat(replay.getStatusCode().value()).isEqualTo(202);
        assertThat(idOf(replay)).isEqualTo(first);
        assertThat(PG.db().fetchCount(IMPORTS)).isEqualTo(1);
        assertThat(storedFiles()).hasSize(1);
        Long size = PG.db().select(IMPORTS.FILE_SIZE_BYTES).from(IMPORTS).where(IMPORTS.ID.eq(first)).fetchOne(IMPORTS.FILE_SIZE_BYTES);
        assertThat(size).isEqualTo(Fixtures.validLines(3).getBytes(StandardCharsets.UTF_8).length);
    }

    @Test
    void concurrentPostsWithTheSameKeyConvergeOnOneImport() throws Exception {
        int threads = 12;
        String body = Fixtures.validLines(20);
        List<ResponseEntity<String>> responses = concurrently(threads, () -> upload("race-1", body));

        Set<UUID> ids = new HashSet<>();
        for (ResponseEntity<String> r : responses) {
            assertThat(r.getStatusCode().value()).as(r.getBody()).isEqualTo(202);
            ids.add(idOf(r));
        }
        assertThat(ids).hasSize(1);
        assertThat(PG.db().fetchCount(IMPORTS)).isEqualTo(1);
        assertThat(storedFiles()).hasSize(1);
        assertThat(getJson("/v1/imports/" + ids.iterator().next()).get("status").asText()).isEqualTo("pending");
    }

    // ---- rejections at the HTTP boundary -----------------------------------

    @Test
    void missingIdempotencyKeyIs400() throws Exception {
        ResponseEntity<String> r = upload(null, Fixtures.validLines(1));
        assertError(r, 400, "IDEMPOTENCY_KEY_REQUIRED");
        assertThat(PG.db().fetchCount(IMPORTS)).isZero();
    }

    @Test
    void malformedIdempotencyKeyIs400() throws Exception {
        assertError(upload("has spaces!", Fixtures.validLines(1)), 400, "IDEMPOTENCY_KEY_INVALID");
    }

    @Test
    void emptyFileIs400AndTheKeyCanBeRetried() throws Exception {
        assertError(upload("empty-1", ""), 400, "IMPORT_FILE_EMPTY");

        ResponseEntity<String> retry = upload("empty-1", Fixtures.validLines(2));
        assertThat(retry.getStatusCode().value()).isEqualTo(202);
        assertThat(PG.db().fetchCount(IMPORTS)).isEqualTo(1);
    }

    @Test
    void missingFilePartIs400() throws Exception {
        MultiValueMap<String, Object> form = new LinkedMultiValueMap<>();
        form.add("note", "no file here");
        assertError(post("/v1/imports", form, headers("nofile-1", MediaType.MULTIPART_FORM_DATA)), 400,
                "IMPORT_FILE_REQUIRED");
    }

    @Test
    void oversizedFileIs413AndNothingIsKept() throws Exception {
        StringBuilder big = new StringBuilder();
        int n = 0;
        while (big.length() <= MAX_FILE_BYTES + 16 * 1024) {
            big.append(Fixtures.line("big-" + n++)).append('\n');
        }

        assertError(upload("big-1", big.toString()), 413, "IMPORT_FILE_TOO_LARGE");
        assertThat(storedFiles()).isEmpty();
        assertThat(PG.db().select(IMPORTS.STATUS).from(IMPORTS).fetchOne(IMPORTS.STATUS)).isEqualTo("failed");
    }

    @Test
    void binaryContentIs415() throws Exception {
        byte[] gzip = {(byte) 0x1F, (byte) 0x8B, 8, 0, 0, 0, 0, 0, 0, 3};
        assertError(upload("gzip-1", gzip), 415, "IMPORT_FILE_TYPE_INVALID");
        assertThat(storedFiles()).isEmpty();
    }

    @Test
    void nonJsonTextIs415() throws Exception {
        assertError(upload("csv-1", "id,amount\n1,2\n"), 415, "IMPORT_FILE_TYPE_INVALID");
    }

    @Test
    void nonMultipartRequestIs415() throws Exception {
        HttpHeaders h = headers("json-1", MediaType.APPLICATION_JSON);
        assertError(post("/v1/imports", "{\"a\":1}", h), 415, "IMPORT_FILE_TYPE_INVALID");
    }

    @Test
    void errorsCarryTheRequestId() throws Exception {
        HttpHeaders h = headers(null, MediaType.MULTIPART_FORM_DATA);
        h.set(RequestIdFilter.HEADER, "it-req-1");
        ResponseEntity<String> r = post("/v1/imports", fileForm(Fixtures.validLines(1).getBytes(StandardCharsets.UTF_8)), h);

        assertThat(r.getHeaders().getFirst(RequestIdFilter.HEADER)).isEqualTo("it-req-1");
        assertThat(json.readTree(r.getBody()).at("/error/requestId").asText()).isEqualTo("it-req-1");
    }

    @Test
    void unknownImportIs404AndBadIdIs400() throws Exception {
        assertError(http.getForEntity("/v1/imports/" + UUID.randomUUID(), String.class), 404, "IMPORT_NOT_FOUND");
        assertError(http.getForEntity("/v1/imports/not-a-uuid", String.class), 400, "VALIDATION_FAILED");
    }

    // ---- rejections pagination ----------------------------------------------

    @Test
    void rejectionsArePaginatedByCursorOverHttp() throws Exception {
        StringBuilder file = new StringBuilder(Fixtures.line("ok-1")).append('\n');
        for (int i = 0; i < 25; i++) {
            file.append("{\"transactionId\": broken ").append(i).append('\n');
        }
        UUID id = idOf(upload("rej-1", file.toString()));
        assertThat(process(id, "worker-a", batchWriter)).isEqualTo(ProcessingOutcome.COMPLETED);

        List<Long> lines = new ArrayList<>();
        String cursor = null;
        List<Integer> pageSizes = new ArrayList<>();
        do {
            String url = "/v1/imports/" + id + "/rejections?limit=10" + (cursor == null ? "" : "&cursor=" + cursor);
            JsonNode page = getJson(url);
            pageSizes.add(page.get("items").size());
            for (JsonNode item : page.get("items")) {
                lines.add(item.get("lineNumber").asLong());
                assertThat(item.get("reason").asText()).isEqualTo("INVALID_JSON");
            }
            cursor = page.get("nextCursor").isNull() ? null : page.get("nextCursor").asText();
        } while (cursor != null);

        assertThat(pageSizes).containsExactly(10, 10, 5);
        assertThat(lines).hasSize(25).doesNotHaveDuplicates().isSorted();
        assertThat(lines.getFirst()).isEqualTo(2L);
        assertThat(lines.getLast()).isEqualTo(26L);

        assertError(http.getForEntity("/v1/imports/" + id + "/rejections?cursor=%25%25bogus", String.class), 400,
                "INVALID_CURSOR");
        assertError(http.getForEntity("/v1/imports/" + id + "/rejections?limit=0", String.class), 400,
                "VALIDATION_FAILED");
        assertError(http.getForEntity("/v1/imports/" + id + "/rejections?limit=201", String.class), 400,
                "VALIDATION_FAILED");
    }

    // ---- cancel --------------------------------------------------------------

    @Test
    void cancellingAPendingImportIsImmediateAndIdempotent() throws Exception {
        UUID id = idOf(upload("cancel-1", Fixtures.validLines(5)));

        ResponseEntity<String> first = http.postForEntity("/v1/imports/" + id + "/cancel", null, String.class);
        ResponseEntity<String> second = http.postForEntity("/v1/imports/" + id + "/cancel", null, String.class);

        assertThat(first.getStatusCode().value()).isEqualTo(202);
        assertThat(json.readTree(first.getBody()).get("status").asText()).isEqualTo("cancelled");
        assertThat(second.getStatusCode().value()).isEqualTo(202);
        assertThat(json.readTree(second.getBody()).get("status").asText()).isEqualTo("cancelled");
        assertThat(queue.claim(id, "worker-a")).isEmpty();
    }

    @Test
    void cancellingAProcessingImportStopsAtABatchBoundary() throws Exception {
        UUID id = idOf(upload("cancel-2", Fixtures.validLines(60)));
        ImportJob claimed = queue.claim(id, "worker-a").orElseThrow();

        ResponseEntity<String> r = http.postForEntity("/v1/imports/" + id + "/cancel", null, String.class);
        assertThat(r.getStatusCode().value()).isEqualTo(202);
        assertThat(json.readTree(r.getBody()).get("status").asText()).isEqualTo("cancelling");

        ProcessingOutcome outcome = useCase(batchWriter).execute(claimed, "worker-a", new CancellationToken());

        assertThat(outcome).isEqualTo(ProcessingOutcome.CANCELLED);
        JsonNode status = getJson("/v1/imports/" + id);
        assertThat(status.get("status").asText()).isEqualTo("cancelled");
        long processed = status.at("/progress/processed").asLong();
        assertThat(processed).isPositive().isLessThan(60).isEqualTo(status.at("/progress/accepted").asLong());
        assertThat(processed % BATCH_SIZE).isZero();
        assertThat(transactionCount(id)).isEqualTo(processed);
    }

    @Test
    void cancellingACompletedImportIs409AndUnknownIs404() throws Exception {
        UUID id = idOf(upload("cancel-3", Fixtures.validLines(3)));
        assertThat(process(id, "worker-a", batchWriter)).isEqualTo(ProcessingOutcome.COMPLETED);

        assertError(http.postForEntity("/v1/imports/" + id + "/cancel", null, String.class), 409, "IMPORT_NOT_CANCELLABLE");
        assertError(http.postForEntity("/v1/imports/" + UUID.randomUUID() + "/cancel", null, String.class), 404,
                "IMPORT_NOT_FOUND");
    }

    // ---- summary -------------------------------------------------------------

    @Test
    void summaryIsLiveWhilePendingAndFinalAfterCompletion() throws Exception {
        String file = Fixtures.line("s-1", "10.00", "USD", "2026-07-20T10:25:00Z", null) + "\n"
                + Fixtures.line("s-2", "20.50", "usd", "2026-07-20T10:25:00Z", null) + "\n"
                + Fixtures.line("s-3", "5", "EUR", "2026-07-20T10:25:00Z", null) + "\n"
                + Fixtures.line("s-1", "10.00", "USD", "2026-07-20T10:25:00Z", null) + "\n"
                + Fixtures.line("s-4", "0", "USD", "2026-07-20T10:25:00Z", null) + "\n";
        UUID id = idOf(upload("sum-1", file));

        JsonNode live = getJson("/v1/imports/" + id + "/summary");
        assertThat(live.get("isFinal").asBoolean()).isFalse();
        assertThat(live.at("/totals/accepted").asLong()).isZero();

        assertThat(process(id, "worker-a", batchWriter)).isEqualTo(ProcessingOutcome.COMPLETED);

        JsonNode summary = getJson("/v1/imports/" + id + "/summary");
        assertThat(summary.get("isFinal").asBoolean()).isTrue();
        assertThat(summary.get("status").asText()).isEqualTo("completed");
        assertThat(summary.at("/totals/accepted").asLong()).isEqualTo(3);
        assertThat(summary.at("/totals/rejected").asLong()).isEqualTo(1);
        assertThat(summary.at("/totals/duplicates").asLong()).isEqualTo(1);
        assertThat(currencyTotal(summary, "USD")).isEqualByComparingTo("30.50");
        assertThat(currencyTotal(summary, "EUR")).isEqualByComparingTo("5");
        JsonNode risk = summary.get("byRiskLevel");
        assertThat(risk.get("low").asLong() + risk.get("medium").asLong() + risk.get("high").asLong()).isEqualTo(3);
        assertThat(summary.at("/byMerchant/0/transactionCount").asLong()).isEqualTo(3);

        assertError(http.getForEntity("/v1/imports/" + UUID.randomUUID() + "/summary", String.class), 404,
                "IMPORT_NOT_FOUND");
    }

    // ---- health and metrics --------------------------------------------------

    @Test
    void livenessAndReadinessAreUpWithoutKafkaOrRedis() throws Exception {
        ResponseEntity<String> live = http.getForEntity("/health/live", String.class);
        assertThat(live.getStatusCode().value()).isEqualTo(200);
        assertThat(json.readTree(live.getBody()).get("status").asText()).isEqualTo("UP");

        ResponseEntity<String> ready = http.getForEntity("/health/ready", String.class);
        assertThat(ready.getStatusCode().value()).as(ready.getBody()).isEqualTo(200);
        JsonNode body = json.readTree(ready.getBody());
        assertThat(body.get("status").asText()).isEqualTo("UP");
        assertThat(body.at("/components/db/status").asText()).isEqualTo("UP");
        assertThat(body.at("/components/storage/status").asText()).isEqualTo("UP");
        assertThat(body.at("/components/worker/details/role").asText()).isEqualTo("api-only");
        assertThat(body.at("/components/kafka").isMissingNode()).isTrue();
    }

    @Test
    void metricsAreExposedInPrometheusFormat() {
        upload("metrics-1", Fixtures.validLines(2));
        http.getForEntity("/health/live", String.class);

        ResponseEntity<String> r = http.getForEntity("/metrics", String.class);

        assertThat(r.getStatusCode().value()).isEqualTo(200);
        assertThat(r.getHeaders().getContentType()).isNotNull();
        assertThat(r.getHeaders().getContentType().toString()).startsWith("text/plain");
        assertThat(r.getBody())
                .contains("http_server_requests_seconds_count")
                .contains("import_queue_depth")
                .contains("upload_slots_capacity")
                // "imports.created": the Prometheus client drops the reserved "_created" suffix
                .contains("imports_total")
                .contains("imports_replayed_total")
                .contains("application=\"transaction-reconciliation-service\"");
    }

    // ---- crash and resume -----------------------------------------------------

    /**
     * Worker A commits four batches and then vanishes without releasing or finishing
     * (from the database's point of view, exactly what a killed process leaves: a
     * live lease and a checkpoint). Worker B cannot take over until the lease expires,
     * then resumes from the checkpoint. Every record is counted exactly once.
     */
    @Test
    void anImportAbandonedMidwayIsResumedFromItsCheckpointAfterTheLeaseExpires() throws Exception {
        // 100 lines: 90 valid, 5 invalid JSON (lines 7, 27, ...), 5 identical repeats of
        // earlier ids (lines 13, 33, ...) of which some land before the crash, some after.
        StringBuilder file = new StringBuilder();
        int valid = 0;
        for (int line = 1; line <= 100; line++) {
            if (line % 20 == 7) {
                file.append("{\"transactionId\": not json\n");
            } else if (line % 20 == 13) {
                file.append(Fixtures.line("v-" + (line / 20 + 1))).append('\n');
            } else {
                file.append(Fixtures.line("v-" + ++valid)).append('\n');
            }
        }
        UUID id = idOf(upload("crash-1", file.toString()));

        AtomicInteger committed = new AtomicInteger();
        BatchWriter crashesAfterFourBatches = batch -> {
            if (committed.get() >= 4) {
                throw new LeaseLostException(batch.importId(), batch.workerId()); // the process "dies" here
            }
            BatchWriter.BatchOutcome outcome = batchWriter.write(batch);
            committed.incrementAndGet();
            return outcome;
        };
        assertThat(process(id, "worker-a", crashesAfterFourBatches)).isEqualTo(ProcessingOutcome.ABANDONED);

        JsonNode mid = getJson("/v1/imports/" + id);
        assertThat(mid.get("status").asText()).isEqualTo("processing");
        assertThat(mid.at("/progress/processed").asLong()).isEqualTo(40);
        assertThat(imports.findById(id).orElseThrow().lastLineCommitted()).isEqualTo(40);

        // The lease is still live: a second worker cannot steal the import.
        assertThat(queue.claim(id, "worker-b")).isEmpty();

        clock.advance(LEASE.plusSeconds(1));
        ImportJob resumed = queue.claim(id, "worker-b").orElseThrow();
        assertThat(resumed.attemptCount()).isEqualTo(2);
        assertThat(resumed.lastLineCommitted()).isEqualTo(40);
        assertThat(useCase(batchWriter).execute(resumed, "worker-b", new CancellationToken()))
                .isEqualTo(ProcessingOutcome.COMPLETED);

        JsonNode done = getJson("/v1/imports/" + id);
        assertThat(done.get("status").asText()).isEqualTo("completed");
        assertThat(done.at("/progress/processed").asLong()).isEqualTo(100);
        assertThat(done.at("/progress/accepted").asLong()).isEqualTo(90);
        assertThat(done.at("/progress/rejected").asLong()).isEqualTo(5);
        assertThat(done.at("/progress/duplicates").asLong()).isEqualTo(5);
        assertThat(transactionCount(id)).isEqualTo(90);
        assertThat(PG.db().fetchCount(PG.db().selectDistinct(TRANSACTIONS.TRANSACTION_ID).from(TRANSACTIONS)
                .where(TRANSACTIONS.IMPORT_ID.eq(id)))).isEqualTo(90);

        JsonNode summary = getJson("/v1/imports/" + id + "/summary");
        assertThat(summary.at("/totals/accepted").asLong()).isEqualTo(90);
        assertThat(currencyTotal(summary, "USD")).isEqualByComparingTo(new BigDecimal("145.75").multiply(BigDecimal.valueOf(90)));

        JsonNode rejections = getJson("/v1/imports/" + id + "/rejections?limit=200");
        assertThat(rejections.get("items")).hasSize(10);
        assertThat(rejections.get("nextCursor").isNull()).isTrue();
    }

    // ---- helpers ---------------------------------------------------------------

    private ProcessImportUseCase useCase(BatchWriter writer) {
        return new ProcessImportUseCase(imports, queue, storage, new DirectRiskScorer(), writer, retryPolicy, classifier,
                clock, metrics, new ProcessImportUseCase.Settings(BATCH_SIZE, 2, 16384, 512, 3));
    }

    private ProcessingOutcome process(UUID id, String workerId, BatchWriter writer) {
        ImportJob job = queue.claim(id, workerId).orElseThrow();
        return useCase(writer).execute(job, workerId, new CancellationToken());
    }

    private ResponseEntity<String> upload(String idempotencyKey, String content) {
        return upload(idempotencyKey, content.getBytes(StandardCharsets.UTF_8));
    }

    private ResponseEntity<String> upload(String idempotencyKey, byte[] content) {
        HttpHeaders h = headers(idempotencyKey, MediaType.MULTIPART_FORM_DATA);
        h.set("X-Provider-Id", Fixtures.PROVIDER);
        return post("/v1/imports", fileForm(content), h);
    }

    private static MultiValueMap<String, Object> fileForm(byte[] content) {
        MultiValueMap<String, Object> form = new LinkedMultiValueMap<>();
        form.add("file", new ByteArrayResource(content) {
            @Override
            public String getFilename() {
                return "transactions.ndjson";
            }
        });
        return form;
    }

    private static HttpHeaders headers(String idempotencyKey, MediaType contentType) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(contentType);
        if (idempotencyKey != null) {
            h.set("Idempotency-Key", idempotencyKey);
        }
        return h;
    }

    private ResponseEntity<String> post(String path, Object body, HttpHeaders headers) {
        return http.exchange(path, HttpMethod.POST, new HttpEntity<>(body, headers), String.class);
    }

    private JsonNode getJson(String path) throws IOException {
        ResponseEntity<String> r = http.getForEntity(path, String.class);
        assertThat(r.getStatusCode().value()).as("GET %s -> %s", path, r.getBody()).isEqualTo(200);
        return json.readTree(r.getBody());
    }

    private UUID idOf(ResponseEntity<String> response) throws IOException {
        assertThat(response.getStatusCode().value()).as(response.getBody()).isEqualTo(202);
        return UUID.fromString(json.readTree(response.getBody()).get("id").asText());
    }

    private void assertError(ResponseEntity<String> r, int status, String code) throws IOException {
        assertThat(r.getStatusCode().value()).as(r.getBody()).isEqualTo(status);
        JsonNode error = json.readTree(r.getBody()).get("error");
        assertThat(error.get("code").asText()).isEqualTo(code);
        assertThat(error.get("requestId").asText()).isNotBlank();
    }

    private static BigDecimal currencyTotal(JsonNode summary, String currency) {
        for (JsonNode c : summary.get("byCurrency")) {
            if (c.get("currency").asText().equals(currency)) {
                return c.get("totalAmount").decimalValue();
            }
        }
        throw new AssertionError("no " + currency + " in " + summary);
    }

    private static long transactionCount(UUID importId) {
        return PG.db().fetchCount(TRANSACTIONS, TRANSACTIONS.IMPORT_ID.eq(importId));
    }

    private static List<Path> storedFiles() throws IOException {
        Path uploads = STORAGE_ROOT.resolve("imports");
        if (!Files.exists(uploads)) {
            return List.of();
        }
        try (Stream<Path> files = Files.list(uploads)) {
            return files.toList();
        }
    }

    private static <T> List<T> concurrently(int threads, Callable<T> action) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<T>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return action.call();
                }));
            }
            start.countDown();
            List<T> results = new ArrayList<>();
            for (Future<T> f : futures) {
                results.add(f.get(30, TimeUnit.SECONDS));
            }
            return results;
        } finally {
            pool.shutdownNow();
        }
    }

    private static Path createStorageRoot() {
        try {
            return Files.createTempDirectory("import-api-it-");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void delete(Path p) {
        try {
            Files.deleteIfExists(p);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * {@link ImportJobQueue} without a broker: the same database transitions and leases
     * as {@code KafkaImportJobQueue}, minus the publish. Nothing consumes events in this
     * test; it drives claims explicitly.
     */
    static final class DatabaseOnlyImportJobQueue implements ImportJobQueue {

        private final JooqImportJobStore store;
        private final ImportRepository imports;
        private final Clock clock;
        private final Duration leaseDuration;

        DatabaseOnlyImportJobQueue(JooqImportJobStore store, ImportRepository imports, Clock clock, Duration leaseDuration) {
            this.store = store;
            this.imports = imports;
            this.clock = clock;
            this.leaseDuration = leaseDuration;
        }

        @Override
        public boolean enqueue(UUID importId) {
            return store.markPending(importId, clock.now());
        }

        @Override
        public void redispatch(UUID importId) {
            // nothing to publish
        }

        @Override
        public int redispatchStale(Duration pendingOlderThan, int limit) {
            Instant now = clock.now();
            return store.findNeedingDispatch(now, now.minus(pendingOlderThan), limit).size();
        }

        @Override
        public Optional<ImportJob> claim(UUID importId, String workerId) {
            return imports.claim(importId, workerId, leaseDuration, clock.now());
        }

        @Override
        public boolean heartbeat(UUID importId, String workerId) {
            return imports.extendLease(importId, workerId, leaseDuration, clock.now());
        }

        @Override
        public boolean release(UUID importId, String workerId) {
            return imports.release(importId, workerId, clock.now());
        }

        @Override
        public long queuedCount() {
            return store.queuedCount();
        }

        @Override
        public long inFlightCount() {
            return store.inFlightCount(clock.now());
        }
    }
}
