package com.reconciliation.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * All tunables of the service in one validated, immutable structure.
 * Every limit that bounds memory, concurrency or queue growth lives here so the
 * README can point at a single place that documents how the limits were chosen.
 */
@Validated
@ConfigurationProperties(prefix = "app")
public record AppProperties(
        @Valid @NotNull Upload upload,
        @Valid @NotNull Storage storage,
        @Valid @NotNull Processing processing,
        @Valid @NotNull Risk risk,
        @Valid @NotNull Retry retry,
        @Valid @NotNull Shutdown shutdown,
        @Valid @NotNull Kafka kafka,
        @Valid @NotNull Cache cache,
        @Valid @NotNull Security security,
        @Valid @NotNull Retention retention,
        @Valid @NotNull Events events
) {

    /** Limits applied to the HTTP upload path. */
    public record Upload(
            /** Hard cap on the NDJSON payload; enforced while streaming, not after buffering. */
            @Min(1) @DefaultValue("209715200") long maxFileSizeBytes,
            /** Uploads admitted at once; the rest receive 429 immediately rather than queueing. */
            @Min(1) @Max(256) @DefaultValue("4") int maxConcurrentUploads,
            /** Provider is taken from this header; it is part of the duplicate-detection key. */
            @NotBlank @DefaultValue("X-Provider-Id") String providerHeader,
            @NotBlank @DefaultValue("default") String defaultProvider,
            /** Uploads admitted per provider per window; 0 disables the limit. Enforced in Redis, fails open. */
            @Min(0) @Max(1000000) @DefaultValue("0") int providerRateLimit,
            @NotNull @DefaultValue("1m") Duration providerRateWindow
    ) {}

    /** Where uploaded files live. The abstraction allows an S3 adapter later. */
    public record Storage(
            @NotBlank @DefaultValue("./data/uploads") String root
    ) {}

    /** Background processing (worker role). */
    public record Processing(
            /** When false this instance only serves HTTP and never claims jobs. */
            @DefaultValue("true") boolean enabled,
            /** Imports processed simultaneously by this instance. */
            @Min(1) @Max(64) @DefaultValue("2") int maxConcurrentImports,
            /** Records per database batch; also the unit of checkpointing. */
            @Min(1) @Max(10000) @DefaultValue("1000") int batchSize,
            /** Batches parsed-but-not-yet-persisted per import; bounds memory per import. */
            @Min(1) @Max(16) @DefaultValue("2") int maxInFlightBatches,
            /** Lines longer than this are rejected without being materialised. */
            @Min(256) @Max(1048576) @DefaultValue("16384") int maxLineLength,
            /** Bytes of malformed raw input stored alongside a rejection. */
            @Min(32) @Max(8192) @DefaultValue("512") int maxRawValueLength,
            @NotNull @DefaultValue("1s") Duration pollInterval,
            /** A job whose lease is older than this is considered abandoned and re-claimable. */
            @NotNull @DefaultValue("30s") Duration leaseDuration,
            @NotNull @DefaultValue("10s") Duration heartbeatInterval,
            /** An upload that has not completed within this window is marked failed. */
            @NotNull @DefaultValue("15m") Duration staleUploadTimeout,
            /** Processing attempts after which an import is marked failed. */
            @Min(1) @Max(20) @DefaultValue("3") int maxAttempts,
            /** How often the worker re-publishes pending imports nobody picked up and expired leases. */
            @NotNull @DefaultValue("60s") Duration redispatchInterval,
            /** Client-requested retries (POST .../retry) allowed per import. */
            @Min(0) @Max(20) @DefaultValue("3") int maxManualRetries
    ) {}

    /** CPU-bound risk scoring pool. */
    public record Risk(
            /** 0 = number of available processors. */
            @Min(0) @Max(256) @DefaultValue("0") int workerThreads,
            /** Pending scoring tasks allowed before producers block (backpressure). */
            @Min(1) @Max(1000000) @DefaultValue("4000") int queueCapacity,
            /** Hash iterations used to simulate CPU work; part of the documented algorithm. */
            @Min(1) @Max(100000) @DefaultValue("200") int hashIterations
    ) {}

    /** Retry policy for transient infrastructure failures. */
    public record Retry(
            @Min(1) @Max(20) @DefaultValue("5") int maxAttempts,
            @NotNull @DefaultValue("200ms") Duration initialBackoff,
            @NotNull @DefaultValue("5s") Duration maxBackoff,
            @Min(1) @Max(10) @DefaultValue("2") double multiplier,
            /** Fraction of the computed delay randomised to avoid retry storms. */
            @Min(0) @Max(1) @DefaultValue("0.25") double jitter
    ) {}

    /** Kafka dispatch of imports to workers. */
    public record Kafka(
            @NotBlank @DefaultValue("imports.requested") String topic,
            /** Upper bound on parallelism across all worker instances. */
            @Min(1) @Max(256) @DefaultValue("8") int partitions,
            @NotBlank @DefaultValue("import-workers") String consumerGroup,
            /** How long the API waits for the broker to acknowledge an event before answering 503. */
            @NotNull @DefaultValue("5s") Duration publishTimeout,
            /** Delay before a released (transiently failed) import is redelivered to the same consumer. */
            @NotNull @DefaultValue("10s") Duration redeliveryDelay,
            /** max.poll.interval.ms: the longest a single import may take before the broker rebalances. */
            @NotNull @DefaultValue("1h") Duration maxProcessingTime
    ) {}

    /** Redis cache TTLs. */
    public record Cache(
            /** Live (in-progress) summary; short so progress stays fresh. */
            @NotNull @DefaultValue("2s") Duration summaryLiveTtl,
            /** Final summary of a terminal import; immutable, so long. */
            @NotNull @DefaultValue("1h") Duration summaryFinalTtl,
            /** Idempotency-key -> import id; clients retry within hours, not days. */
            @NotNull @DefaultValue("24h") Duration idempotencyTtl
    ) {}

    /** Graceful shutdown. */
    public record Shutdown(
            /** Upper bound on waiting for in-flight batches before forcing exit. */
            @NotNull @DefaultValue("30s") Duration gracePeriod
    ) {}

    /**
     * Optional API-key authentication. When enabled, every /v1 request needs
     * {@code Authorization: Bearer <key>} (or {@code X-API-Key}); the key decides the
     * provider, and an import is visible only to the provider that created it.
     */
    public record Security(
            @DefaultValue("false") boolean apiKeysEnabled,
            /**
             * Comma-separated {@code <sha256-hex-of-key>:<providerId>} pairs. Only hashes are
             * configured, never the keys themselves.
             */
            @DefaultValue("") String apiKeys
    ) {}

    /** Deletion of uploaded files after an import has been terminal for a while (worker role). */
    public record Retention(
            @DefaultValue("false") boolean enabled,
            /** How long a terminal import keeps its uploaded file. */
            @NotNull @DefaultValue("7d") Duration uploadedFilesAfter,
            @NotNull @DefaultValue("1h") Duration interval,
            /** Imports purged per run; bounds the work and the transaction size of one run. */
            @Min(1) @Max(10000) @DefaultValue("100") int batchSize
    ) {}

    /** Server-sent progress events (GET /v1/imports/{id}/events). */
    public record Events(
            /** Open streams allowed per instance; further requests get 429. */
            @Min(1) @Max(10000) @DefaultValue("100") int maxConnections,
            /** How often each stream re-reads the import's persisted progress. */
            @NotNull @DefaultValue("1s") Duration pollInterval,
            /** Comment line sent when nothing changed, so proxies keep the connection open. */
            @NotNull @DefaultValue("15s") Duration heartbeatInterval,
            /** A stream is closed after this long even if the import is still running; clients reconnect. */
            @NotNull @DefaultValue("30m") Duration timeout
    ) {}
}
