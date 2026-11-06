import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Repeatable benchmark against a running instance (no build needed):
 *
 *   java scripts/Benchmark.java --file=data/transactions-500000.ndjson [--base-url=http://localhost:8080]
 *                               [--metrics-url=http://localhost:8081/metrics] [--probe-interval-ms=250]
 *
 * It uploads the file, then until the import is terminal it concurrently:
 *   - polls GET /v1/imports/{id} once a second (progress, throughput),
 *   - probes GET /health/ready and GET /v1/imports/{id} every probe interval and records
 *     their latency (API responsiveness while the import runs),
 *   - scrapes /metrics for heap, CPU, scorer and Hikari gauges (peak values).
 * Finally it prints a report and appends it to benchmark-results/<timestamp>.md.
 *
 * --metrics-url defaults to the API's own /metrics; point it at the worker (8081) when the
 * API and worker are separate processes so heap/CPU reflect the process doing the work.
 */
public final class Benchmark {

    public static void main(String[] args) throws Exception {
        Path file = null;
        String baseUrl = "http://localhost:8080";
        String metricsUrl = null;
        long probeIntervalMs = 250;
        for (String a : args) {
            String[] kv = a.split("=", 2);
            String v = kv.length > 1 ? kv[1] : "";
            switch (kv[0]) {
                case "--file" -> file = Path.of(v);
                case "--base-url" -> baseUrl = v.replaceAll("/+$", "");
                case "--metrics-url" -> metricsUrl = v;
                case "--probe-interval-ms" -> probeIntervalMs = Long.parseLong(v);
                default -> throw new IllegalArgumentException("Unknown option " + a);
            }
        }
        if (file == null || !Files.exists(file)) {
            System.err.println("--file=<ndjson> is required (generate one with scripts/generate-data.sh)");
            System.exit(2);
        }
        if (metricsUrl == null) {
            metricsUrl = baseUrl + "/metrics";
        }
        long lines;
        try (var s = Files.lines(file, StandardCharsets.UTF_8)) {
            lines = s.count();
        }
        HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

        // --- upload ---------------------------------------------------------------
        String boundary = "----bench" + UUID.randomUUID();
        byte[] head = ("--" + boundary + "\r\nContent-Disposition: form-data; name=\"file\"; filename=\""
                + file.getFileName() + "\"\r\nContent-Type: application/x-ndjson\r\n\r\n").getBytes(StandardCharsets.UTF_8);
        byte[] tail = ("\r\n--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8);
        HttpRequest upload = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/imports"))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .header("Idempotency-Key", "bench-" + UUID.randomUUID())
                .timeout(Duration.ofMinutes(10))
                .POST(HttpRequest.BodyPublishers.concat(
                        HttpRequest.BodyPublishers.ofByteArray(head),
                        HttpRequest.BodyPublishers.ofFile(file),
                        HttpRequest.BodyPublishers.ofByteArray(tail)))
                .build();
        Instant t0 = Instant.now();
        HttpResponse<String> created = http.send(upload, HttpResponse.BodyHandlers.ofString());
        Duration uploadTime = Duration.between(t0, Instant.now());
        if (created.statusCode() != 202) {
            System.err.println("Upload failed: " + created.statusCode() + " " + created.body());
            System.exit(1);
        }
        String importId = extract(created.body(), "\"id\":\"([^\"]+)\"");
        System.out.printf(Locale.ROOT, "Uploaded %s (%d lines, %.1f MB) in %d ms -> import %s%n",
                file, lines, Files.size(file) / 1048576.0, uploadTime.toMillis(), importId);

        // --- probes + metrics while processing ------------------------------------
        List<Long> readyLatencies = Collections.synchronizedList(new ArrayList<>());
        List<Long> statusLatencies = Collections.synchronizedList(new ArrayList<>());
        AtomicBoolean done = new AtomicBoolean(false);
        Peaks peaks = new Peaks();
        final String mUrl = metricsUrl;
        final long interval = probeIntervalMs;
        final String readyUrl = baseUrl + "/health/ready";
        final String statusUrl = baseUrl + "/v1/imports/" + importId;
        Thread prober = Thread.ofVirtual().start(() -> {
            while (!done.get()) {
                readyLatencies.add(timed(http, readyUrl));
                statusLatencies.add(timed(http, statusUrl));
                sleep(interval);
            }
        });
        Thread scraper = Thread.ofVirtual().start(() -> {
            while (!done.get()) {
                try {
                    String body = http.send(HttpRequest.newBuilder(URI.create(mUrl)).GET().build(),
                            HttpResponse.BodyHandlers.ofString()).body();
                    peaks.observe(body);
                } catch (Exception ignored) {
                    // metrics endpoint unavailable this tick
                }
                sleep(1000);
            }
        });

        Instant processingStart = null;
        Instant processingEnd = null;
        String status = "pending";
        long processed = 0;
        long accepted = 0;
        long rejected = 0;
        long duplicates = 0;
        long lastProcessed = 0;
        Instant lastTick = Instant.now();
        double peakRate = 0;
        while (true) {
            HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(baseUrl + "/v1/imports/" + importId)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            String body = r.body();
            status = extract(body, "\"status\":\"([^\"]+)\"");
            processed = Long.parseLong(extract(body, "\"processed\":(\\d+)"));
            accepted = Long.parseLong(extract(body, "\"accepted\":(\\d+)"));
            rejected = Long.parseLong(extract(body, "\"rejected\":(\\d+)"));
            duplicates = Long.parseLong(extract(body, "\"duplicates\":(\\d+)"));
            String startedAt = extract(body, "\"startedAt\":\"([^\"]+)\"");
            if (startedAt != null && processingStart == null) {
                processingStart = Instant.parse(startedAt);
            }
            Instant now = Instant.now();
            double rate = (processed - lastProcessed) * 1000.0 / Math.max(1, Duration.between(lastTick, now).toMillis());
            peakRate = Math.max(peakRate, rate);
            lastProcessed = processed;
            lastTick = now;
            System.out.printf(Locale.ROOT, "  %-10s processed=%,d accepted=%,d rejected=%,d duplicates=%,d  (%.0f rec/s)%n",
                    status, processed, accepted, rejected, duplicates, rate);
            if (status.equals("completed") || status.equals("failed") || status.equals("cancelled")) {
                String completedAt = extract(body, "\"completedAt\":\"([^\"]+)\"");
                processingEnd = completedAt == null ? now : Instant.parse(completedAt);
                break;
            }
            sleep(1000);
        }
        done.set(true);
        prober.join();
        scraper.join();

        // --- report ------------------------------------------------------------------
        Duration processing = Duration.between(processingStart == null ? t0 : processingStart, processingEnd);
        double avgRate = processed * 1000.0 / Math.max(1, processing.toMillis());
        StringBuilder md = new StringBuilder();
        md.append("# Benchmark run ").append(Instant.now()).append("\n\n");
        md.append("| Metric | Value |\n|---|---|\n");
        md.append(row("File", file + " (" + String.format(Locale.ROOT, "%.1f", Files.size(file) / 1048576.0) + " MB)"));
        md.append(row("Lines", String.format(Locale.ROOT, "%,d", lines)));
        md.append(row("Final status", status));
        md.append(row("Processed / accepted / rejected / duplicates",
                String.format(Locale.ROOT, "%,d / %,d / %,d / %,d", processed, accepted, rejected, duplicates)));
        md.append(row("Upload time", uploadTime.toMillis() + " ms"));
        md.append(row("Processing duration", String.format(Locale.ROOT, "%.1f s", processing.toMillis() / 1000.0)));
        md.append(row("Average throughput", String.format(Locale.ROOT, "%,.0f records/s", avgRate)));
        md.append(row("Peak 1s throughput", String.format(Locale.ROOT, "%,.0f records/s", peakRate)));
        md.append(row("GET /health/ready latency p50 / p95 / p99 / max (ms)", pct(readyLatencies)));
        md.append(row("GET /v1/imports/{id} latency p50 / p95 / p99 / max (ms)", pct(statusLatencies)));
        md.append(row("Peak JVM heap used", String.format(Locale.ROOT, "%.0f MB", peaks.heapUsed / 1048576.0)));
        md.append(row("Peak process RSS (if exposed)", peaks.rss > 0 ? String.format(Locale.ROOT, "%.0f MB", peaks.rss / 1048576.0) : "n/a"));
        md.append(row("Peak process CPU", String.format(Locale.ROOT, "%.0f%%", peaks.cpu * 100)));
        md.append(row("Peak scorer threads active / pool size", peaks.scorerActive + " / " + peaks.scorerPool));
        md.append(row("Peak scorer queue depth", String.valueOf(peaks.scorerQueue)));
        md.append(row("Peak Hikari active connections / max", peaks.hikariActive + " / " + peaks.hikariMax));
        md.append(row("Peak worker active imports", String.valueOf(peaks.workerActive)));
        md.append(row("Machine", Runtime.getRuntime().availableProcessors() + " CPUs visible to this JVM, "
                + System.getProperty("os.name") + ", Java " + System.getProperty("java.version")));
        System.out.println();
        System.out.print(md);
        Path out = Path.of("benchmark-results", "run-" + Instant.now().toString().replace(':', '-') + ".md");
        Files.createDirectories(out.getParent());
        Files.writeString(out, md.toString());
        System.out.println("\nSaved " + out);
    }

    private static String row(String k, String v) {
        return "| " + k + " | " + v + " |\n";
    }

    private static String pct(List<Long> samples) {
        if (samples.isEmpty()) {
            return "n/a";
        }
        List<Long> s = new ArrayList<>(samples);
        Collections.sort(s);
        return String.format(Locale.ROOT, "%d / %d / %d / %d", s.get(s.size() / 2), s.get((int) (s.size() * 0.95)),
                s.get(Math.min(s.size() - 1, (int) (s.size() * 0.99))), s.get(s.size() - 1));
    }

    private static long timed(HttpClient http, String url) {
        long t = System.nanoTime();
        try {
            http.send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10)).GET().build(),
                    HttpResponse.BodyHandlers.discarding());
        } catch (Exception e) {
            return 10_000;
        }
        return (System.nanoTime() - t) / 1_000_000;
    }

    private static String extract(String body, String regex) {
        Matcher m = Pattern.compile(regex).matcher(body);
        return m.find() ? m.group(1) : null;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Peak values of selected Prometheus gauges across scrapes. */
    static final class Peaks {
        double heapUsed;
        double rss;
        double cpu;
        double scorerActive;
        double scorerPool;
        double scorerQueue;
        double hikariActive;
        double hikariMax;
        double workerActive;

        void observe(String text) {
            double heap = 0;
            for (String line : text.split("\n")) {
                if (line.startsWith("#")) {
                    continue;
                }
                if (line.startsWith("jvm_memory_used_bytes{") && line.contains("area=\"heap\"")) {
                    heap += value(line);
                } else if (line.startsWith("process_cpu_usage")) {
                    cpu = Math.max(cpu, value(line));
                } else if (line.startsWith("process_memory_rss_bytes") || line.startsWith("process_resident_memory_bytes")) {
                    rss = Math.max(rss, value(line));
                } else if (line.startsWith("risk_scorer_active")) {
                    scorerActive = Math.max(scorerActive, value(line));
                } else if (line.startsWith("risk_scorer_pool_size")) {
                    scorerPool = Math.max(scorerPool, value(line));
                } else if (line.startsWith("risk_scorer_queue_depth")) {
                    scorerQueue = Math.max(scorerQueue, value(line));
                } else if (line.startsWith("hikaricp_connections_active")) {
                    hikariActive = Math.max(hikariActive, value(line));
                } else if (line.startsWith("hikaricp_connections_max")) {
                    hikariMax = Math.max(hikariMax, value(line));
                } else if (line.startsWith("import_worker_active")) {
                    workerActive = Math.max(workerActive, value(line));
                }
            }
            heapUsed = Math.max(heapUsed, heap);
        }

        private static double value(String line) {
            try {
                return Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1));
            } catch (NumberFormatException e) {
                return 0;
            }
        }
    }
}
