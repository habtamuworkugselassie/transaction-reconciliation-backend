package com.reconciliation.api;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** Prometheus text exposition at the assignment's path. */
@RestController
@Tag(name = "Metrics")
public class MetricsController {

    private static final String PROMETHEUS_TEXT = "text/plain; version=0.0.4; charset=utf-8";

    private final PrometheusMeterRegistry registry;

    public MetricsController(PrometheusMeterRegistry registry) {
        this.registry = registry;
    }

    @GetMapping(value = "/metrics", produces = {PROMETHEUS_TEXT, MediaType.TEXT_PLAIN_VALUE})
    @Operation(summary = "Prometheus metrics", description = "HTTP, JVM, Hikari, Tomcat and import pipeline metrics")
    public String scrape() {
        return registry.scrape();
    }
}
