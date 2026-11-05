package com.reconciliation.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.boot.actuate.health.HealthComponent;
import org.springframework.boot.actuate.health.HealthEndpoint;
import org.springframework.boot.actuate.health.Status;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Liveness and readiness probes.
 *
 * <ul>
 *   <li>{@code /health/live}: is the process alive and not deadlocked? Only checks the
 *       application's own liveness state; never touches the database, so a slow
 *       dependency can't get a healthy process restarted.</li>
 *   <li>{@code /health/ready}: may this instance receive traffic? Checks readiness state
 *       (flipped DOWN during graceful shutdown), database connectivity, upload storage and
 *       worker pool state. A failing readiness check removes the instance from load
 *       balancing without killing in-flight work.</li>
 * </ul>
 */
@RestController
@Tag(name = "Health")
public class HealthController {

    private final HealthEndpoint healthEndpoint;

    public HealthController(HealthEndpoint healthEndpoint) {
        this.healthEndpoint = healthEndpoint;
    }

    @GetMapping("/health/live")
    @Operation(summary = "Liveness probe", description = "200 while the process is alive; no dependency checks.")
    public ResponseEntity<HealthComponent> live() {
        return respond(healthEndpoint.healthForPath("liveness"));
    }

    @GetMapping("/health/ready")
    @Operation(summary = "Readiness probe", description = "200 only when the database and storage are reachable and the instance is not shutting down.")
    public ResponseEntity<HealthComponent> ready() {
        return respond(healthEndpoint.healthForPath("readiness"));
    }

    private static ResponseEntity<HealthComponent> respond(HealthComponent component) {
        Status status = component == null ? Status.UNKNOWN : component.getStatus();
        HttpStatus http = Status.UP.equals(status) ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE;
        return ResponseEntity.status(http).body(component);
    }
}
