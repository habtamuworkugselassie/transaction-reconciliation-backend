package com.reconciliation.infrastructure.messaging;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.concurrent.TimeUnit;

/** Readiness contribution: the broker must answer a cluster description within a short timeout. */
public final class KafkaHealthIndicator implements HealthIndicator {

    private final KafkaAdmin admin;

    public KafkaHealthIndicator(KafkaAdmin admin) {
        this.admin = admin;
    }

    @Override
    public Health health() {
        try (AdminClient client = AdminClient.create(admin.getConfigurationProperties())) {
            var cluster = client.describeCluster(new DescribeClusterOptions().timeoutMs(2000));
            String clusterId = cluster.clusterId().get(3, TimeUnit.SECONDS);
            int nodes = cluster.nodes().get(3, TimeUnit.SECONDS).size();
            return Health.up().withDetail("clusterId", clusterId).withDetail("nodes", nodes).build();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Health.down().withDetail("reason", "interrupted").build();
        } catch (Exception e) {
            return Health.down().withDetail("reason", e.getClass().getSimpleName()).build();
        }
    }
}
