package com.reconciliation;

import com.reconciliation.config.AppProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Entry point. The same binary runs in two roles, selected by configuration:
 *
 * <ul>
 *   <li>{@code api} – serves HTTP (uploads, status, summaries)</li>
 *   <li>{@code worker} – claims pending imports and processes them</li>
 * </ul>
 *
 * Both roles may be enabled in a single process for local development
 * ({@code app.processing.enabled=true}); docker-compose runs them separately.
 */
@SpringBootApplication
@EnableConfigurationProperties(AppProperties.class)
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
