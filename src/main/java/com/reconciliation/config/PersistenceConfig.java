package com.reconciliation.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.reconciliation.application.port.BatchWriter;
import com.reconciliation.application.port.Clock;
import com.reconciliation.application.port.ImportRepository;
import com.reconciliation.application.port.RejectionRepository;
import com.reconciliation.application.port.SummaryRepository;
import com.reconciliation.infrastructure.persistence.JooqBatchWriter;
import com.reconciliation.infrastructure.persistence.JooqImportRepository;
import com.reconciliation.infrastructure.persistence.JooqRejectionRepository;
import com.reconciliation.infrastructure.persistence.JooqSummaryRepository;
import org.jooq.DSLContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Wires the jOOQ adapters. The DSLContext comes from Spring Boot's jOOQ auto-configuration
 * and participates in Spring-managed transactions, so the batch writer's
 * TransactionTemplate spans every statement it issues.
 */
@Configuration
public class PersistenceConfig {

    @Bean
    ImportRepository importRepository(DSLContext db) {
        return new JooqImportRepository(db);
    }

    @Bean
    SummaryRepository summaryRepository(DSLContext db, ObjectMapper objectMapper) {
        return new JooqSummaryRepository(db, objectMapper);
    }

    @Bean
    RejectionRepository rejectionRepository(DSLContext db, ObjectMapper objectMapper) {
        return new JooqRejectionRepository(db, objectMapper);
    }

    @Bean
    BatchWriter batchWriter(DSLContext db, PlatformTransactionManager txManager, Clock clock) {
        return new JooqBatchWriter(db, new TransactionTemplate(txManager), clock);
    }
}
