package com.reconciliation.infrastructure.retry;

import com.reconciliation.application.port.LeaseLostException;
import com.reconciliation.domain.error.AppException;
import com.reconciliation.domain.error.ErrorCode;
import org.junit.jupiter.api.Test;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.UUID;
import java.util.concurrent.CancellationException;

import static org.assertj.core.api.Assertions.assertThat;

class SqlStateFailureClassifierTest {

    private final SqlStateFailureClassifier classifier = new SqlStateFailureClassifier();

    @Test
    void connectionLossSerializationAndDeadlocksAreTransient() {
        assertThat(classifier.isTransient(new SQLException("conn", "08006"))).isTrue();
        assertThat(classifier.isTransient(new SQLException("serialize", "40001"))).isTrue();
        assertThat(classifier.isTransient(new SQLException("deadlock", "40P01"))).isTrue();
        assertThat(classifier.isTransient(new SQLException("admin shutdown", "57P01"))).isTrue();
        assertThat(classifier.isTransient(new SQLException("too many connections", "53300"))).isTrue();
        assertThat(classifier.isTransient(new SQLTransientConnectionException("pool timeout"))).isTrue();
        assertThat(classifier.isTransient(new CannotGetJdbcConnectionException("no connection"))).isTrue();
        assertThat(classifier.isTransient(new QueryTimeoutException("slow"))).isTrue();
        assertThat(classifier.isTransient(new CannotAcquireLockException("lock"))).isTrue();
        assertThat(classifier.isTransient(new IOException("storage"))).isTrue();
    }

    @Test
    void transientCausesAreFoundInsideWrappers() {
        RuntimeException wrapped = new RuntimeException("outer", new IllegalStateException(new SQLException("x", "08001")));
        assertThat(classifier.isTransient(wrapped)).isTrue();
    }

    @Test
    void constraintViolationsValidationAndBugsAreNotTransient() {
        assertThat(classifier.isTransient(new SQLException("dup", "23505"))).isFalse();
        assertThat(classifier.isTransient(new DuplicateKeyException("dup"))).isFalse();
        assertThat(classifier.isTransient(new DataIntegrityViolationException("check"))).isFalse();
        assertThat(classifier.isTransient(new IllegalArgumentException("bad"))).isFalse();
        assertThat(classifier.isTransient(new NullPointerException())).isFalse();
        assertThat(classifier.isTransient(new AppException(ErrorCode.VALIDATION_FAILED, "bad input"))).isFalse();
        assertThat(classifier.isTransient(new AppException(ErrorCode.IMPORT_NOT_FOUND, "missing"))).isFalse();
    }

    @Test
    void retryableInfrastructureAppErrorsAreTransientButOthersAreNot() {
        assertThat(classifier.isTransient(new AppException(ErrorCode.STORAGE_FAILURE, "disk"))).isTrue();
        assertThat(classifier.isTransient(new AppException(ErrorCode.DATABASE_UNAVAILABLE, "db"))).isTrue();
        assertThat(classifier.isTransient(new AppException(ErrorCode.PROCESSING_FAILED, "permanent"))).isFalse();
        assertThat(classifier.isTransient(new AppException(ErrorCode.INTERNAL_ERROR, "bug"))).isFalse();
    }

    @Test
    void leaseLossAndCancellationAreNeverTransientEvenWhenCausedByATransientError() {
        assertThat(classifier.isTransient(new LeaseLostException(UUID.randomUUID(), "w"))).isFalse();
        assertThat(classifier.isTransient(new CancellationException("stop"))).isFalse();
        assertThat(classifier.isTransient(new InterruptedException())).isFalse();
        assertThat(classifier.isTransient(new RuntimeException(new LeaseLostException(UUID.randomUUID(), "w")))).isFalse();
    }

    @Test
    void sqlStateParsing() {
        assertThat(SqlStateFailureClassifier.isTransientSqlState(null)).isFalse();
        assertThat(SqlStateFailureClassifier.isTransientSqlState("0")).isFalse();
        assertThat(SqlStateFailureClassifier.isTransientSqlState("57014")).isTrue();
        assertThat(SqlStateFailureClassifier.isTransientSqlState("22P02")).as("invalid text representation").isFalse();
    }
}
