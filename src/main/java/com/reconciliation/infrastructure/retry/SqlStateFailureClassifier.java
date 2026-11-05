package com.reconciliation.infrastructure.retry;

import com.reconciliation.application.port.LeaseLostException;
import com.reconciliation.application.port.TransientFailureClassifier;
import com.reconciliation.domain.error.AppException;
import com.reconciliation.domain.error.ErrorCategory;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionSystemException;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.util.concurrent.CancellationException;

/**
 * Classifies failures for the retry policy.
 *
 * <p>Transient: lost connections (SQLSTATE class 08), serialisation failures and
 * deadlocks (40001, 40P01), admin shutdown / crash recovery (57P01-57P03), lock and
 * query timeouts, and Spring's transient/recoverable data-access hierarchy.
 *
 * <p>Never transient: validation and business errors, lease loss, cancellation, unique
 * or check constraint violations (class 23) and any programming error. Retrying those
 * cannot succeed and would only delay the honest failure.
 */
public final class SqlStateFailureClassifier implements TransientFailureClassifier {

    @Override
    public boolean isTransient(Throwable failure) {
        Throwable t = failure;
        int depth = 0;
        while (t != null && depth++ < 10) {
            if (t instanceof LeaseLostException || t instanceof CancellationException || t instanceof InterruptedException) {
                return false;
            }
            if (t instanceof AppException app) {
                return app.category() == ErrorCategory.INFRASTRUCTURE && app.retryable();
            }
            if (t instanceof TransientDataAccessException || t instanceof RecoverableDataAccessException
                    || t instanceof DataAccessResourceFailureException || t instanceof CannotGetJdbcConnectionException
                    || t instanceof CannotCreateTransactionException || t instanceof QueryTimeoutException
                    || t instanceof CannotAcquireLockException || t instanceof PessimisticLockingFailureException) {
                return true;
            }
            if (t instanceof TransactionSystemException) {
                return true; // commit/rollback failed at the driver level
            }
            if (t instanceof SQLException sql) {
                return isTransientSqlState(sql.getSQLState()) || sql instanceof SQLTransientException
                        || sql instanceof SQLRecoverableException;
            }
            if (t instanceof IOException) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    static boolean isTransientSqlState(String state) {
        if (state == null || state.length() < 2) {
            return false;
        }
        String cls = state.substring(0, 2);
        return cls.equals("08")                   // connection exception
                || state.equals("40001")          // serialization failure
                || state.equals("40P01")          // deadlock detected
                || state.equals("55P03")          // lock not available
                || state.equals("57P01")          // admin shutdown
                || state.equals("57P02")          // crash shutdown
                || state.equals("57P03")          // cannot connect now
                || state.equals("57014")          // query canceled (statement timeout)
                || cls.equals("53");              // insufficient resources
    }
}
