package com.reconciliation.domain.error;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Structured application error.
 *
 * <p>{@link #getMessage()} is safe to show to a client. {@link #diagnostics()} holds
 * internal context for logs only and is never serialised into a response.
 */
public class AppException extends RuntimeException {

    private final ErrorCode code;
    private final Map<String, Object> diagnostics;

    public AppException(ErrorCode code, String safeMessage) {
        this(code, safeMessage, null, Map.of());
    }

    public AppException(ErrorCode code, String safeMessage, Throwable cause) {
        this(code, safeMessage, cause, Map.of());
    }

    public AppException(ErrorCode code, String safeMessage, Throwable cause, Map<String, Object> diagnostics) {
        super(safeMessage, cause);
        this.code = code;
        this.diagnostics = Collections.unmodifiableMap(new LinkedHashMap<>(diagnostics));
    }

    public ErrorCode code() {
        return code;
    }

    public ErrorCategory category() {
        return code.category();
    }

    public boolean retryable() {
        return code.retryable();
    }

    /** Internal-only context (ids, counts, SQL state). Never returned to callers. */
    public Map<String, Object> diagnostics() {
        return diagnostics;
    }

    public static AppException notFound(String what, Object id) {
        return new AppException(ErrorCode.IMPORT_NOT_FOUND, what + " not found", null, Map.of("id", String.valueOf(id)));
    }
}
