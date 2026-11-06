package com.reconciliation.api;

import com.reconciliation.api.dto.ErrorResponse;
import com.reconciliation.domain.error.AppException;
import com.reconciliation.domain.error.ErrorCategory;
import com.reconciliation.domain.error.ErrorCode;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

/**
 * Maps every failure to the public error contract.
 *
 * <p>Client-caused errors are logged at INFO/WARN without a stack trace; programming
 * errors are logged at ERROR with the trace and the request id, and the client sees
 * only {@code INTERNAL_ERROR}. Diagnostics attached to an {@link AppException} are
 * written to the log, never to the response.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(AppException.class)
    public ResponseEntity<ErrorResponse> handleApp(AppException e, HttpServletRequest request) {
        String requestId = RequestIdFilter.current(request);
        MDC.put("errorCode", e.code().name());
        try {
            if (e.category() == ErrorCategory.PROGRAMMING || e.category() == ErrorCategory.INFRASTRUCTURE) {
                log.error("{} {} code={} diagnostics={}", request.getMethod(), request.getRequestURI(),
                        e.code(), e.diagnostics(), e);
            } else {
                log.info("{} {} code={} message={} diagnostics={}", request.getMethod(), request.getRequestURI(),
                        e.code(), e.getMessage(), e.diagnostics());
            }
        } finally {
            MDC.remove("errorCode");
        }
        ResponseEntity<ErrorResponse> response = respond(e.code(), e.getMessage(), requestId);
        // 429s tell the client when to come back. The value is computed by the use case and
        // carried in the diagnostics, which are otherwise only logged.
        if (e.code().httpStatus() == 429 && e.diagnostics().get("retryAfterSeconds") instanceof Number seconds) {
            return ResponseEntity.status(response.getStatusCode()).headers(response.getHeaders())
                    .header(HttpHeaders.RETRY_AFTER, String.valueOf(Math.max(1, seconds.longValue())))
                    .body(response.getBody());
        }
        return response;
    }

    @ExceptionHandler(MissingRequestHeaderException.class)
    public ResponseEntity<ErrorResponse> handleMissingHeader(MissingRequestHeaderException e, HttpServletRequest request) {
        ErrorCode code = "Idempotency-Key".equalsIgnoreCase(e.getHeaderName())
                ? ErrorCode.IDEMPOTENCY_KEY_REQUIRED : ErrorCode.VALIDATION_FAILED;
        return respond(code, "Missing required header: " + e.getHeaderName(), RequestIdFilter.current(request));
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ErrorResponse> handleTypeMismatch(MethodArgumentTypeMismatchException e, HttpServletRequest request) {
        return respond(ErrorCode.VALIDATION_FAILED, "Invalid value for parameter '" + e.getName() + "'",
                RequestIdFilter.current(request));
    }

    @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
    public ResponseEntity<ErrorResponse> handleMediaType(HttpMediaTypeNotSupportedException e, HttpServletRequest request) {
        return respond(ErrorCode.IMPORT_FILE_TYPE_INVALID, "Content-Type must be multipart/form-data",
                RequestIdFilter.current(request));
    }

    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<ErrorResponse> handleMethod(HttpRequestMethodNotSupportedException e, HttpServletRequest request) {
        return ResponseEntity.status(HttpStatus.METHOD_NOT_ALLOWED).contentType(MediaType.APPLICATION_JSON)
                .body(ErrorResponse.of("METHOD_NOT_ALLOWED", "Method not allowed", RequestIdFilter.current(request)));
    }

    @ExceptionHandler(NoResourceFoundException.class)
    public ResponseEntity<ErrorResponse> handleNoResource(NoResourceFoundException e, HttpServletRequest request) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).contentType(MediaType.APPLICATION_JSON)
                .body(ErrorResponse.of("NOT_FOUND", "Resource not found", RequestIdFilter.current(request)));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleUnexpected(Exception e, HttpServletRequest request) {
        String requestId = RequestIdFilter.current(request);
        log.error("Unhandled error {} {} requestId={}", request.getMethod(), request.getRequestURI(), requestId, e);
        return respond(ErrorCode.INTERNAL_ERROR, "An unexpected error occurred", requestId);
    }

    private static ResponseEntity<ErrorResponse> respond(ErrorCode code, String message, String requestId) {
        return ResponseEntity.status(code.httpStatus())
                .contentType(MediaType.APPLICATION_JSON)
                .body(ErrorResponse.of(code.name(), message, requestId));
    }
}
