package com.reconciliation.api;

import com.reconciliation.api.dto.ErrorResponse;
import com.reconciliation.api.dto.ImportCreatedResponse;
import com.reconciliation.api.dto.ImportStatusResponse;
import com.reconciliation.api.dto.ImportSummaryResponse;
import com.reconciliation.api.dto.RejectionsPageResponse;
import com.reconciliation.application.usecase.CancelImportUseCase;
import com.reconciliation.application.usecase.CreateImportUseCase;
import com.reconciliation.application.usecase.GetImportSummaryUseCase;
import com.reconciliation.application.usecase.GetImportUseCase;
import com.reconciliation.application.usecase.ListRejectionsUseCase;
import com.reconciliation.application.usecase.RetryImportUseCase;
import com.reconciliation.domain.error.AppException;
import com.reconciliation.domain.error.ErrorCode;
import com.reconciliation.domain.model.ImportJob;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.UUID;

@RestController
@RequestMapping(path = "/v1/imports", produces = MediaType.APPLICATION_JSON_VALUE)
@Tag(name = "Imports", description = "Upload NDJSON transaction files and track their processing")
@SecurityRequirement(name = "apiKey")
public class ImportController {

    private final CreateImportUseCase createImport;
    private final GetImportUseCase getImport;
    private final CancelImportUseCase cancelImport;
    private final GetImportSummaryUseCase getSummary;
    private final ListRejectionsUseCase listRejections;
    private final RetryImportUseCase retryImport;
    private final ImportProgressStreamer progressStreamer;
    private final String providerHeader;
    private final String defaultProvider;

    public ImportController(CreateImportUseCase createImport, GetImportUseCase getImport,
                            CancelImportUseCase cancelImport, GetImportSummaryUseCase getSummary,
                            ListRejectionsUseCase listRejections, RetryImportUseCase retryImport,
                            ImportProgressStreamer progressStreamer, String providerHeader, String defaultProvider) {
        this.createImport = createImport;
        this.getImport = getImport;
        this.cancelImport = cancelImport;
        this.getSummary = getSummary;
        this.listRejections = listRejections;
        this.retryImport = retryImport;
        this.progressStreamer = progressStreamer;
        this.providerHeader = providerHeader;
        this.defaultProvider = defaultProvider;
    }

    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @Operation(
            summary = "Create an import",
            description = """
                    Uploads one NDJSON file (multipart field `file`) and queues it for asynchronous processing.
                    The body is streamed to storage; it is never held in memory. Repeating the request with the
                    same `Idempotency-Key` returns the original import (202) and does not read the body again.
                    The optional `X-Provider-Id` header scopes duplicate detection (defaults to `default`).""",
            parameters = {
                    @Parameter(name = "Idempotency-Key", in = ParameterIn.HEADER, required = true,
                            description = "Client-generated unique key, 1-128 chars of [A-Za-z0-9_.:-]"),
                    @Parameter(name = "X-Provider-Id", in = ParameterIn.HEADER, required = false,
                            description = "Provider the file comes from; part of the duplicate-detection key")
            },
            requestBody = @io.swagger.v3.oas.annotations.parameters.RequestBody(
                    content = @Content(mediaType = MediaType.MULTIPART_FORM_DATA_VALUE,
                            schema = @Schema(implementation = UploadForm.class))))
    @ApiResponse(responseCode = "202", description = "Import accepted (or replayed)",
            content = @Content(schema = @Schema(implementation = ImportCreatedResponse.class)))
    @ApiResponse(responseCode = "400", description = "Missing/invalid header, empty file or interrupted upload",
            content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    @ApiResponse(responseCode = "413", description = "File exceeds the configured size limit",
            content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    @ApiResponse(responseCode = "415", description = "Body is not NDJSON",
            content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    @ApiResponse(responseCode = "429", description = "Too many concurrent uploads",
            content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    public ResponseEntity<ImportCreatedResponse> create(
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            HttpServletRequest request) throws IOException {
        String provider = resolveProvider(request);
        MultipartUploadStreamer.FilePart part = MultipartUploadStreamer.firstFilePart(request);
        CreateImportUseCase.Result result = createImport.execute(new CreateImportUseCase.Command(
                idempotencyKey,
                provider,
                part.filename(),
                part.body()));
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(ImportCreatedResponse.from(result.job()));
    }

    @GetMapping("/{id}")
    @Operation(summary = "Get import status and progress")
    @ApiResponse(responseCode = "200", description = "Current persisted state")
    @ApiResponse(responseCode = "404", description = "Unknown import",
            content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    public ImportStatusResponse get(@PathVariable UUID id, HttpServletRequest request) {
        MDC.put("importId", id.toString());
        try {
            return ImportStatusResponse.from(authorizedImport(id, request));
        } finally {
            MDC.remove("importId");
        }
    }

    @PostMapping("/{id}/cancel")
    @Operation(summary = "Cancel an import",
            description = "Pending imports are cancelled immediately. Processing imports stop at the next batch boundary; "
                    + "batches already in flight are committed, nothing after them is. Idempotent.")
    @ApiResponse(responseCode = "202", description = "Cancellation requested or already in effect")
    @ApiResponse(responseCode = "409", description = "Import already finished",
            content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    public ResponseEntity<ImportStatusResponse> cancel(@PathVariable UUID id, HttpServletRequest request) {
        authorize(id, request);
        return ResponseEntity.accepted().body(ImportStatusResponse.from(cancelImport.execute(id)));
    }

    @PostMapping("/{id}/retry")
    @Operation(summary = "Retry a failed import",
            description = "Re-queues a failed import. Processing resumes from the last committed checkpoint: records "
                    + "already accepted, rejected or counted as duplicates are kept and never counted twice. Only "
                    + "imports whose file was stored completely can be retried (a failed upload is retried by "
                    + "uploading again with the same Idempotency-Key), and the number of retries per import is "
                    + "limited. Idempotent: retrying an import that is already re-queued returns its current state.")
    @ApiResponse(responseCode = "202", description = "Import re-queued (or already re-queued)")
    @ApiResponse(responseCode = "404", description = "Unknown import",
            content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    @ApiResponse(responseCode = "409", description = "Not failed, upload never completed, file purged, or retry limit reached",
            content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    public ResponseEntity<ImportStatusResponse> retry(@PathVariable UUID id, HttpServletRequest request) {
        authorize(id, request);
        return ResponseEntity.accepted().body(ImportStatusResponse.from(retryImport.execute(id)));
    }

    @GetMapping(path = "/{id}/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @Operation(summary = "Stream import progress (server-sent events)",
            description = "Sends a `progress` event with the same body as GET /v1/imports/{id} immediately, then "
                    + "whenever the persisted status or counters change, and closes the stream once the import is "
                    + "terminal. Idle streams receive a comment every 15 s. Streams close after 30 minutes; clients "
                    + "reconnect (the standard EventSource does this automatically).")
    @ApiResponse(responseCode = "200", description = "text/event-stream of `progress` events")
    @ApiResponse(responseCode = "404", description = "Unknown import",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorResponse.class)))
    @ApiResponse(responseCode = "429", description = "Too many open streams on this instance",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = ErrorResponse.class)))
    public ResponseEntity<SseEmitter> events(@PathVariable UUID id, HttpServletRequest request) {
        ImportJob job = authorizedImport(id, request);
        return ResponseEntity.ok()
                .header("Cache-Control", "no-cache")
                .header("X-Accel-Buffering", "no") // tell buffering proxies to pass events through immediately
                .body(progressStreamer.open(job));
    }

    @GetMapping("/{id}/summary")
    @Operation(summary = "Get reconciliation summary",
            description = "Derived from persisted results. Live while processing; cached once the import is terminal.")
    public ImportSummaryResponse summary(@PathVariable UUID id, HttpServletRequest request) {
        authorize(id, request);
        GetImportSummaryUseCase.Result r = getSummary.execute(id);
        return ImportSummaryResponse.from(r.job(), r.summary(), r.isFinal());
    }

    @GetMapping("/{id}/rejections")
    @Operation(summary = "List rejected records", description = "Cursor-paginated; includes duplicates. Max 200 per page.")
    @ApiResponse(responseCode = "400", description = "Invalid cursor or limit",
            content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    public RejectionsPageResponse rejections(@PathVariable UUID id,
                                             @RequestParam(required = false) @Parameter(example = "50") Integer limit,
                                             @RequestParam(required = false) String cursor,
                                             HttpServletRequest request) {
        authorize(id, request);
        ListRejectionsUseCase.Page page = listRejections.execute(id, limit, cursor);
        return RejectionsPageResponse.from(page.items(), page.nextCursor());
    }

    // ---- provider identity ----------------------------------------------------

    /**
     * The provider of a new import. With API keys enabled it is the key's provider, and an
     * {@code X-Provider-Id} that disagrees is refused; without, it is the header (or the default).
     */
    private String resolveProvider(HttpServletRequest request) {
        String header = request.getHeader(providerHeader);
        String claimed = header == null || header.isBlank() ? null : header.trim();
        String authenticated = authenticatedProvider(request);
        if (authenticated == null) {
            return claimed == null ? defaultProvider : claimed;
        }
        if (claimed != null && !claimed.equals(authenticated)) {
            throw new AppException(ErrorCode.PROVIDER_MISMATCH,
                    "The " + providerHeader + " header does not match the provider of the API key");
        }
        return authenticated;
    }

    /** Without API keys every import is visible (the original contract); with them, only the owner's. */
    private void authorize(UUID id, HttpServletRequest request) {
        if (authenticatedProvider(request) != null) {
            authorizedImport(id, request);
        }
    }

    /**
     * Loads the import and checks ownership. Another provider's import is reported as not
     * found rather than forbidden, so ids cannot be probed for existence.
     */
    private ImportJob authorizedImport(UUID id, HttpServletRequest request) {
        ImportJob job = getImport.execute(id);
        String authenticated = authenticatedProvider(request);
        if (authenticated != null && !authenticated.equals(job.providerId())) {
            throw AppException.notFound("Import", id);
        }
        return job;
    }

    private static String authenticatedProvider(HttpServletRequest request) {
        Object provider = request.getAttribute(ApiKeyAuthenticationFilter.PROVIDER_ATTRIBUTE);
        return provider == null ? null : provider.toString();
    }

    /** Only used to describe the multipart body in OpenAPI. */
    @Schema(name = "UploadForm")
    record UploadForm(@Schema(type = "string", format = "binary", description = "NDJSON file") String file) {}
}
