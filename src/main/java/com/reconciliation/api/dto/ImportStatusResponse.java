package com.reconciliation.api.dto;

import com.reconciliation.domain.model.ImportJob;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.UUID;

@Schema(name = "ImportStatus")
public record ImportStatusResponse(
        UUID id,
        @Schema(example = "processing", allowableValues = {"uploading", "pending", "processing", "completed", "failed", "cancelling", "cancelled"})
        String status,
        Progress progress,
        Instant createdAt,
        Instant startedAt,
        Instant completedAt,
        @Schema(nullable = true, example = "PROCESSING_FAILED") String failureReason) {

    public record Progress(long processed, long accepted, long rejected, long duplicates) {}

    public static ImportStatusResponse from(ImportJob job) {
        return new ImportStatusResponse(
                job.id(),
                job.status().wireName(),
                new Progress(job.progress().processed(), job.progress().accepted(),
                        job.progress().rejected(), job.progress().duplicates()),
                job.createdAt(),
                job.startedAt(),
                job.completedAt(),
                job.failureReason());
    }
}
