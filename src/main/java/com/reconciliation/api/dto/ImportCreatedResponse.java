package com.reconciliation.api.dto;

import com.reconciliation.domain.model.ImportJob;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.UUID;

@Schema(name = "ImportCreated")
public record ImportCreatedResponse(
        @Schema(example = "7b0f3a3e-2c1d-4a0e-9f0a-1b2c3d4e5f60") UUID id,
        @Schema(example = "pending") String status,
        Instant createdAt) {

    public static ImportCreatedResponse from(ImportJob job) {
        return new ImportCreatedResponse(job.id(), job.status().wireName(), job.createdAt());
    }
}
