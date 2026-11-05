package com.reconciliation.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;

/** Public error contract. Never carries stack traces, SQL, paths or internal ids. */
@Schema(name = "Error")
public record ErrorResponse(Body error) {

    public record Body(
            @Schema(example = "IMPORT_FILE_TOO_LARGE") String code,
            @Schema(example = "The uploaded file exceeds the allowed size") String message,
            @Schema(example = "f3c1a7e2-9d7b-4a3c-8e2f-0b1c2d3e4f5a") String requestId) {}

    public static ErrorResponse of(String code, String message, String requestId) {
        return new ErrorResponse(new Body(code, message, requestId));
    }
}
