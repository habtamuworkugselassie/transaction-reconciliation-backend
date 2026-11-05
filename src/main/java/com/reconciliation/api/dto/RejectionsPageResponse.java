package com.reconciliation.api.dto;

import com.reconciliation.domain.model.Rejection;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;
import java.util.Map;

@Schema(name = "RejectionsPage")
public record RejectionsPageResponse(List<Item> items,
                                     @Schema(nullable = true, description = "Pass as ?cursor= to fetch the next page; null on the last page")
                                     String nextCursor) {

    public record Item(long lineNumber,
                       @Schema(example = "INVALID_CURRENCY") String reason,
                       @Schema(example = "Currency must be a supported three-letter code") String message,
                       @Schema(description = "Limited excerpt of the input, never the full line") Map<String, Object> rawValue) {}

    public static RejectionsPageResponse from(List<Rejection> items, String nextCursor) {
        return new RejectionsPageResponse(
                items.stream().map(r -> new Item(r.lineNumber(), r.reason().name(), r.message(), r.rawValue())).toList(),
                nextCursor);
    }
}
