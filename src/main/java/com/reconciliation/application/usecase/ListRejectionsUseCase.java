package com.reconciliation.application.usecase;

import com.reconciliation.application.port.ImportRepository;
import com.reconciliation.application.port.RejectionRepository;
import com.reconciliation.domain.error.AppException;
import com.reconciliation.domain.error.ErrorCode;
import com.reconciliation.domain.model.Rejection;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

/**
 * Cursor pagination over rejected records. The cursor is an opaque, URL-safe token that
 * encodes the last row id seen (keyset pagination), so each page is one indexed range
 * scan regardless of how deep the client has paged. Page size is capped.
 */
public class ListRejectionsUseCase {

    public static final int DEFAULT_LIMIT = 50;
    public static final int MAX_LIMIT = 200;

    public record Page(List<Rejection> items, String nextCursor) {}

    private final ImportRepository imports;
    private final RejectionRepository rejections;

    public ListRejectionsUseCase(ImportRepository imports, RejectionRepository rejections) {
        this.imports = imports;
        this.rejections = rejections;
    }

    public Page execute(UUID importId, Integer limit, String cursor) {
        imports.findById(importId).orElseThrow(() -> AppException.notFound("Import", importId));
        int size = limit == null ? DEFAULT_LIMIT : limit;
        if (size < 1 || size > MAX_LIMIT) {
            throw new AppException(ErrorCode.VALIDATION_FAILED, "limit must be between 1 and " + MAX_LIMIT);
        }
        long afterId = decode(cursor);
        RejectionRepository.Page page = rejections.list(importId, afterId, size);
        return new Page(page.items(), page.nextAfterId() == null ? null : encode(page.nextAfterId()));
    }

    static String encode(long afterId) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(("r:" + afterId).getBytes(StandardCharsets.US_ASCII));
    }

    static long decode(String cursor) {
        if (cursor == null || cursor.isBlank()) {
            return 0;
        }
        try {
            String raw = new String(Base64.getUrlDecoder().decode(cursor), StandardCharsets.US_ASCII);
            if (!raw.startsWith("r:")) {
                throw new IllegalArgumentException();
            }
            long id = Long.parseLong(raw.substring(2));
            if (id < 0) {
                throw new IllegalArgumentException();
            }
            return id;
        } catch (IllegalArgumentException e) {
            throw new AppException(ErrorCode.INVALID_CURSOR, "cursor is not valid");
        }
    }
}
