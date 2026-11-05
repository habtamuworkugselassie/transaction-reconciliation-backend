package com.reconciliation.application.upload;

import java.io.IOException;
import java.io.InputStream;

/**
 * Lazily opened upload body. The use case opens it only after the idempotency key
 * has been reserved, so a replayed request never reads its body at all.
 */
@FunctionalInterface
public interface UploadSource {
    InputStream open() throws IOException;
}
