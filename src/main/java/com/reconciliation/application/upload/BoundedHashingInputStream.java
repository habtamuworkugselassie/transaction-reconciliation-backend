package com.reconciliation.application.upload;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Counts, hashes and bounds a stream in one pass. The size limit is enforced while
 * the bytes flow, so an oversized upload is cut off at the limit rather than after
 * it has been fully written to disk.
 */
public final class BoundedHashingInputStream extends FilterInputStream {

    /** Thrown when more than {@code maxBytes} would be read. */
    public static final class LimitExceededException extends IOException {
        public LimitExceededException(long limit) {
            super("Upload exceeds the limit of " + limit + " bytes");
        }
    }

    private final long maxBytes;
    private final MessageDigest digest;
    private long count;

    public BoundedHashingInputStream(InputStream in, long maxBytes) {
        super(in);
        this.maxBytes = maxBytes;
        try {
            this.digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is mandatory in every JVM", e);
        }
    }

    @Override
    public int read() throws IOException {
        int b = in.read();
        if (b >= 0) {
            account(1);
            digest.update((byte) b);
        }
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int n = in.read(b, off, len);
        if (n > 0) {
            account(n);
            digest.update(b, off, n);
        }
        return n;
    }

    private void account(int n) throws IOException {
        count += n;
        if (count > maxBytes) {
            throw new LimitExceededException(maxBytes);
        }
    }

    public long count() {
        return count;
    }

    public String sha256Hex() {
        return HexFormat.of().formatHex(digest.digest());
    }
}
