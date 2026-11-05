package com.reconciliation.application.upload;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;

/**
 * Decides whether a body plausibly is NDJSON by looking at its first bytes, because
 * neither the client-supplied MIME type nor the file name can be trusted.
 *
 * <p>Rules: no NUL bytes in the first {@value #PROBE_SIZE} bytes (binary), no
 * gzip/zip/PDF magic numbers (compression bombs are out of scope and refused), and
 * the first non-whitespace byte after an optional UTF-8 BOM must be '{'.
 */
public final class ContentSniffer {

    static final int PROBE_SIZE = 4096;

    public enum Verdict { NDJSON, EMPTY, BINARY, NOT_JSON }

    private ContentSniffer() {
    }

    /** Wraps {@code in} so the probe bytes are pushed back and the caller reads from byte 0. */
    public static PushbackInputStream wrap(InputStream in) {
        return new PushbackInputStream(in, PROBE_SIZE);
    }

    public static Verdict sniff(PushbackInputStream in) throws IOException {
        byte[] buf = new byte[PROBE_SIZE];
        int n = in.readNBytes(buf, 0, PROBE_SIZE);
        if (n > 0) {
            in.unread(buf, 0, n);
        }
        if (n == 0) {
            return Verdict.EMPTY;
        }
        if (n >= 2 && (buf[0] & 0xFF) == 0x1F && (buf[1] & 0xFF) == 0x8B) {
            return Verdict.BINARY; // gzip
        }
        if (n >= 2 && buf[0] == 'P' && buf[1] == 'K') {
            return Verdict.BINARY; // zip
        }
        if (n >= 4 && buf[0] == '%' && buf[1] == 'P' && buf[2] == 'D' && buf[3] == 'F') {
            return Verdict.BINARY;
        }
        int start = 0;
        if (n >= 3 && (buf[0] & 0xFF) == 0xEF && (buf[1] & 0xFF) == 0xBB && (buf[2] & 0xFF) == 0xBF) {
            start = 3; // UTF-8 BOM
        }
        for (int i = start; i < n; i++) {
            byte b = buf[i];
            if (b == 0) {
                return Verdict.BINARY;
            }
            if (b == ' ' || b == '\n' || b == '\r' || b == '\t') {
                continue;
            }
            return b == '{' ? Verdict.NDJSON : Verdict.NOT_JSON;
        }
        return Verdict.EMPTY; // only whitespace seen so far
    }
}
