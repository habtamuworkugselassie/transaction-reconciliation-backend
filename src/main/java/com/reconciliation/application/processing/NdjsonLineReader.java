package com.reconciliation.application.processing;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Streams a file line by line with bounded memory.
 *
 * <p>Reads fixed 64 KiB chunks and splits on {@code \n}; only the bytes of the current
 * line are retained, and never more than {@code maxLineLength} of them. A longer line
 * is reported as {@link Line#tooLong()} with its content discarded, so a hostile
 * single-line file cannot grow the heap. Handles {@code \r\n}, a UTF-8 BOM, and a final
 * line without a trailing newline. Line numbers are physical (1-based) so they match
 * what a user sees in an editor.
 */
public final class NdjsonLineReader implements AutoCloseable {

    /** One physical line. {@code text} is empty when {@code tooLong}. */
    public record Line(long number, String text, boolean tooLong) {
        public boolean isBlank() {
            return !tooLong && text.isBlank();
        }
    }

    private static final int CHUNK = 64 * 1024;

    private final InputStream in;
    private final int maxLineLength;
    private final byte[] chunk = new byte[CHUNK];
    private byte[] lineBuf;
    private int lineLen;
    private int chunkPos;
    private int chunkLen;
    private boolean eof;
    private boolean first = true;
    private long lineNumber;
    private boolean overflow;

    public NdjsonLineReader(InputStream in, int maxLineLength) {
        this.in = in;
        this.maxLineLength = maxLineLength;
        this.lineBuf = new byte[Math.min(maxLineLength, 1024)];
    }

    /** Next line or {@code null} at end of input. */
    public Line next() throws IOException {
        lineLen = 0;
        overflow = false;
        boolean sawAny = false;
        while (true) {
            if (chunkPos >= chunkLen) {
                if (eof) {
                    return sawAny ? emit(false) : null;
                }
                int n = in.read(chunk);
                if (n < 0) {
                    eof = true;
                    continue;
                }
                chunkPos = 0;
                chunkLen = n;
                if (first) {
                    first = false;
                    if (n >= 3 && (chunk[0] & 0xFF) == 0xEF && (chunk[1] & 0xFF) == 0xBB && (chunk[2] & 0xFF) == 0xBF) {
                        chunkPos = 3;
                    }
                }
                continue;
            }
            sawAny = true;
            int start = chunkPos;
            int nl = -1;
            for (int i = start; i < chunkLen; i++) {
                if (chunk[i] == '\n') {
                    nl = i;
                    break;
                }
            }
            int end = nl >= 0 ? nl : chunkLen;
            append(start, end - start);
            chunkPos = nl >= 0 ? nl + 1 : chunkLen;
            if (nl >= 0) {
                return emit(true);
            }
        }
    }

    private void append(int from, int count) {
        if (overflow) {
            return;
        }
        if (lineLen + count > maxLineLength) {
            overflow = true;
            lineLen = 0;
            return;
        }
        if (lineLen + count > lineBuf.length) {
            int newSize = Math.min(maxLineLength, Math.max(lineBuf.length * 2, lineLen + count));
            byte[] bigger = new byte[newSize];
            System.arraycopy(lineBuf, 0, bigger, 0, lineLen);
            lineBuf = bigger;
        }
        System.arraycopy(chunk, from, lineBuf, lineLen, count);
        lineLen += count;
    }

    private Line emit(boolean terminated) {
        lineNumber++;
        if (overflow) {
            return new Line(lineNumber, "", true);
        }
        int len = lineLen;
        if (len > 0 && lineBuf[len - 1] == '\r') {
            len--;
        }
        return new Line(lineNumber, new String(lineBuf, 0, len, StandardCharsets.UTF_8), false);
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
