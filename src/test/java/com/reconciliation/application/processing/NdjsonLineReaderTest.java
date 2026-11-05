package com.reconciliation.application.processing;

import com.reconciliation.application.processing.NdjsonLineReader.Line;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NdjsonLineReaderTest {

    private static List<Line> readAll(String text, int maxLine) throws IOException {
        return readAll(new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8)), maxLine);
    }

    private static List<Line> readAll(InputStream in, int maxLine) throws IOException {
        List<Line> lines = new ArrayList<>();
        try (NdjsonLineReader reader = new NdjsonLineReader(in, maxLine)) {
            Line line;
            while ((line = reader.next()) != null) {
                lines.add(line);
            }
        }
        return lines;
    }

    @Test
    void splitsOnNewlineWithPhysicalLineNumbers() throws IOException {
        List<Line> lines = readAll("{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n", 1024);

        assertThat(lines).extracting(Line::number).containsExactly(1L, 2L, 3L);
        assertThat(lines).extracting(Line::text).containsExactly("{\"a\":1}", "{\"a\":2}", "{\"a\":3}");
        assertThat(lines).noneMatch(Line::tooLong);
    }

    @Test
    void finalLineWithoutNewlineIsReturned() throws IOException {
        List<Line> lines = readAll("first\nlast", 1024);

        assertThat(lines).extracting(Line::text).containsExactly("first", "last");
    }

    @Test
    void emptyLinesKeepTheirLineNumbersAndAreBlank() throws IOException {
        List<Line> lines = readAll("a\n\n   \nb\n", 1024);

        assertThat(lines).extracting(Line::number).containsExactly(1L, 2L, 3L, 4L);
        assertThat(lines.get(1).isBlank()).isTrue();
        assertThat(lines.get(2).isBlank()).isTrue();
        assertThat(lines.get(3).text()).isEqualTo("b");
    }

    @Test
    void handlesCrLfAndUtf8Bom() throws IOException {
        List<Line> lines = readAll("\uFEFF{\"a\":1}\r\n{\"a\":2}\r\n", 1024);

        assertThat(lines).extracting(Line::text).containsExactly("{\"a\":1}", "{\"a\":2}");
    }

    @Test
    void emptyInputYieldsNoLines() throws IOException {
        assertThat(readAll("", 1024)).isEmpty();
    }

    @Test
    void unicodeSurvivesChunkBoundaries() throws IOException {
        // A multi-byte character placed exactly across the 64 KiB chunk boundary.
        String prefix = "x".repeat(64 * 1024 - 1);
        String text = prefix + "東京\n" + "ጤና café\n";

        List<Line> lines = readAll(text, 1 << 20);

        assertThat(lines.get(0).text()).isEqualTo(prefix + "東京");
        assertThat(lines.get(1).text()).isEqualTo("ጤና café");
    }

    @Test
    void linesSpanningManyChunksAreReassembled() throws IOException {
        String big = "y".repeat(200 * 1024);
        List<Line> lines = readAll(big + "\nshort\n", 1 << 20);

        assertThat(lines.get(0).text()).hasSize(200 * 1024);
        assertThat(lines.get(1).text()).isEqualTo("short");
    }

    @Test
    void overLongLineIsReportedWithoutItsContentAndTheReaderContinues() throws IOException {
        String tooLong = "z".repeat(5000);
        List<Line> lines = readAll("ok\n" + tooLong + "\nafter\n", 1024);

        assertThat(lines).hasSize(3);
        assertThat(lines.get(1).tooLong()).isTrue();
        assertThat(lines.get(1).text()).isEmpty();
        assertThat(lines.get(1).number()).isEqualTo(2);
        assertThat(lines.get(2).text()).isEqualTo("after");
    }

    @Test
    void overLongFinalLineWithoutNewlineIsAlsoReported() throws IOException {
        List<Line> lines = readAll("ok\n" + "z".repeat(5000), 1024);

        assertThat(lines).hasSize(2);
        assertThat(lines.get(1).tooLong()).isTrue();
    }

    @Test
    void lineAtExactlyTheLimitIsAccepted() throws IOException {
        String exact = "e".repeat(1024);
        List<Line> lines = readAll(exact + "\n" + "e".repeat(1025) + "\n", 1024);

        assertThat(lines.get(0).tooLong()).isFalse();
        assertThat(lines.get(0).text()).hasSize(1024);
        assertThat(lines.get(1).tooLong()).isTrue();
    }

    @Test
    void smallReadsFromTheStreamAreHandled() throws IOException {
        // A stream that returns one byte per read() call: exercises chunk refills on every byte.
        byte[] data = "ab\ncd\n\nef".getBytes(StandardCharsets.UTF_8);
        InputStream trickle = new InputStream() {
            int i = 0;
            @Override public int read() { return i < data.length ? data[i++] & 0xFF : -1; }
            @Override public int read(byte[] b, int off, int len) {
                if (i >= data.length) return -1;
                b[off] = data[i++];
                return 1;
            }
        };

        assertThat(readAll(trickle, 1024)).extracting(Line::text).containsExactly("ab", "cd", "", "ef");
    }

    @Test
    void streamErrorsPropagate() {
        InputStream broken = new InputStream() {
            @Override public int read() throws IOException { throw new IOException("disk gone"); }
        };

        assertThatThrownBy(() -> readAll(broken, 1024)).isInstanceOf(IOException.class).hasMessage("disk gone");
    }
}
