package com.reconciliation.application.upload;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PushbackInputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class ContentSnifferTest {

    private static PushbackInputStream wrap(byte[] bytes) {
        return ContentSniffer.wrap(new ByteArrayInputStream(bytes));
    }

    private static ContentSniffer.Verdict sniff(String text) throws IOException {
        return ContentSniffer.sniff(wrap(text.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void ndjsonStartsWithAnObject() throws IOException {
        assertThat(sniff("{\"a\":1}\n")).isEqualTo(ContentSniffer.Verdict.NDJSON);
        assertThat(sniff("\n\n  \t{\"a\":1}")).isEqualTo(ContentSniffer.Verdict.NDJSON);
        assertThat(sniff("\uFEFF{\"a\":1}")).as("UTF-8 BOM is tolerated").isEqualTo(ContentSniffer.Verdict.NDJSON);
    }

    @Test
    void emptyOrWhitespaceOnlyIsEmpty() throws IOException {
        assertThat(sniff("")).isEqualTo(ContentSniffer.Verdict.EMPTY);
        assertThat(sniff("   \n\r\n\t")).isEqualTo(ContentSniffer.Verdict.EMPTY);
    }

    @Test
    void textThatIsNotJsonIsRefused() throws IOException {
        assertThat(sniff("transactionId,amount\ntxn-1,5")).isEqualTo(ContentSniffer.Verdict.NOT_JSON);
        assertThat(sniff("[{\"a\":1}]")).as("a JSON array is not NDJSON").isEqualTo(ContentSniffer.Verdict.NOT_JSON);
        assertThat(sniff("<html>")).isEqualTo(ContentSniffer.Verdict.NOT_JSON);
    }

    @Test
    void binaryAndCompressedContentIsRefused() throws IOException {
        assertThat(ContentSniffer.sniff(wrap(new byte[]{0x1F, (byte) 0x8B, 8, 0}))).as("gzip").isEqualTo(ContentSniffer.Verdict.BINARY);
        assertThat(ContentSniffer.sniff(wrap("PK\u0003\u0004".getBytes(StandardCharsets.ISO_8859_1)))).as("zip").isEqualTo(ContentSniffer.Verdict.BINARY);
        assertThat(sniff("%PDF-1.7")).isEqualTo(ContentSniffer.Verdict.BINARY);
        assertThat(sniff("\n\u0000{\"a\":1}")).as("NUL before first token").isEqualTo(ContentSniffer.Verdict.BINARY);
    }

    @Test
    void probeBytesArePushedBackSoTheCallerReadsFromByteZero() throws IOException {
        String text = "{\"a\":1}\n" + "x".repeat(10_000);
        PushbackInputStream in = wrap(text.getBytes(StandardCharsets.UTF_8));

        ContentSniffer.sniff(in);

        assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8)).isEqualTo(text);
    }
}
