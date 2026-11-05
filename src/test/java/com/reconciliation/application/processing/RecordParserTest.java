package com.reconciliation.application.processing;

import com.reconciliation.application.processing.NdjsonLineReader.Line;
import com.reconciliation.domain.model.Rejection;
import com.reconciliation.domain.model.RejectionReason;
import com.reconciliation.domain.processing.TransactionValidator;
import com.reconciliation.domain.processing.ValidationResult;
import com.reconciliation.testing.Fixtures;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class RecordParserTest {

    private final RecordParser parser = new RecordParser(new TransactionValidator(Fixtures.PROVIDER), 64);

    private Rejection rejection(String text) {
        ValidationResult r = parser.parse(new Line(3, text, false));
        assertThat(r).isInstanceOf(ValidationResult.Invalid.class);
        return ((ValidationResult.Invalid) r).rejection();
    }

    @Test
    void validLineBecomesANormalisedTransaction() {
        ValidationResult r = parser.parse(new Line(3, Fixtures.line("txn-1"), false));

        assertThat(r).isInstanceOf(ValidationResult.Valid.class);
        var t = ((ValidationResult.Valid) r).transaction();
        assertThat(t.lineNumber()).isEqualTo(3);
        assertThat(t.transactionId()).isEqualTo("txn-1");
        assertThat(t.amount()).isEqualTo(new BigDecimal("145.7500"));
    }

    @Test
    void floatsAreReadAsBigDecimalWithoutBinaryRounding() {
        String line = Fixtures.line("txn-1", "0.1", "USD", "2026-07-20T10:25:00Z", null);
        var t = ((ValidationResult.Valid) parser.parse(new Line(1, line, false))).transaction();
        assertThat(t.amount()).isEqualTo(new BigDecimal("0.1000"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"{", "not json", "{\"a\":}", "{\"a\":1} trailing", "{\"a\":NaN}", "{\"a\":1 /* c */}",
            "{\"a\":1,\"a\":2}"})
    void malformedJsonIsRejectedWithATruncatedExcerpt(String text) {
        Rejection r = rejection(text);

        assertThat(r.reason()).isEqualTo(RejectionReason.INVALID_JSON);
        assertThat(r.lineNumber()).isEqualTo(3);
        assertThat(r.rawValue().get("raw")).isEqualTo(text);
    }

    @ParameterizedTest
    @ValueSource(strings = {"[1,2]", "42", "\"string\"", "null", "true"})
    void nonObjectJsonIsRejected(String text) {
        assertThat(rejection(text).reason()).isEqualTo(RejectionReason.NOT_AN_OBJECT);
    }

    @Test
    void tooLongLineIsRejectedWithoutAnyRawContent() {
        Rejection r = ((ValidationResult.Invalid) parser.parse(new Line(9, "", true))).rejection();

        assertThat(r.reason()).isEqualTo(RejectionReason.LINE_TOO_LONG);
        assertThat(r.lineNumber()).isEqualTo(9);
        assertThat(r.rawValue()).isEmpty();
    }

    @Test
    void rawExcerptIsBoundedAndStrippedOfControlCharacters() {
        String text = "{\"bad\u0007\":" + "9".repeat(500);
        Rejection r = rejection(text);

        String raw = (String) r.rawValue().get("raw");
        assertThat(raw).hasSize(65).endsWith("…").doesNotContain("\u0007");
    }

    @Test
    void deeplyNestedJsonIsRejectedRatherThanParsed() {
        String deep = "{\"transactionId\":" + "[".repeat(50) + "]".repeat(50) + "}";
        assertThat(rejection(deep).reason()).isEqualTo(RejectionReason.INVALID_JSON);
    }

    @Test
    void validationFailuresKeepOnlyTheTransactionIdAsExcerpt() {
        String line = Fixtures.line("txn-9", "-5", "USD", "2026-07-20T10:25:00Z", "private note");
        Rejection r = rejection(line);

        assertThat(r.reason()).isEqualTo(RejectionReason.INVALID_AMOUNT);
        assertThat(r.rawValue()).containsExactly(java.util.Map.entry("transactionId", "txn-9"));
    }
}
