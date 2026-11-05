package com.reconciliation.domain.processing;

import com.reconciliation.domain.model.NormalizedTransaction;
import com.reconciliation.domain.model.RejectionReason;
import com.reconciliation.testing.Fixtures;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Validation rules and the normalisation rules documented in ARCHITECTURE.md. */
class TransactionValidatorTest {

    private final TransactionValidator validator = new TransactionValidator(Fixtures.PROVIDER);

    private NormalizedTransaction valid(Map<String, Object> record) {
        ValidationResult r = validator.validate(7, record);
        assertThat(r).as("expected valid: " + record).isInstanceOf(ValidationResult.Valid.class);
        return ((ValidationResult.Valid) r).transaction();
    }

    private RejectionReason rejected(Map<String, Object> record) {
        ValidationResult r = validator.validate(7, record);
        assertThat(r).as("expected invalid: " + record).isInstanceOf(ValidationResult.Invalid.class);
        var rejection = ((ValidationResult.Invalid) r).rejection();
        assertThat(rejection.lineNumber()).isEqualTo(7);
        assertThat(rejection.message()).isNotBlank();
        return rejection.reason();
    }

    @Test
    void validRecordIsNormalised() {
        NormalizedTransaction t = valid(Fixtures.rawRecord("txn-1"));

        assertThat(t.lineNumber()).isEqualTo(7);
        assertThat(t.providerId()).isEqualTo(Fixtures.PROVIDER);
        assertThat(t.transactionId()).isEqualTo("txn-1");
        assertThat(t.amount()).isEqualTo(new BigDecimal("145.7500"));
        assertThat(t.amount().scale()).isEqualTo(TransactionValidator.AMOUNT_SCALE);
        assertThat(t.currency()).isEqualTo("USD");
        assertThat(t.timestamp()).isEqualTo(Instant.parse("2026-07-20T10:25:00Z"));
        assertThat(t.description()).isEqualTo("Subscription payment");
    }

    // ---- normalisation ---------------------------------------------------------

    @Test
    void stringsAreTrimmed() {
        var record = Fixtures.rawRecord("  txn-1 \t");
        record.put("accountId", " acc-201 ");
        record.put("merchantId", "\tmerchant-18\n");

        NormalizedTransaction t = valid(record);

        assertThat(t.transactionId()).isEqualTo("txn-1");
        assertThat(t.accountId()).isEqualTo("acc-201");
        assertThat(t.merchantId()).isEqualTo("merchant-18");
    }

    @Test
    void currencyIsUpperCased() {
        var record = Fixtures.rawRecord("txn-1");
        record.put("currency", " usd ");

        assertThat(valid(record).currency()).isEqualTo("USD");
    }

    @Test
    void timestampIsConvertedToUtcAndTruncatedToMillis() {
        var record = Fixtures.rawRecord("txn-1");
        record.put("timestamp", "2026-07-20T12:25:00.123456+02:00");

        assertThat(valid(record).timestamp()).isEqualTo(Instant.parse("2026-07-20T10:25:00.123Z"));
    }

    @Test
    void amountIsStoredWithScaleFour() {
        var record = Fixtures.rawRecord("txn-1");
        record.put("amount", 100);
        assertThat(valid(record).amount()).isEqualTo(new BigDecimal("100.0000"));

        record.put("amount", new BigDecimal("0.5"));
        assertThat(valid(record).amount()).isEqualTo(new BigDecimal("0.5000"));

        record.put("amount", 12.5d);
        assertThat(valid(record).amount()).isEqualTo(new BigDecimal("12.5000"));
    }

    @Test
    void descriptionWhitespaceIsCollapsedAndControlCharactersBecomeSpaces() {
        var record = Fixtures.rawRecord("txn-1");
        record.put("description", "  Sub\u0000scription \t\n  payment  ");

        assertThat(valid(record).description()).isEqualTo("Sub scription payment");
    }

    @Test
    void emptyOrMissingDescriptionIsNull() {
        var record = Fixtures.rawRecord("txn-1");
        record.put("description", "   ");
        assertThat(valid(record).description()).isNull();

        record.remove("description");
        assertThat(valid(record).description()).isNull();
    }

    @Test
    void unicodeContentIsPreserved() {
        var record = Fixtures.rawRecord("txn-東京");
        record.put("description", "Zürich café ጤና");

        NormalizedTransaction t = valid(record);
        assertThat(t.transactionId()).isEqualTo("txn-東京");
        assertThat(t.description()).isEqualTo("Zürich café ጤና");
    }

    @Test
    void unexpectedFieldsAreIgnored() {
        var record = Fixtures.rawRecord("txn-1");
        record.put("extra", Map.of("nested", true));
        record.put("__proto__", "x");

        valid(record);
    }

    @Test
    void logicallyIdenticalInputsNormaliseIdentically() {
        var a = Fixtures.rawRecord("txn-1");
        var b = Fixtures.rawRecord(" txn-1 ");
        b.put("currency", "usd");
        b.put("amount", new BigDecimal("145.750"));
        b.put("timestamp", "2026-07-20T13:25:00+03:00");
        b.put("description", " Subscription   payment ");

        assertThat(valid(b)).isEqualTo(valid(a));
    }

    // ---- rejections ------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(strings = {"transactionId", "accountId", "merchantId", "amount", "currency", "timestamp"})
    void missingRequiredFieldIsRejected(String field) {
        var record = Fixtures.rawRecord("txn-1");
        record.remove(field);
        assertThat(rejected(record)).isEqualTo(RejectionReason.MISSING_FIELD);
    }

    @ParameterizedTest
    @ValueSource(strings = {"transactionId", "accountId", "merchantId"})
    void blankIdentifierIsRejected(String field) {
        var record = Fixtures.rawRecord("txn-1");
        record.put(field, "   ");
        assertThat(rejected(record)).isEqualTo(RejectionReason.MISSING_FIELD);
    }

    @Test
    void wrongTypesAreRejected() {
        var record = Fixtures.rawRecord("txn-1");
        record.put("transactionId", 42);
        assertThat(rejected(record)).isEqualTo(RejectionReason.INVALID_TYPE);

        record = Fixtures.rawRecord("txn-1");
        record.put("amount", "145.75");
        assertThat(rejected(record)).isEqualTo(RejectionReason.INVALID_TYPE);

        record = Fixtures.rawRecord("txn-1");
        record.put("currency", 840);
        assertThat(rejected(record)).isEqualTo(RejectionReason.INVALID_TYPE);

        record = Fixtures.rawRecord("txn-1");
        record.put("timestamp", 1721471100000L);
        assertThat(rejected(record)).isEqualTo(RejectionReason.INVALID_TYPE);

        record = Fixtures.rawRecord("txn-1");
        record.put("description", Map.of());
        assertThat(rejected(record)).isEqualTo(RejectionReason.INVALID_TYPE);
    }

    @ParameterizedTest
    @CsvSource({"0", "-1", "-0.01", "1.23456", "1000000000000000", "9999999999999999"})
    void invalidAmountsAreRejected(String amount) {
        var record = Fixtures.rawRecord("txn-1");
        record.put("amount", new BigDecimal(amount));
        assertThat(rejected(record)).isEqualTo(RejectionReason.INVALID_AMOUNT);
    }

    @Test
    void nonFiniteAmountIsRejectedAsWrongType() {
        var record = Fixtures.rawRecord("txn-1");
        record.put("amount", Double.NaN);
        assertThat(rejected(record)).isEqualTo(RejectionReason.INVALID_TYPE);
        record.put("amount", Double.POSITIVE_INFINITY);
        assertThat(rejected(record)).isEqualTo(RejectionReason.INVALID_TYPE);
    }

    @ParameterizedTest
    @ValueSource(strings = {"US", "USDD", "XXX1", "ZZZ", "$", "", "US D"})
    void unsupportedCurrencyIsRejected(String currency) {
        var record = Fixtures.rawRecord("txn-1");
        record.put("currency", currency);
        assertThat(rejected(record)).isEqualTo(RejectionReason.INVALID_CURRENCY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"2026-07-20", "2026-07-20T10:25:00", "20/07/2026", "not a date", "2026-13-45T00:00:00Z",
            "2026-07-20T10:25:00.000Z-extra-characters-that-make-it-long"})
    void invalidTimestampIsRejected(String timestamp) {
        var record = Fixtures.rawRecord("txn-1");
        record.put("timestamp", timestamp);
        assertThat(rejected(record)).isEqualTo(RejectionReason.INVALID_TIMESTAMP);
    }

    @Test
    void overLongFieldsAreRejected() {
        var record = Fixtures.rawRecord("x".repeat(TransactionValidator.MAX_ID_LENGTH + 1));
        assertThat(rejected(record)).isEqualTo(RejectionReason.FIELD_TOO_LONG);

        record = Fixtures.rawRecord("txn-1");
        record.put("description", "d".repeat(TransactionValidator.MAX_DESCRIPTION_LENGTH + 1));
        assertThat(rejected(record)).isEqualTo(RejectionReason.DESCRIPTION_TOO_LONG);

        record = Fixtures.rawRecord("txn-1");
        record.put("description", "d".repeat(TransactionValidator.MAX_DESCRIPTION_LENGTH));
        valid(record);
    }

    @Test
    void controlCharactersInIdentifiersAreRejected() {
        var record = Fixtures.rawRecord("txn\u00001");
        assertThat(rejected(record)).isEqualTo(RejectionReason.INVALID_VALUE);

        record = Fixtures.rawRecord("txn-1");
        record.put("merchantId", "merchant\u001b[31m");
        assertThat(rejected(record)).isEqualTo(RejectionReason.INVALID_VALUE);
    }

    // ---- excerpt ---------------------------------------------------------------

    @Test
    void excerptContainsOnlyASanitisedTransactionId() {
        var record = Fixtures.rawRecord("txn-\u0007bell");
        record.put("description", "secret");

        Map<String, Object> excerpt = TransactionValidator.excerpt(record);

        assertThat(excerpt).containsOnlyKeys("transactionId");
        assertThat(excerpt.get("transactionId")).isEqualTo("txn-bell");
    }

    @Test
    void excerptTruncatesVeryLongIdsAndHidesNonStringIds() {
        var record = Fixtures.rawRecord("x".repeat(1000));
        assertThat((String) TransactionValidator.excerpt(record).get("transactionId"))
                .hasSize(TransactionValidator.MAX_ID_LENGTH + 1).endsWith("…");

        record.put("transactionId", Map.of("a", "b".repeat(100)));
        assertThat(TransactionValidator.excerpt(record).get("transactionId")).isEqualTo("<non-string>");

        record.put("transactionId", 12);
        assertThat(TransactionValidator.excerpt(record).get("transactionId")).isEqualTo("12");

        record.remove("transactionId");
        assertThat(TransactionValidator.excerpt(record)).isEmpty();
    }
}
