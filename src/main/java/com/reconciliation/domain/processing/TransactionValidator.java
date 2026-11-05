package com.reconciliation.domain.processing;

import com.reconciliation.domain.model.NormalizedTransaction;
import com.reconciliation.domain.model.Rejection;
import com.reconciliation.domain.model.RejectionReason;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Currency;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Validates one parsed record and produces its normalised form.
 *
 * <p>Normalisation rules (documented in ARCHITECTURE.md, tested in TransactionValidatorTest):
 * <ul>
 *   <li>All string fields are trimmed. Control characters (U+0000-U+001F, U+007F) are rejected
 *       in identifiers and replaced by a space in the description, which is then
 *       whitespace-collapsed.</li>
 *   <li>Identifiers must be 1-128 characters after trimming.</li>
 *   <li>{@code amount} must be a JSON number, finite, greater than zero, at most 4 fractional
 *       digits and below 10^15. It is stored with scale 4 (145.75 -> 145.7500).</li>
 *   <li>{@code currency} is upper-cased after trimming and must be a known ISO 4217 code.
 *       Lower-case input is accepted as a normalisation; anything else is rejected.</li>
 *   <li>{@code timestamp} must be ISO-8601 with an explicit offset or 'Z'. It is converted to
 *       UTC and truncated to milliseconds.</li>
 *   <li>{@code description} is optional; after normalisation it must be at most 500 characters.
 *       An empty description is stored as null.</li>
 *   <li>Unknown fields are ignored (lenient reader); the line-length limit bounds abuse.</li>
 * </ul>
 */
public final class TransactionValidator {

    public static final int MAX_ID_LENGTH = 128;
    public static final int MAX_DESCRIPTION_LENGTH = 500;
    public static final int AMOUNT_SCALE = 4;
    private static final BigDecimal MAX_AMOUNT = new BigDecimal("1000000000000000"); // 10^15
    private static final Pattern CONTROL = Pattern.compile("[\\p{Cntrl}]");
    private static final Pattern WHITESPACE_RUN = Pattern.compile("\\s+");
    private static final Set<String> CURRENCIES = Currency.getAvailableCurrencies().stream()
            .map(Currency::getCurrencyCode).collect(Collectors.toUnmodifiableSet());

    private final String providerId;

    public TransactionValidator(String providerId) {
        this.providerId = providerId;
    }

    public ValidationResult validate(long lineNumber, Map<String, Object> record) {
        try {
            String transactionId = requireId(record, "transactionId");
            String accountId = requireId(record, "accountId");
            String merchantId = requireId(record, "merchantId");
            BigDecimal amount = requireAmount(record);
            String currency = requireCurrency(record);
            Instant timestamp = requireTimestamp(record);
            String description = optionalDescription(record);
            return new ValidationResult.Valid(new NormalizedTransaction(lineNumber, providerId, transactionId,
                    accountId, merchantId, amount, currency, timestamp, description));
        } catch (Invalid e) {
            return new ValidationResult.Invalid(new Rejection(lineNumber, e.reason, e.getMessage(), excerpt(record)));
        }
    }

    /** Small, safe excerpt of the record for the rejection list: only the transactionId. */
    public static Map<String, Object> excerpt(Map<String, Object> record) {
        Object id = record.get("transactionId");
        Map<String, Object> out = new LinkedHashMap<>();
        if (id instanceof String s) {
            String clean = CONTROL.matcher(s).replaceAll("");
            out.put("transactionId", clean.length() > MAX_ID_LENGTH ? clean.substring(0, MAX_ID_LENGTH) + "…" : clean);
        } else if (id != null) {
            out.put("transactionId", String.valueOf(id).length() > 64 ? "<non-string>" : String.valueOf(id));
        }
        return out;
    }

    private static String requireId(Map<String, Object> record, String field) {
        Object value = record.get(field);
        if (value == null) {
            throw new Invalid(RejectionReason.MISSING_FIELD, field + " is required");
        }
        if (!(value instanceof String s)) {
            throw new Invalid(RejectionReason.INVALID_TYPE, field + " must be a string");
        }
        String trimmed = s.strip();
        if (trimmed.isEmpty()) {
            throw new Invalid(RejectionReason.MISSING_FIELD, field + " must not be blank");
        }
        if (trimmed.length() > MAX_ID_LENGTH) {
            throw new Invalid(RejectionReason.FIELD_TOO_LONG, field + " exceeds " + MAX_ID_LENGTH + " characters");
        }
        if (CONTROL.matcher(trimmed).find()) {
            throw new Invalid(RejectionReason.INVALID_VALUE, field + " contains control characters");
        }
        return trimmed;
    }

    private static BigDecimal requireAmount(Map<String, Object> record) {
        Object value = record.get("amount");
        if (value == null) {
            throw new Invalid(RejectionReason.MISSING_FIELD, "amount is required");
        }
        BigDecimal amount = switch (value) {
            case BigDecimal d -> d;
            case BigInteger i -> new BigDecimal(i);
            case Integer i -> BigDecimal.valueOf(i);
            case Long l -> BigDecimal.valueOf(l);
            case Double d when !d.isNaN() && !d.isInfinite() -> BigDecimal.valueOf(d);
            case Float f when !f.isNaN() && !f.isInfinite() -> new BigDecimal(f.toString());
            default -> throw new Invalid(RejectionReason.INVALID_TYPE, "amount must be a number");
        };
        if (amount.signum() <= 0) {
            throw new Invalid(RejectionReason.INVALID_AMOUNT, "amount must be greater than zero");
        }
        if (amount.stripTrailingZeros().scale() > AMOUNT_SCALE) {
            throw new Invalid(RejectionReason.INVALID_AMOUNT, "amount must have at most " + AMOUNT_SCALE + " decimal places");
        }
        if (amount.compareTo(MAX_AMOUNT) >= 0) {
            throw new Invalid(RejectionReason.INVALID_AMOUNT, "amount is too large");
        }
        return amount.setScale(AMOUNT_SCALE);
    }

    private static String requireCurrency(Map<String, Object> record) {
        Object value = record.get("currency");
        if (value == null) {
            throw new Invalid(RejectionReason.MISSING_FIELD, "currency is required");
        }
        if (!(value instanceof String s)) {
            throw new Invalid(RejectionReason.INVALID_TYPE, "currency must be a string");
        }
        String code = s.strip().toUpperCase(Locale.ROOT);
        if (code.length() != 3 || !CURRENCIES.contains(code)) {
            throw new Invalid(RejectionReason.INVALID_CURRENCY, "Currency must be a supported three-letter code");
        }
        return code;
    }

    private static Instant requireTimestamp(Map<String, Object> record) {
        Object value = record.get("timestamp");
        if (value == null) {
            throw new Invalid(RejectionReason.MISSING_FIELD, "timestamp is required");
        }
        if (!(value instanceof String s)) {
            throw new Invalid(RejectionReason.INVALID_TYPE, "timestamp must be a string");
        }
        String text = s.strip();
        if (text.length() > 40) {
            throw new Invalid(RejectionReason.INVALID_TIMESTAMP, "timestamp must be an ISO-8601 date-time with offset");
        }
        try {
            return OffsetDateTime.parse(text).toInstant().truncatedTo(ChronoUnit.MILLIS);
        } catch (DateTimeParseException e) {
            throw new Invalid(RejectionReason.INVALID_TIMESTAMP, "timestamp must be an ISO-8601 date-time with offset");
        }
    }

    private static String optionalDescription(Map<String, Object> record) {
        Object value = record.get("description");
        if (value == null) {
            return null;
        }
        if (!(value instanceof String s)) {
            throw new Invalid(RejectionReason.INVALID_TYPE, "description must be a string");
        }
        String normalized = normalizeDescription(s);
        if (normalized.length() > MAX_DESCRIPTION_LENGTH) {
            throw new Invalid(RejectionReason.DESCRIPTION_TOO_LONG,
                    "description exceeds " + MAX_DESCRIPTION_LENGTH + " characters");
        }
        return normalized.isEmpty() ? null : normalized;
    }

    /** Control characters become spaces, whitespace runs collapse to one space, ends are trimmed. */
    public static String normalizeDescription(String raw) {
        String noControl = CONTROL.matcher(raw).replaceAll(" ");
        return WHITESPACE_RUN.matcher(noControl).replaceAll(" ").strip();
    }

    private static final class Invalid extends RuntimeException {
        private final RejectionReason reason;

        Invalid(RejectionReason reason, String message) {
            super(message, null, false, false);
            this.reason = reason;
        }
    }
}
