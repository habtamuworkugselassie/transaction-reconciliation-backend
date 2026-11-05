package com.reconciliation.application.processing;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.reconciliation.domain.model.Rejection;
import com.reconciliation.domain.model.RejectionReason;
import com.reconciliation.domain.processing.TransactionValidator;
import com.reconciliation.domain.processing.ValidationResult;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Turns one line into a {@link ValidationResult}: JSON parsing (strict: no NaN, no
 * comments, no trailing tokens, bounded nesting) followed by domain validation.
 */
public final class RecordParser {

    private static final Pattern CONTROL = Pattern.compile("[\\p{Cntrl}]");

    private final ObjectMapper mapper;
    private final TransactionValidator validator;
    private final int maxRawValueLength;

    public RecordParser(TransactionValidator validator, int maxRawValueLength) {
        this.validator = validator;
        this.maxRawValueLength = maxRawValueLength;
        this.mapper = JsonMapper.builder()
                .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                .disable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS)
                .disable(JsonReadFeature.ALLOW_JAVA_COMMENTS)
                .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                .build();
        this.mapper.getFactory().setStreamReadConstraints(StreamReadConstraints.builder()
                .maxNestingDepth(16)
                .maxNumberLength(40)
                .build());
    }

    public ValidationResult parse(NdjsonLineReader.Line line) {
        if (line.tooLong()) {
            return invalid(line.number(), RejectionReason.LINE_TOO_LONG, "Line exceeds the maximum allowed length", "");
        }
        Object parsed;
        try {
            parsed = mapper.readValue(line.text(), Object.class);
        } catch (IOException | IllegalArgumentException e) {
            return invalid(line.number(), RejectionReason.INVALID_JSON, "Line is not valid JSON", line.text());
        }
        if (!(parsed instanceof Map<?, ?> map)) {
            return invalid(line.number(), RejectionReason.NOT_AN_OBJECT, "Line must be a JSON object", line.text());
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> record = (Map<String, Object>) map;
        return validator.validate(line.number(), record);
    }

    private ValidationResult invalid(long lineNumber, RejectionReason reason, String message, String raw) {
        Map<String, Object> excerpt = new LinkedHashMap<>();
        if (!raw.isEmpty()) {
            String clean = CONTROL.matcher(raw).replaceAll(" ");
            excerpt.put("raw", clean.length() > maxRawValueLength ? clean.substring(0, maxRawValueLength) + "…" : clean);
        }
        return new ValidationResult.Invalid(new Rejection(lineNumber, reason, message, excerpt));
    }
}
