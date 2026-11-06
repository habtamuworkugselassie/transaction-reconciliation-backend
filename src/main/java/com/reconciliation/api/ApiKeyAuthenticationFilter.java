package com.reconciliation.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.reconciliation.api.dto.ErrorResponse;
import com.reconciliation.domain.error.ErrorCode;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Optional API-key authentication for {@code /v1/**} (enabled with
 * {@code app.security.api-keys-enabled=true}).
 *
 * <p>The client sends {@code Authorization: Bearer <key>} or {@code X-API-Key: <key>}.
 * The server stores only SHA-256 hashes of keys, each mapped to a provider id, so a leaked
 * configuration does not leak usable keys. A valid key puts its provider id in a request
 * attribute; the controller uses it as the import's provider (instead of the
 * client-supplied {@code X-Provider-Id}) and as the owner check on every read, so a
 * provider can only see and act on its own imports.
 *
 * <p>Health, metrics and API documentation are not under {@code /v1} and stay open:
 * probes and scrapers do not carry credentials, and those endpoints expose no import data.
 *
 * <p>This is the extension point the design reserved for authentication: a servlet
 * filter that resolves a principal to a provider id, with no change to the use cases.
 * Swapping in JWT validation means replacing {@link #resolveProvider}.
 */
public final class ApiKeyAuthenticationFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(ApiKeyAuthenticationFilter.class);

    /** Request attribute holding the authenticated provider id. */
    public static final String PROVIDER_ATTRIBUTE = ApiKeyAuthenticationFilter.class.getName() + ".provider";
    static final String API_KEY_HEADER = "X-API-Key";

    private static final Pattern HASH = Pattern.compile("[0-9a-f]{64}");
    private static final Pattern PROVIDER = Pattern.compile("[A-Za-z0-9_.-]{1,64}");

    private final Map<String, String> providersByKeyHash;
    private final ObjectMapper json;

    public ApiKeyAuthenticationFilter(Map<String, String> providersByKeyHash, ObjectMapper json) {
        if (providersByKeyHash.isEmpty()) {
            throw new IllegalArgumentException("API keys are enabled but none are configured (app.security.api-keys)");
        }
        this.providersByKeyHash = Map.copyOf(providersByKeyHash);
        this.json = json;
    }

    /**
     * Parses {@code <sha256-hex>:<providerId>[,<sha256-hex>:<providerId>...]}. Fails at startup
     * on anything malformed, so a typo cannot silently lock everyone out or let everyone in.
     */
    public static Map<String, String> parseKeys(String config) {
        Map<String, String> keys = new LinkedHashMap<>();
        if (config == null || config.isBlank()) {
            return keys;
        }
        for (String entry : config.split(",")) {
            String e = entry.strip();
            if (e.isEmpty()) {
                continue;
            }
            int colon = e.indexOf(':');
            String hash = colon < 0 ? "" : e.substring(0, colon).strip().toLowerCase();
            String provider = colon < 0 ? "" : e.substring(colon + 1).strip();
            if (!HASH.matcher(hash).matches() || !PROVIDER.matcher(provider).matches()) {
                throw new IllegalArgumentException(
                        "Invalid app.security.api-keys entry; expected <sha256-hex>:<providerId> (entry " + (keys.size() + 1) + ")");
            }
            if (keys.putIfAbsent(hash, provider) != null) {
                throw new IllegalArgumentException("Duplicate key hash in app.security.api-keys");
            }
        }
        return keys;
    }

    public static String sha256Hex(String key) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(key.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI().substring(request.getContextPath().length());
        return !(path.equals("/v1") || path.startsWith("/v1/"));
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String key = presentedKey(request);
        if (key == null) {
            reject(request, response, "An API key is required (Authorization: Bearer <key> or X-API-Key)");
            return;
        }
        String provider = resolveProvider(key);
        if (provider == null) {
            log.info("Rejected request with an unknown API key {} {}", request.getMethod(), request.getRequestURI());
            reject(request, response, "The API key is not valid");
            return;
        }
        request.setAttribute(PROVIDER_ATTRIBUTE, provider);
        MDC.put("providerId", provider);
        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove("providerId");
        }
    }

    /** The provider a key belongs to, or null. Only the hash is compared; the key is never logged. */
    String resolveProvider(String key) {
        return providersByKeyHash.get(sha256Hex(key));
    }

    private static String presentedKey(HttpServletRequest request) {
        String authorization = request.getHeader(HttpHeaders.AUTHORIZATION);
        if (authorization != null && authorization.regionMatches(true, 0, "Bearer ", 0, 7)) {
            String token = authorization.substring(7).strip();
            return token.isEmpty() ? null : token;
        }
        String header = request.getHeader(API_KEY_HEADER);
        return header == null || header.isBlank() ? null : header.strip();
    }

    private void reject(HttpServletRequest request, HttpServletResponse response, String message) throws IOException {
        response.setStatus(ErrorCode.UNAUTHENTICATED.httpStatus());
        response.setHeader(HttpHeaders.WWW_AUTHENTICATE, "Bearer");
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        json.writeValue(response.getOutputStream(),
                ErrorResponse.of(ErrorCode.UNAUTHENTICATED.name(), message, RequestIdFilter.current(request)));
    }
}
