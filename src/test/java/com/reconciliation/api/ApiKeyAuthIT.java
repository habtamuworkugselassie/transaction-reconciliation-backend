package com.reconciliation.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.reconciliation.infrastructure.persistence.PostgresSupport;
import com.reconciliation.testing.Fixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.UUID;

import static com.reconciliation.infrastructure.persistence.jooq.Tables.IMPORTS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * API-key authentication over real HTTP: keys are required on /v1, the key decides the
 * provider, and one provider cannot see or act on another provider's imports.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "app.processing.enabled=false",
                "spring.kafka.admin.auto-create=false",
                "management.endpoint.health.group.readiness.include=readinessState,db,storage,worker",
                "app.security.api-keys-enabled=true"
        })
@Import(ImportApiIT.TestBeans.class)
class ApiKeyAuthIT {

    private static final String KEY_ACME = "acme-secret-key";
    private static final String KEY_GLOBEX = "globex-secret-key";
    private static final PostgresSupport PG = PostgresSupport.get();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) throws IOException {
        registry.add("spring.datasource.url", PG::jdbcUrl);
        registry.add("spring.datasource.username", PG::username);
        registry.add("spring.datasource.password", PG::password);
        String root = Files.createTempDirectory("api-key-it-").toString();
        registry.add("app.storage.root", () -> root);
        registry.add("app.security.api-keys", () ->
                ApiKeyAuthenticationFilter.sha256Hex(KEY_ACME) + ":acme,"
                        + ApiKeyAuthenticationFilter.sha256Hex(KEY_GLOBEX) + ":globex");
    }

    @Autowired TestRestTemplate http;
    @Autowired ObjectMapper json;

    @BeforeEach
    void clean() {
        PG.truncateAll();
    }

    @Test
    void v1RequiresAKeyButProbesAndMetricsDoNot() throws Exception {
        assertError(upload(null, "k1", null), 401, "UNAUTHENTICATED");
        assertError(exchange(HttpMethod.GET, "/v1/imports/" + UUID.randomUUID(), null), 401, "UNAUTHENTICATED");
        assertError(exchange(HttpMethod.GET, "/v1/imports/" + UUID.randomUUID(), "wrong-key"), 401, "UNAUTHENTICATED");

        assertThat(http.getForEntity("/health/live", String.class).getStatusCode().value()).isEqualTo(200);
        assertThat(http.getForEntity("/health/ready", String.class).getStatusCode().value()).isEqualTo(200);
        assertThat(http.getForEntity("/metrics", String.class).getStatusCode().value()).isEqualTo(200);
        assertThat(http.getForEntity("/v3/api-docs", String.class).getBody()).contains("bearer");
    }

    @Test
    void theKeyDecidesTheProviderAndAContradictingHeaderIsRefused() throws Exception {
        ResponseEntity<String> created = upload(KEY_ACME, "k1", null);
        assertThat(created.getStatusCode().value()).isEqualTo(202);
        UUID id = UUID.fromString(json.readTree(created.getBody()).get("id").asText());
        assertThat(PG.db().select(IMPORTS.PROVIDER_ID).from(IMPORTS).where(IMPORTS.ID.eq(id)).fetchOne(IMPORTS.PROVIDER_ID))
                .isEqualTo("acme");

        assertThat(upload(KEY_ACME, "k2", "acme").getStatusCode().value()).isEqualTo(202);
        assertError(upload(KEY_ACME, "k3", "globex"), 403, "PROVIDER_MISMATCH");
    }

    @Test
    void anotherProvidersImportIsInvisibleOnEveryEndpoint() throws Exception {
        UUID id = UUID.fromString(json.readTree(upload(KEY_ACME, "mine", null).getBody()).get("id").asText());

        assertThat(exchange(HttpMethod.GET, "/v1/imports/" + id, KEY_ACME).getStatusCode().value()).isEqualTo(200);
        assertThat(exchange(HttpMethod.GET, "/v1/imports/" + id + "/summary", KEY_ACME).getStatusCode().value()).isEqualTo(200);

        for (String path : new String[] {"", "/summary", "/rejections"}) {
            assertError(exchange(HttpMethod.GET, "/v1/imports/" + id + path, KEY_GLOBEX), 404, "IMPORT_NOT_FOUND");
        }
        assertError(exchange(HttpMethod.POST, "/v1/imports/" + id + "/cancel", KEY_GLOBEX), 404, "IMPORT_NOT_FOUND");
        assertError(exchange(HttpMethod.POST, "/v1/imports/" + id + "/retry", KEY_GLOBEX), 404, "IMPORT_NOT_FOUND");
        // and globex's attempt changed nothing
        JsonNode status = json.readTree(exchange(HttpMethod.GET, "/v1/imports/" + id, KEY_ACME).getBody());
        assertThat(status.get("status").asText()).isEqualTo("pending");
    }

    // ---- helpers ---------------------------------------------------------------

    private ResponseEntity<String> upload(String apiKey, String idempotencyKey, String providerHeader) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.MULTIPART_FORM_DATA);
        h.set("Idempotency-Key", idempotencyKey);
        if (apiKey != null) {
            h.setBearerAuth(apiKey);
        }
        if (providerHeader != null) {
            h.set("X-Provider-Id", providerHeader);
        }
        MultiValueMap<String, Object> form = new LinkedMultiValueMap<>();
        form.add("file", new ByteArrayResource(Fixtures.validLines(2).getBytes(StandardCharsets.UTF_8)) {
            @Override
            public String getFilename() {
                return "t.ndjson";
            }
        });
        return http.exchange("/v1/imports", HttpMethod.POST, new HttpEntity<>(form, h), String.class);
    }

    private ResponseEntity<String> exchange(HttpMethod method, String path, String apiKey) {
        HttpHeaders h = new HttpHeaders();
        if (apiKey != null) {
            h.set("X-API-Key", apiKey);
        }
        return http.exchange(path, method, new HttpEntity<>(h), String.class);
    }

    private void assertError(ResponseEntity<String> r, int status, String code) throws IOException {
        assertThat(r.getStatusCode().value()).as(r.getBody()).isEqualTo(status);
        JsonNode error = json.readTree(r.getBody()).get("error");
        assertThat(error.get("code").asText()).isEqualTo(code);
        assertThat(error.get("requestId").asText()).isNotBlank();
    }
}
