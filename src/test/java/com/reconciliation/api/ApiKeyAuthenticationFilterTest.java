package com.reconciliation.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApiKeyAuthenticationFilterTest {

    private static final String KEY_A = "key-for-provider-a";
    private final ApiKeyAuthenticationFilter filter = new ApiKeyAuthenticationFilter(
            Map.of(ApiKeyAuthenticationFilter.sha256Hex(KEY_A), "provider-a"), new ObjectMapper());

    private record Outcome(MockHttpServletResponse response, MockFilterChain chain, MockHttpServletRequest request) {}

    private Outcome call(String path, String header, String value) throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", path);
        if (header != null) {
            request.addHeader(header, value);
        }
        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();
        filter.doFilter(request, response, chain);
        return new Outcome(response, chain, request);
    }

    @Test
    void bearerKeyAuthenticatesAndSetsTheProvider() throws Exception {
        Outcome o = call("/v1/imports/x", "Authorization", "Bearer " + KEY_A);

        assertThat(o.chain().getRequest()).isNotNull();
        assertThat(o.request().getAttribute(ApiKeyAuthenticationFilter.PROVIDER_ATTRIBUTE)).isEqualTo("provider-a");
    }

    @Test
    void xApiKeyHeaderIsAccepted() throws Exception {
        Outcome o = call("/v1/imports", "X-API-Key", KEY_A);

        assertThat(o.request().getAttribute(ApiKeyAuthenticationFilter.PROVIDER_ATTRIBUTE)).isEqualTo("provider-a");
    }

    @Test
    void missingKeyIs401WithTheErrorContract() throws Exception {
        Outcome o = call("/v1/imports", null, null);

        assertThat(o.response().getStatus()).isEqualTo(401);
        assertThat(o.response().getHeader("WWW-Authenticate")).isEqualTo("Bearer");
        assertThat(o.response().getContentAsString()).contains("\"code\":\"UNAUTHENTICATED\"");
        assertThat(o.chain().getRequest()).isNull();
    }

    @Test
    void unknownKeyIs401() throws Exception {
        Outcome o = call("/v1/imports", "Authorization", "Bearer not-a-key");

        assertThat(o.response().getStatus()).isEqualTo(401);
        assertThat(o.response().getContentAsString()).doesNotContain("not-a-key");
        assertThat(o.chain().getRequest()).isNull();
    }

    @Test
    void probesMetricsAndDocsStayOpen() throws Exception {
        for (String path : new String[] {"/health/ready", "/health/live", "/metrics", "/swagger-ui.html", "/v3/api-docs"}) {
            Outcome o = call(path, null, null);
            assertThat(o.chain().getRequest()).as(path).isNotNull();
            assertThat(o.response().getStatus()).as(path).isEqualTo(200);
        }
    }

    @Test
    void configurationIsParsedStrictly() {
        String hash = ApiKeyAuthenticationFilter.sha256Hex("k");
        assertThat(ApiKeyAuthenticationFilter.parseKeys(" " + hash + ":acme , " + ApiKeyAuthenticationFilter.sha256Hex("j") + ":beta "))
                .containsEntry(hash, "acme").hasSize(2);
        assertThat(ApiKeyAuthenticationFilter.parseKeys("")).isEmpty();

        assertThatThrownBy(() -> ApiKeyAuthenticationFilter.parseKeys("plaintext-key:acme"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageNotContaining("plaintext-key");
        assertThatThrownBy(() -> ApiKeyAuthenticationFilter.parseKeys(hash + ":bad provider"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ApiKeyAuthenticationFilter.parseKeys(hash + ":a," + hash + ":b"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ApiKeyAuthenticationFilter(Map.of(), new ObjectMapper()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
