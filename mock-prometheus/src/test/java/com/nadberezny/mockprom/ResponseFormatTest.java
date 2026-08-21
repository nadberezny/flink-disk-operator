package com.nadberezny.mockprom;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ResponseFormatTest {

    private final Locale original = Locale.getDefault();

    @AfterEach
    void restoreLocale() {
        Locale.setDefault(original);
    }

    /** Regression: a comma decimal separator produced syntactically invalid JSON. */
    @Test
    void rendersNumbersLocaleIndependently() {
        Locale.setDefault(Locale.forLanguageTag("pl-PL"));

        String json = MockPrometheusServer.vectorResponse(
                List.of(new PromQl.Sample(Map.of("namespace", "stream"), 0.9277)),
                1_700_000_000_917L);

        assertTrue(json.contains("[1700000000.917,\"0.9277\"]"), json);
    }

    @Test
    void rendersPrometheusVectorEnvelope() {
        String json = MockPrometheusServer.vectorResponse(List.of(), 1_700_000_000_000L);

        assertEquals("{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[]}}", json);
    }

    @Test
    void rendersPrometheusErrorEnvelope() {
        assertEquals("{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"nope\"}",
                MockPrometheusServer.errorResponse("bad_data", "nope"));
    }
}
