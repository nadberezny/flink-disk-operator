package com.nadberezny.mockprom;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The request shape KEDA's Prometheus scaler actually sends: an aggregation plus a {@code time} parameter. */
class QueryEndpointTest {

    private static final long T0 = 1_700_000_000_000L;

    private MockPrometheusServer server;
    private final HttpClient http = HttpClient.newHttpClient();

    @BeforeEach
    void start() throws IOException {
        MetricStore store = new MetricStore(
                "namespace=stream,pvc=flink-disk-job-taskmanager-1-1-local-storage,capacity=1Gi,used=950Mi"
                        + ";namespace=stream,pvc=flink-disk-job-taskmanager-1-2-local-storage,capacity=1Gi,used=100Mi",
                false, "", () -> T0);
        server = new MockPrometheusServer(0, store);
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop();
    }

    @Test
    void answersKedaStyleAggregationQueryWithASingleLabellessSample() throws Exception {
        String promql = "max(kubelet_volume_stats_used_bytes{namespace=\"stream\","
                + "persistentvolumeclaim=~\"flink-disk-job-taskmanager-[0-9]+-[0-9]+-local-storage\"}"
                + " / kubelet_volume_stats_capacity_bytes{namespace=\"stream\","
                + "persistentvolumeclaim=~\"flink-disk-job-taskmanager-[0-9]+-[0-9]+-local-storage\"})";

        HttpResponse<String> response = get("/api/v1/query?query="
                + URLEncoder.encode(promql, StandardCharsets.UTF_8) + "&time=2026-09-22T10:00:00Z");

        assertEquals(200, response.statusCode(), response.body());
        assertEquals("{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":["
                + "{\"metric\":{},\"value\":[1700000000.000,\"" + (950.0 / 1024.0) + "\"]}]}}",
                response.body());
    }

    @Test
    void rejectsUnsupportedPromQlWithAPrometheusShapedError() throws Exception {
        HttpResponse<String> response = get("/api/v1/query?query="
                + URLEncoder.encode("topk(1, kubelet_volume_stats_used_bytes)", StandardCharsets.UTF_8));

        assertEquals(400, response.statusCode());
        assertTrue(response.body().startsWith("{\"status\":\"error\",\"errorType\":\"bad_data\""), response.body());
    }

    private HttpResponse<String> get(String pathAndQuery) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(
                URI.create("http://127.0.0.1:" + server.port() + pathAndQuery)).GET().build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
