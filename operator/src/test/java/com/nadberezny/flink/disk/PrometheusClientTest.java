package com.nadberezny.flink.disk;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static com.nadberezny.flink.disk.Quantities.GI;
import static com.nadberezny.flink.disk.Quantities.MI;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises the client against a real HTTP server returning canned Prometheus payloads. */
class PrometheusClientTest {

    private HttpServer server;
    private PrometheusClient client;
    private final List<String> receivedQueries = new ArrayList<>();

    private void serve(Function<String, Response> handler) throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/query", exchange -> {
            String query = URLDecoder.decode(
                    exchange.getRequestURI().getRawQuery().substring("query=".length()),
                    StandardCharsets.UTF_8);
            receivedQueries.add(query);
            Response response = handler.apply(query);
            byte[] body = response.body().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(response.status(), body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.start();

        client = new PrometheusClient(new OperatorConfig(
                "http://127.0.0.1:" + server.getAddress().getPort() + "/",
                Duration.ofSeconds(5), 0.80, 0.50, GI, 20 * GI, GI,
                Duration.ofSeconds(30), Set.of()));
    }

    private record Response(int status, String body) {}

    private static String vector(String metric, String pvc, long value) {
        return """
                {"status":"success","data":{"resultType":"vector","result":[
                  {"metric":{"__name__":"%s","namespace":"stream","persistentvolumeclaim":"%s"},
                   "value":[1787324223.917,"%d"]}]}}
                """.formatted(metric, pvc, value);
    }

    @BeforeEach
    void resetQueries() {
        receivedQueries.clear();
    }

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void joinsUsedAndCapacityByPvc() throws Exception {
        serve(query -> new Response(200, query.startsWith("kubelet_volume_stats_used_bytes")
                ? vector("kubelet_volume_stats_used_bytes", "job-taskmanager-1-1-local-storage", 900 * MI)
                : vector("kubelet_volume_stats_capacity_bytes", "job-taskmanager-1-1-local-storage", GI)));

        List<VolumeUsage> usages =
                client.taskManagerVolumeUsage("stream", "job", "local-storage");

        assertEquals(1, usages.size());
        assertEquals("job-taskmanager-1-1-local-storage", usages.get(0).persistentVolumeClaim());
        assertEquals(900 * MI, usages.get(0).usedBytes());
        assertEquals(GI, usages.get(0).capacityBytes());
        assertEquals(0.87890625, usages.get(0).ratio(), 1e-9);
    }

    @Test
    void sendsNamespaceScopedSelectorsForBothMetrics() throws Exception {
        serve(query -> new Response(200, emptyVector()));

        client.taskManagerVolumeUsage("stream", "flink-disk-job", "local-storage");

        assertEquals(2, receivedQueries.size());
        assertTrue(receivedQueries.get(0).startsWith("kubelet_volume_stats_used_bytes{"),
                receivedQueries.get(0));
        assertTrue(receivedQueries.get(1).startsWith("kubelet_volume_stats_capacity_bytes{"),
                receivedQueries.get(1));
        for (String query : receivedQueries) {
            assertTrue(query.contains("namespace=\"stream\""), query);
            assertTrue(query.contains(
                    "persistentvolumeclaim=~\"flink-disk-job-taskmanager-[0-9]+-[0-9]+-local-storage\""),
                    query);
        }
    }

    @Test
    void dropsPvcsMissingOneOfTheTwoMetrics() throws Exception {
        // Capacity has not been scraped for this PVC yet: a ratio would be meaningless.
        serve(query -> new Response(200, query.startsWith("kubelet_volume_stats_used_bytes")
                ? vector("kubelet_volume_stats_used_bytes", "job-taskmanager-1-1-local-storage", 900 * MI)
                : emptyVector()));

        assertTrue(client.taskManagerVolumeUsage("stream", "job", "local-storage").isEmpty());
    }

    @Test
    void surfacesPrometheusErrorPayloads() throws Exception {
        serve(query -> new Response(400,
                "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"parse error\"}"));

        IOException e = assertThrows(IOException.class,
                () -> client.taskManagerVolumeUsage("stream", "job", "local-storage"));

        assertTrue(e.getMessage().contains("HTTP 400"), e.getMessage());
    }

    @Test
    void surfacesErrorStatusOnHttp200() throws Exception {
        serve(query -> new Response(200,
                "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"unknown metric\"}"));

        IOException e = assertThrows(IOException.class,
                () -> client.taskManagerVolumeUsage("stream", "job", "local-storage"));

        assertTrue(e.getMessage().contains("unknown metric"), e.getMessage());
    }

    @Test
    void skipsNaNSamples() throws Exception {
        serve(query -> new Response(200, """
                {"status":"success","data":{"resultType":"vector","result":[
                  {"metric":{"persistentvolumeclaim":"job-taskmanager-1-1-local-storage"},
                   "value":[1787324223.917,"NaN"]}]}}
                """));

        assertTrue(client.taskManagerVolumeUsage("stream", "job", "local-storage").isEmpty());
    }

    private static String emptyVector() {
        return "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[]}}";
    }
}
