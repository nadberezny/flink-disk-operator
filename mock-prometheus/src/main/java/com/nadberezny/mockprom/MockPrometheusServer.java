package com.nadberezny.mockprom;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public final class MockPrometheusServer {

    private static final Logger LOG = Logger.getLogger(MockPrometheusServer.class.getName());

    private final HttpServer http;
    private final MetricStore store;

    MockPrometheusServer(int port, MetricStore store) throws IOException {
        this.store = store;
        this.http = HttpServer.create(new InetSocketAddress(port), 0);
        this.http.setExecutor(Executors.newFixedThreadPool(4));

        http.createContext("/", this::handleIndex);
        http.createContext("/api/v1/query", this::handleQuery);
        http.createContext("/metrics", this::handleMetrics);
        http.createContext("/mock/volumes", this::handleVolumes);
        http.createContext("/mock/reset", this::handleReset);
        http.createContext("/-/healthy", ok("Mock Prometheus is Healthy.\n"));
        http.createContext("/-/ready", ok("Mock Prometheus is Ready.\n"));
    }

    public static void main(String[] args) throws IOException {
        System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tFT%1$tT %4$s %5$s%n");

        int port = Integer.parseInt(env("PORT", "9090"));
        MetricStore store = new MetricStore(
                env("MOCK_SEED", ""),
                Boolean.parseBoolean(env("MOCK_AUTO_REGISTER", "false")),
                env("MOCK_AUTO_DEFAULTS", "capacity=1Gi,used=850Mi,growth=1Mi"),
                System::currentTimeMillis);

        MockPrometheusServer server = new MockPrometheusServer(port, store);
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
        server.start();
        LOG.info("Mock Prometheus listening on :" + port);
    }

    void start() {
        http.start();
    }

    void stop() {
        http.stop(0);
    }

    int port() {
        return http.getAddress().getPort();
    }

    private void handleIndex(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestURI().getPath().equals("/")) {
            respond(exchange, 404, "text/plain", "not found\n");
            return;
        }
        respond(exchange, 200, "text/plain", """
                Mock Prometheus (flink-disk-operator POC)

                Prometheus-compatible:
                  GET  /api/v1/query?query=<promql>
                  GET  /metrics
                  GET  /-/healthy, /-/ready

                Metrics served:
                  kubelet_volume_stats_used_bytes
                  kubelet_volume_stats_capacity_bytes
                  kubelet_volume_stats_available_bytes

                Control API:
                  GET    /mock/volumes
                  POST   /mock/volumes?namespace=&pvc=&capacity=&used=&growth=&pod=&node=
                  DELETE /mock/volumes?namespace=&pvc=
                  POST   /mock/reset
                """);
    }

    private void handleQuery(HttpExchange exchange) throws IOException {
        Map<String, String> params = allParams(exchange);
        String query = params.get("query");
        try {
            List<PromQl.Sample> samples = PromQl.evaluate(query, store);
            respond(exchange, 200, "application/json", vectorResponse(samples, store.now()));
        } catch (PromQl.BadQueryException e) {
            LOG.warning("Rejected query '" + query + "': " + e.getMessage());
            respond(exchange, 400, "application/json", errorResponse("bad_data", e.getMessage()));
        }
    }

    private void handleMetrics(HttpExchange exchange) throws IOException {
        long now = store.now();
        StringBuilder sb = new StringBuilder();
        appendMetric(sb, PromQl.CAPACITY, "Capacity in bytes of the volume", now, Kind.CAPACITY);
        appendMetric(sb, PromQl.USED, "Number of used bytes in the volume", now, Kind.USED);
        appendMetric(sb, PromQl.AVAILABLE, "Number of available bytes in the volume", now, Kind.AVAILABLE);
        respond(exchange, 200, "text/plain; version=0.0.4", sb.toString());
    }

    private void handleVolumes(HttpExchange exchange) throws IOException {
        Map<String, String> params = allParams(exchange);
        switch (exchange.getRequestMethod()) {
            case "GET" -> respond(exchange, 200, "application/json", volumesResponse());
            case "POST", "PUT" -> {
                try {
                    store.upsert(params);
                    respond(exchange, 200, "application/json", volumesResponse());
                } catch (RuntimeException e) {
                    respond(exchange, 400, "application/json", errorResponse("bad_data", e.getMessage()));
                }
            }
            case "DELETE" -> {
                String namespace = params.get("namespace");
                String pvc = params.get("pvc");
                if (namespace == null || pvc == null) {
                    respond(exchange, 400, "application/json",
                            errorResponse("bad_data", "'namespace' and 'pvc' are required"));
                    return;
                }
                boolean removed = store.remove(namespace, pvc);
                respond(exchange, removed ? 200 : 404, "application/json", volumesResponse());
            }
            default -> respond(exchange, 405, "text/plain", "method not allowed\n");
        }
    }

    private void handleReset(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equals("POST")) {
            respond(exchange, 405, "text/plain", "method not allowed\n");
            return;
        }
        store.reset();
        respond(exchange, 200, "application/json", volumesResponse());
    }

    private HttpHandler ok(String body) {
        return exchange -> respond(exchange, 200, "text/plain", body);
    }

    // ---------------------------------------------------------------- rendering

    private enum Kind { USED, CAPACITY, AVAILABLE }

    private void appendMetric(StringBuilder sb, String name, String help, long now, Kind kind) {
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(" gauge\n");
        for (VolumeSeries volume : store.all()) {
            long value = switch (kind) {
                case USED -> volume.usedBytes(now);
                case CAPACITY -> volume.capacityBytes();
                case AVAILABLE -> volume.availableBytes(now);
            };
            sb.append(name).append('{');
            boolean first = true;
            for (Map.Entry<String, String> label : volume.labels().entrySet()) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                sb.append(label.getKey()).append('=').append(Json.quote(label.getValue()));
            }
            sb.append("} ").append(value).append('\n');
        }
    }

    static String vectorResponse(List<PromQl.Sample> samples, long nowMillis) {
        // Locale.ROOT throughout: a comma decimal separator would emit invalid JSON.
        String timestamp = String.format(Locale.ROOT, "%.3f", nowMillis / 1000.0);
        List<String> results = new ArrayList<>();
        for (PromQl.Sample sample : samples) {
            results.add("{\"metric\":" + Json.object(sample.labels())
                    + ",\"value\":[" + timestamp + "," + Json.quote(Json.sampleValue(sample.value())) + "]}");
        }
        return "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":["
                + String.join(",", results) + "]}}";
    }

    static String errorResponse(String errorType, String message) {
        return "{\"status\":\"error\",\"errorType\":" + Json.quote(errorType)
                + ",\"error\":" + Json.quote(message == null ? "" : message) + "}";
    }

    private String volumesResponse() {
        long now = store.now();
        List<String> entries = new ArrayList<>();
        for (VolumeSeries volume : store.all()) {
            long used = volume.usedBytes(now);
            long capacity = volume.capacityBytes();
            double ratio = capacity == 0 ? 0.0 : (double) used / capacity;
            StringBuilder sb = new StringBuilder("{");
            sb.append("\"namespace\":").append(Json.quote(volume.namespace()));
            sb.append(",\"persistentvolumeclaim\":").append(Json.quote(volume.persistentVolumeClaim()));
            if (volume.pod() != null && !volume.pod().isBlank()) {
                sb.append(",\"pod\":").append(Json.quote(volume.pod()));
            }
            if (volume.node() != null && !volume.node().isBlank()) {
                sb.append(",\"node\":").append(Json.quote(volume.node()));
            }
            sb.append(",\"capacityBytes\":").append(capacity);
            sb.append(",\"capacity\":").append(Json.quote(Sizes.format(capacity)));
            sb.append(",\"usedBytes\":").append(used);
            sb.append(",\"used\":").append(Json.quote(Sizes.format(used)));
            sb.append(",\"usedRatio\":").append(String.format(Locale.ROOT, "%.4f", ratio));
            sb.append(",\"growthBytesPerSecond\":").append((long) volume.growthBytesPerSecond());
            entries.add(sb.append('}').toString());
        }
        return "{\"volumes\":[" + String.join(",", entries) + "]}";
    }

    // ---------------------------------------------------------------- plumbing

    /** Query-string params, overlaid with form-encoded body params for POST/PUT/DELETE. */
    private static Map<String, String> allParams(HttpExchange exchange) throws IOException {
        Map<String, String> params = new LinkedHashMap<>(parseQueryString(exchange.getRequestURI().getRawQuery()));
        String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
        if (contentType != null && contentType.startsWith("application/x-www-form-urlencoded")) {
            try (InputStream in = exchange.getRequestBody()) {
                params.putAll(parseQueryString(new String(in.readAllBytes(), StandardCharsets.UTF_8)));
            }
        }
        return params;
    }

    private static Map<String, String> parseQueryString(String raw) {
        Map<String, String> params = new LinkedHashMap<>();
        if (raw == null || raw.isBlank()) {
            return params;
        }
        for (String pair : raw.split("&")) {
            if (pair.isBlank()) {
                continue;
            }
            int eq = pair.indexOf('=');
            String key = eq < 0 ? pair : pair.substring(0, eq);
            String value = eq < 0 ? "" : pair.substring(eq + 1);
            params.put(URLDecoder.decode(key, StandardCharsets.UTF_8),
                    URLDecoder.decode(value, StandardCharsets.UTF_8));
        }
        return params;
    }

    private static void respond(HttpExchange exchange, int status, String contentType, String body)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static String env(String name, String defaultValue) {
        String raw = System.getenv(name);
        return raw == null || raw.isBlank() ? defaultValue : raw.trim();
    }
}
