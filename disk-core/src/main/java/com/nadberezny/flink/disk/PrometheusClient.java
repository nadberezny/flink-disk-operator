package com.nadberezny.flink.disk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads TaskManager volume usage off the Prometheus HTTP API.
 *
 * <p>Two instant queries rather than one {@code used/capacity} division: the resize policy needs
 * absolute used bytes to compute a target size, not just a ratio.
 */
public class PrometheusClient {

    private static final String USED_METRIC = "kubelet_volume_stats_used_bytes";
    private static final String CAPACITY_METRIC = "kubelet_volume_stats_capacity_bytes";
    private static final String PVC_LABEL = "persistentvolumeclaim";

    private final HttpClient http;
    private final String baseUrl;
    private final java.time.Duration timeout;
    private final ObjectMapper mapper = new ObjectMapper();

    /** @param baseUrl base URL of the Prometheus HTTP API, e.g. {@code http://prometheus:9090} */
    public PrometheusClient(String baseUrl, java.time.Duration timeout) {
        this.baseUrl = baseUrl.replaceAll("/+$", "");
        this.timeout = timeout;
        this.http = HttpClient.newBuilder().connectTimeout(timeout).build();
    }

    /**
     * Usage for every PVC belonging to the given FlinkDeployment's TaskManagers, joined from the
     * used and capacity metrics. PVCs missing either metric are dropped.
     */
    public List<VolumeUsage> taskManagerVolumeUsage(String namespace, String flinkDeploymentName,
                                                    String volumeName)
            throws IOException, InterruptedException {
        String selector = "{namespace=\"" + namespace + "\","
                + PVC_LABEL + "=~\"" + pvcPattern(flinkDeploymentName, volumeName) + "\"}";

        Map<String, Long> used = byPvc(instantQuery(USED_METRIC + selector));
        Map<String, Long> capacity = byPvc(instantQuery(CAPACITY_METRIC + selector));

        List<VolumeUsage> result = new ArrayList<>();
        used.forEach((pvc, usedBytes) -> {
            Long capacityBytes = capacity.get(pvc);
            if (capacityBytes != null) {
                result.add(new VolumeUsage(pvc, usedBytes, capacityBytes));
            }
        });
        return result;
    }

    /**
     * The kubelet names a generic ephemeral volume's PVC {@code <pod>-<volume>}, and the Flink
     * operator names TaskManager pods {@code <deployment>-taskmanager-<attempt>-<index>}.
     *
     * <p>Escapes only {@code .}: Kubernetes object names are limited to lowercase alphanumerics,
     * {@code -} and {@code .}, so that is the sole regex metacharacter that can appear. Avoids
     * {@code Pattern.quote}, whose {@code \Q...\E} is not valid RE2 and would break against a real
     * Prometheus.
     */
    static String pvcPattern(String flinkDeploymentName, String volumeName) {
        return escape(flinkDeploymentName) + "-taskmanager-[0-9]+-[0-9]+-" + escape(volumeName);
    }

    private static String escape(String name) {
        return name.replace(".", "\\.");
    }

    private static Map<String, Long> byPvc(List<Sample> samples) {
        Map<String, Long> byPvc = new LinkedHashMap<>();
        for (Sample sample : samples) {
            String pvc = sample.labels().get(PVC_LABEL);
            if (pvc != null) {
                byPvc.put(pvc, (long) sample.value());
            }
        }
        return byPvc;
    }

    record Sample(Map<String, String> labels, double value) {}

    List<Sample> instantQuery(String promql) throws IOException, InterruptedException {
        URI uri = URI.create(baseUrl + "/api/v1/query?query="
                + URLEncoder.encode(promql, StandardCharsets.UTF_8));

        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(timeout)
                .header("Accept", "application/json")
                .GET()
                .build();

        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2) {
            throw new IOException("Prometheus returned HTTP " + response.statusCode()
                    + " for query '" + promql + "': " + truncate(response.body()));
        }
        return parse(response.body(), promql);
    }

    private List<Sample> parse(String body, String promql) throws IOException {
        JsonNode root = mapper.readTree(body);
        String status = root.path("status").asText();
        if (!"success".equals(status)) {
            throw new IOException("Prometheus query '" + promql + "' failed: "
                    + root.path("errorType").asText() + ": " + root.path("error").asText());
        }

        List<Sample> samples = new ArrayList<>();
        for (JsonNode entry : root.path("data").path("result")) {
            Map<String, String> labels = new LinkedHashMap<>();
            entry.path("metric").properties()
                    .forEach(field -> labels.put(field.getKey(), field.getValue().asText()));

            // Instant vector: "value" is [ <unix seconds>, "<value as string>" ].
            JsonNode value = entry.path("value");
            if (!value.isArray() || value.size() < 2) {
                continue;
            }
            String raw = value.get(1).asText();
            if ("NaN".equals(raw)) {
                continue;
            }
            samples.add(new Sample(labels, Double.parseDouble(raw)));
        }
        return samples;
    }

    private static String truncate(String s) {
        if (s == null) {
            return "";
        }
        return s.length() <= 200 ? s : s.substring(0, 200) + "...";
    }
}
