package com.nadberezny.flink.disk.keda;

import com.nadberezny.flink.disk.Quantities;
import com.nadberezny.flink.disk.SizingConfig;

import java.time.Duration;
import java.util.Map;

public record ResizerConfig(
        String prometheusUrl,
        Duration prometheusTimeout,
        String namespace,
        String deploymentName,
        SizingConfig sizing,
        boolean dryRun) {

    public ResizerConfig {
        if (prometheusUrl == null || prometheusUrl.isBlank()) {
            throw new IllegalArgumentException("PROMETHEUS_URL is required");
        }
        if (namespace == null || namespace.isBlank()) {
            throw new IllegalArgumentException("TARGET_NAMESPACE is required");
        }
        if (deploymentName == null || deploymentName.isBlank()) {
            throw new IllegalArgumentException("FLINK_DEPLOYMENT is required");
        }
        if (prometheusTimeout.isZero() || prometheusTimeout.isNegative()) {
            throw new IllegalArgumentException("PROMETHEUS_TIMEOUT_SECONDS must be positive");
        }
    }

    public static ResizerConfig fromEnv(Map<String, String> env) {
        return new ResizerConfig(
                env(env, "PROMETHEUS_URL", ""),
                Duration.ofSeconds(longEnv(env, "PROMETHEUS_TIMEOUT_SECONDS", 10)),
                env(env, "TARGET_NAMESPACE", ""),
                env(env, "FLINK_DEPLOYMENT", ""),
                new SizingConfig(
                        doubleEnv(env, "DISK_THRESHOLD", 0.80),
                        doubleEnv(env, "DISK_TARGET_FILL", 0.50),
                        // Defaults to the size the demo chart ships with, so an idle deployment
                        // never gets a "raise to configured minimum" patch.
                        Quantities.toBytes(env(env, "DISK_MIN_SIZE", "1Gi")),
                        Quantities.toBytes(env(env, "DISK_MAX_SIZE", "20Gi")),
                        Quantities.toBytes(env(env, "DISK_SIZE_GRANULARITY", "1Gi"))),
                Boolean.parseBoolean(env(env, "DRY_RUN", "false")));
    }

    public String describe() {
        return "target=" + namespace + "/" + deploymentName
                + " prometheus=" + prometheusUrl
                + " " + sizing.describe()
                + (dryRun ? " DRY_RUN" : "");
    }

    private static String env(Map<String, String> env, String name, String defaultValue) {
        String raw = env.get(name);
        return raw == null || raw.isBlank() ? defaultValue : raw.trim();
    }

    private static double doubleEnv(Map<String, String> env, String name, double defaultValue) {
        String raw = env(env, name, "");
        return raw.isEmpty() ? defaultValue : Double.parseDouble(raw);
    }

    private static long longEnv(Map<String, String> env, String name, long defaultValue) {
        String raw = env(env, name, "");
        return raw.isEmpty() ? defaultValue : Long.parseLong(raw);
    }
}
