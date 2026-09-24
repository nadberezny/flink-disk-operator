package com.nadberezny.flink.disk;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Operator-wide configuration, all from the environment.
 *
 * <p>No per-FlinkDeployment overrides: the current volume name and size are read straight out of
 * the FlinkDeployment spec, which is more reliable than annotations that can drift from reality.
 */
public record OperatorConfig(
        String prometheusUrl,
        Duration prometheusTimeout,
        double threshold,
        double targetFill,
        long minSizeBytes,
        long maxSizeBytes,
        long granularityBytes,
        Duration pollInterval,
        Set<String> watchNamespaces) {

    public OperatorConfig {
        if (prometheusUrl == null || prometheusUrl.isBlank()) {
            throw new IllegalArgumentException("PROMETHEUS_URL is required");
        }
        // Threshold/target/size validation lives with the policy in :disk-core.
        new SizingConfig(threshold, targetFill, minSizeBytes, maxSizeBytes, granularityBytes);
        if (pollInterval.isZero() || pollInterval.isNegative()) {
            throw new IllegalArgumentException("POLL_INTERVAL_SECONDS must be positive");
        }
    }

    public static OperatorConfig fromEnv() {
        return new OperatorConfig(
                env("PROMETHEUS_URL", ""),
                Duration.ofSeconds(longEnv("PROMETHEUS_TIMEOUT_SECONDS", 10)),
                doubleEnv("DISK_THRESHOLD", 0.80),
                doubleEnv("DISK_TARGET_FILL", 0.50),
                Quantities.toBytes(env("DISK_MIN_SIZE", "1Gi")),
                Quantities.toBytes(env("DISK_MAX_SIZE", "20Gi")),
                Quantities.toBytes(env("DISK_SIZE_GRANULARITY", "1Gi")),
                Duration.ofSeconds(longEnv("POLL_INTERVAL_SECONDS", 30)),
                namespacesEnv());
    }

    /** The subset the resize policy needs, shared with the KEDA resizer. */
    public SizingConfig sizing() {
        return new SizingConfig(threshold, targetFill, minSizeBytes, maxSizeBytes, granularityBytes);
    }

    /** Empty means "all namespaces". */
    private static Set<String> namespacesEnv() {
        String raw = env("WATCH_NAMESPACES", "");
        if (raw.isBlank()) {
            return Set.of();
        }
        return Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toUnmodifiableSet());
    }

    public String describe() {
        return "prometheus=" + prometheusUrl
                + " " + sizing().describe()
                + " poll=" + pollInterval.toSeconds() + "s"
                + " namespaces=" + (watchNamespaces.isEmpty() ? "<all>" : List.copyOf(watchNamespaces));
    }

    static String pct(double ratio) {
        return SizingConfig.pct(ratio);
    }

    private static String env(String name, String defaultValue) {
        String raw = System.getenv(name);
        return raw == null || raw.isBlank() ? defaultValue : raw.trim();
    }

    private static double doubleEnv(String name, double defaultValue) {
        String raw = env(name, "");
        return raw.isEmpty() ? defaultValue : Double.parseDouble(raw);
    }

    private static long longEnv(String name, long defaultValue) {
        String raw = env(name, "");
        return raw.isEmpty() ? defaultValue : Long.parseLong(raw);
    }
}
