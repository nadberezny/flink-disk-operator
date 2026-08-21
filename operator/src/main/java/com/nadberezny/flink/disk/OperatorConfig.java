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
        if (threshold <= 0 || threshold > 1) {
            throw new IllegalArgumentException("DISK_THRESHOLD must be in (0,1], got " + threshold);
        }
        if (targetFill <= 0 || targetFill >= 1) {
            throw new IllegalArgumentException("DISK_TARGET_FILL must be in (0,1), got " + targetFill);
        }
        // Otherwise a resize would land at or above the threshold and immediately re-trigger.
        if (targetFill >= threshold) {
            throw new IllegalArgumentException(
                    "DISK_TARGET_FILL (" + targetFill + ") must be below DISK_THRESHOLD (" + threshold
                            + "), or every resize would immediately breach the threshold again");
        }
        if (minSizeBytes <= 0) {
            throw new IllegalArgumentException("DISK_MIN_SIZE must be positive");
        }
        if (maxSizeBytes < minSizeBytes) {
            throw new IllegalArgumentException("DISK_MAX_SIZE must be >= DISK_MIN_SIZE");
        }
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
                + " threshold=" + pct(threshold)
                + " targetFill=" + pct(targetFill)
                + " size=[" + Quantities.format(minSizeBytes) + ".." + Quantities.format(maxSizeBytes) + "]"
                + " granularity=" + Quantities.format(granularityBytes)
                + " poll=" + pollInterval.toSeconds() + "s"
                + " namespaces=" + (watchNamespaces.isEmpty() ? "<all>" : List.copyOf(watchNamespaces));
    }

    static String pct(double ratio) {
        return Math.round(ratio * 1000) / 10.0 + "%";
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
