package com.nadberezny.flink.disk.keda;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static com.nadberezny.flink.disk.Quantities.GI;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ResizerConfigTest {

    private static Map<String, String> minimalEnv() {
        Map<String, String> env = new HashMap<>();
        env.put("PROMETHEUS_URL", "http://mock-prometheus.monitoring.svc.cluster.local:9090");
        env.put("TARGET_NAMESPACE", "stream");
        env.put("FLINK_DEPLOYMENT", "flink-disk-job");
        return env;
    }

    @Test
    void appliesTheOperatorsDefaults() {
        ResizerConfig config = ResizerConfig.fromEnv(minimalEnv());

        assertEquals(Duration.ofSeconds(10), config.prometheusTimeout());
        assertEquals(0.80, config.sizing().threshold());
        assertEquals(0.50, config.sizing().targetFill());
        assertEquals(GI, config.sizing().minSizeBytes());
        assertEquals(20 * GI, config.sizing().maxSizeBytes());
        assertEquals(GI, config.sizing().granularityBytes());
        assertFalse(config.dryRun());
    }

    @Test
    void readsOverrides() {
        Map<String, String> env = minimalEnv();
        env.put("DISK_THRESHOLD", "0.9");
        env.put("DISK_TARGET_FILL", "0.6");
        env.put("DISK_MAX_SIZE", "8Gi");
        env.put("DISK_SIZE_GRANULARITY", "512Mi");
        env.put("PROMETHEUS_TIMEOUT_SECONDS", "3");
        env.put("DRY_RUN", "true");

        ResizerConfig config = ResizerConfig.fromEnv(env);

        assertEquals(0.9, config.sizing().threshold());
        assertEquals(0.6, config.sizing().targetFill());
        assertEquals(8 * GI, config.sizing().maxSizeBytes());
        assertEquals(GI / 2, config.sizing().granularityBytes());
        assertEquals(Duration.ofSeconds(3), config.prometheusTimeout());
        assertTrue(config.dryRun());
        assertTrue(config.describe().contains("DRY_RUN"), config.describe());
    }

    @Test
    void requiresTheTargetDeployment() {
        Map<String, String> env = minimalEnv();
        env.remove("FLINK_DEPLOYMENT");

        IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> ResizerConfig.fromEnv(env));
        assertTrue(e.getMessage().contains("FLINK_DEPLOYMENT"), e.getMessage());
    }

    @Test
    void rejectsATargetFillThatWouldRetriggerImmediately() {
        Map<String, String> env = minimalEnv();
        env.put("DISK_TARGET_FILL", "0.85");

        assertThrows(IllegalArgumentException.class, () -> ResizerConfig.fromEnv(env));
    }
}
