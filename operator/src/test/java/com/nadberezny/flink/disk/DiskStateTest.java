package com.nadberezny.flink.disk;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.nadberezny.flink.disk.Quantities.GI;
import static com.nadberezny.flink.disk.Quantities.MI;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DiskStateTest {

    private static final DiskState SEED = DiskState.initial(GI, "local-storage");

    private static DiskSizePolicy.Decision decision(long size, boolean changed) {
        return new DiskSizePolicy.Decision(size, changed, false, "because",
                Optional.of(new VolumeUsage("tm-1-1-local-storage", 900 * MI, GI)));
    }

    @Test
    void roundTripsThroughConfigMapData() {
        DiskState state = new DiskState(4 * GI, "local-storage", 7, "grew", true);

        Map<String, String> data = state.toConfigMapData(decision(4 * GI, true), "2026-08-24T10:00:00Z");
        DiskState restored = DiskState.fromConfigMapData(data, SEED);

        assertEquals(4 * GI, restored.desiredSizeBytes());
        assertEquals("local-storage", restored.volumeName());
        assertEquals(7, restored.resizeGeneration());
        assertTrue(restored.atMaxSize());
    }

    @Test
    void writesTheDesiredSizeAsAReadableQuantity() {
        Map<String, String> data = new DiskState(2 * GI, "local-storage", 1, "grew", false)
                .toConfigMapData(decision(2 * GI, true), "2026-08-24T10:00:00Z");

        // This exact key/format is the contract the mutator plugin reads.
        assertEquals("2Gi", data.get(Names.STATE_DESIRED_SIZE));
    }

    @Test
    void recordsObservedUsageForVisibility() {
        Map<String, String> data = SEED.toConfigMapData(decision(GI, false), "2026-08-24T10:00:00Z");

        assertEquals("tm-1-1-local-storage", data.get(Names.STATE_OBSERVED_PVC));
        assertEquals("900Mi", data.get(Names.STATE_OBSERVED_USED));
        assertEquals("1Gi", data.get(Names.STATE_OBSERVED_CAPACITY));
        assertEquals("87.9%", data.get(Names.STATE_OBSERVED_RATIO));
    }

    @Test
    void fallsBackWhenConfigMapIsMissingOrCorrupt() {
        assertEquals(GI, DiskState.fromConfigMapData(null, SEED).desiredSizeBytes());
        assertEquals(GI, DiskState.fromConfigMapData(Map.of(), SEED).desiredSizeBytes());

        Map<String, String> corrupt = new HashMap<>();
        corrupt.put(Names.STATE_DESIRED_SIZE, "not-a-quantity");
        assertEquals(GI, DiskState.fromConfigMapData(corrupt, SEED).desiredSizeBytes());
    }

    @Test
    void bumpsGenerationOnlyWhenTheSizeChanges() {
        DiskState state = new DiskState(GI, "local-storage", 3, "steady", false);

        assertEquals(4, state.applying(decision(2 * GI, true)).resizeGeneration());
        assertEquals(3, state.applying(decision(GI, false)).resizeGeneration());
    }

    @Test
    void formatsSizesWithTheLargestExactUnit() {
        assertEquals("2Gi", Quantities.format(2 * GI));
        assertEquals("1536Mi", Quantities.format(1536 * MI));
        assertEquals("1023", Quantities.format(1023));
        assertEquals(2 * GI, Quantities.toBytes("2Gi"));
        assertEquals(1000000, Quantities.toBytes("1M"));
    }

    @Test
    void roundsUpToGranularity() {
        assertEquals(2 * GI, Quantities.roundUp(1800 * MI, GI));
        assertEquals(2 * GI, Quantities.roundUp(2 * GI, GI));
        assertEquals(1801 * MI, Quantities.roundUp(1800 * MI + 1, MI));
        assertEquals(1234, Quantities.roundUp(1234, 0), "granularity 0 is a no-op");
    }

    @Test
    void pvcPatternMatchesTaskManagerClaimsOnly() {
        String pattern = PrometheusClient.pvcPattern("flink-disk-job", "local-storage");

        assertTrue("flink-disk-job-taskmanager-1-1-local-storage".matches(pattern));
        assertTrue("flink-disk-job-taskmanager-12-34-local-storage".matches(pattern));
        assertFalse("flink-disk-job-taskmanager-1-1-other-volume".matches(pattern));
        assertFalse("other-job-taskmanager-1-1-local-storage".matches(pattern));
        assertFalse("flink-disk-job-jobmanager-1-1-local-storage".matches(pattern));
    }

    @Test
    void pvcPatternEscapesDotsInNames() {
        String pattern = PrometheusClient.pvcPattern("my.job", "local-storage");

        assertTrue("my.job-taskmanager-1-1-local-storage".matches(pattern));
        assertFalse("myxjob-taskmanager-1-1-local-storage".matches(pattern),
                "an unescaped '.' would match any character here");
    }
}
