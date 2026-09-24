package com.nadberezny.flink.disk;

import org.junit.jupiter.api.Test;

import java.util.List;

import static com.nadberezny.flink.disk.Quantities.GI;
import static com.nadberezny.flink.disk.Quantities.MI;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DiskSizePolicyTest {

    private static final long MAX = 20 * GI;

    private static DiskSizePolicy policy() {
        return policy(0.80, 0.50, GI, MAX);
    }

    private static DiskSizePolicy policy(double threshold, double targetFill, long granularity, long max) {
        return new DiskSizePolicy(new SizingConfig(threshold, targetFill, GI, max, granularity));
    }

    private static VolumeUsage usage(long usedBytes, long capacityBytes) {
        return new VolumeUsage("flink-disk-job-taskmanager-1-1-local-storage", usedBytes, capacityBytes);
    }

    @Test
    void doesNothingBelowThreshold() {
        DiskSizePolicy.Decision decision = policy().decide(List.of(usage(500 * MI, GI)), GI);

        assertFalse(decision.changed());
        assertEquals(GI, decision.desiredSizeBytes());
        assertTrue(decision.reason().contains("below threshold"), decision.reason());
    }

    @Test
    void growsSoUsageLandsAtTargetFill() {
        // 900Mi used at 50% target wants 1800Mi, which rounds up to 2Gi.
        DiskSizePolicy.Decision decision = policy().decide(List.of(usage(900 * MI, GI)), GI);

        assertTrue(decision.changed());
        assertEquals(2 * GI, decision.desiredSizeBytes());
        assertFalse(decision.atMaxSize());
    }

    @Test
    void convergesInOneStepFromAnOverrun() {
        // The point of target-headroom over a multiplicative step: a nearly-full 8Gi volume goes
        // straight to 16Gi rather than climbing 8 -> 12 -> 16 across three restarts.
        DiskSizePolicy.Decision decision = policy().decide(List.of(usage(7900 * MI, 8 * GI)), 8 * GI);

        assertEquals(16 * GI, decision.desiredSizeBytes());
    }

    @Test
    void resizeLandsBelowThresholdSoItDoesNotImmediatelyRetrigger() {
        DiskSizePolicy.Decision first = policy().decide(List.of(usage(900 * MI, GI)), GI);
        // Same usage, now on the newly granted capacity.
        DiskSizePolicy.Decision second =
                policy().decide(List.of(usage(900 * MI, first.desiredSizeBytes())), first.desiredSizeBytes());

        assertFalse(second.changed(), second.reason());
    }

    @Test
    void picksTheFullestVolumeAcrossTaskManagers() {
        List<VolumeUsage> usages = List.of(
                new VolumeUsage("tm-1-1-local-storage", 100 * MI, GI),
                new VolumeUsage("tm-1-2-local-storage", 950 * MI, GI),
                new VolumeUsage("tm-1-3-local-storage", 300 * MI, GI));

        DiskSizePolicy.Decision decision = policy().decide(usages, GI);

        assertTrue(decision.changed());
        assertEquals("tm-1-2-local-storage", decision.worst().orElseThrow().persistentVolumeClaim());
        assertEquals(2 * GI, decision.desiredSizeBytes());
    }

    @Test
    void neverShrinks() {
        // Remembered size is 8Gi while the volume in front of us is a fresh, nearly-empty 1Gi.
        DiskSizePolicy.Decision decision = policy().decide(List.of(usage(10 * MI, GI)), 8 * GI);

        assertFalse(decision.changed());
        assertEquals(8 * GI, decision.desiredSizeBytes());
    }

    @Test
    void clampsToMaxSizeAndFlagsIt() {
        DiskSizePolicy.Decision decision =
                policy(0.80, 0.50, GI, 4 * GI).decide(List.of(usage(3900 * MI, 4 * GI)), 4 * GI);

        assertFalse(decision.changed(), "already at max, nothing to grow to");
        assertTrue(decision.atMaxSize());
        assertEquals(4 * GI, decision.desiredSizeBytes());
        assertTrue(decision.reason().contains("already at max size"), decision.reason());
    }

    @Test
    void clampsAPartialGrowthToMaxSize() {
        DiskSizePolicy.Decision decision =
                policy(0.80, 0.50, GI, 3 * GI).decide(List.of(usage(1900 * MI, 2 * GI)), 2 * GI);

        assertTrue(decision.changed());
        assertEquals(3 * GI, decision.desiredSizeBytes(), "wanted 4Gi, capped at 3Gi");
        assertTrue(decision.atMaxSize());
    }

    @Test
    void handlesNoMetricsYet() {
        DiskSizePolicy.Decision decision = policy().decide(List.of(), 2 * GI);

        assertFalse(decision.changed());
        assertEquals(2 * GI, decision.desiredSizeBytes());
        assertTrue(decision.reason().contains("no volume metrics"), decision.reason());
    }

    @Test
    void ignoresSeriesWithZeroCapacity() {
        DiskSizePolicy.Decision decision = policy().decide(List.of(usage(900 * MI, 0)), GI);

        assertFalse(decision.changed());
        assertTrue(decision.reason().contains("no volume metrics"), decision.reason());
    }

    @Test
    void raisesToConfiguredMinimumEvenWhenIdle() {
        DiskSizePolicy withBigMin = new DiskSizePolicy(new SizingConfig(0.80, 0.50, 4 * GI, MAX, GI));

        DiskSizePolicy.Decision decision = withBigMin.decide(List.of(usage(10 * MI, GI)), GI);

        assertTrue(decision.changed());
        assertEquals(4 * GI, decision.desiredSizeBytes());
    }

    @Test
    void honoursGranularity() {
        // 100Mi granularity: 900Mi/0.5 = 1800Mi needs no rounding at all.
        DiskSizePolicy.Decision decision =
                policy(0.80, 0.50, 100 * MI, MAX).decide(List.of(usage(900 * MI, GI)), GI);

        assertEquals(1800 * MI, decision.desiredSizeBytes());
    }

    @Test
    void rejectsConfigWhereResizeWouldImmediatelyRetrigger() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                new SizingConfig(0.50, 0.80, GI, MAX, GI));

        assertTrue(e.getMessage().contains("must be below"), e.getMessage());
    }
}
