package com.nadberezny.mockprom;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PromQlTest {

    private static final long T0 = 1_700_000_000_000L;

    private MetricStore store(String seed) {
        return new MetricStore(seed, false, "", () -> T0);
    }

    @Test
    void selectsUsedBytesForSeededVolume() {
        MetricStore store = store("namespace=stream,pvc=tm-1-storage,capacity=1Gi,used=512Mi");

        List<PromQl.Sample> samples =
                PromQl.evaluate("kubelet_volume_stats_used_bytes{namespace=\"stream\"}", store);

        assertEquals(1, samples.size());
        assertEquals(536870912.0, samples.get(0).value());
        Map<String, String> labels = samples.get(0).labels();
        assertEquals("kubelet_volume_stats_used_bytes", labels.get("__name__"));
        assertEquals("tm-1-storage", labels.get("persistentvolumeclaim"));
    }

    @Test
    void filtersOnRegexMatcher() {
        MetricStore store = store("namespace=stream,pvc=tm-1-storage,capacity=1Gi,used=1Mi"
                + ";namespace=stream,pvc=other-storage,capacity=1Gi,used=2Mi");

        List<PromQl.Sample> samples = PromQl.evaluate(
                "kubelet_volume_stats_used_bytes{persistentvolumeclaim=~\"tm-.*\"}", store);

        assertEquals(1, samples.size());
        assertEquals("tm-1-storage", samples.get(0).labels().get("persistentvolumeclaim"));
    }

    @Test
    void dividesUsedByCapacityAndDropsMetricName() {
        MetricStore store = store("namespace=stream,pvc=tm-1-storage,capacity=1Gi,used=880Mi");

        List<PromQl.Sample> samples = PromQl.evaluate(
                "kubelet_volume_stats_used_bytes{namespace=\"stream\"}"
                        + " / kubelet_volume_stats_capacity_bytes{namespace=\"stream\"}", store);

        assertEquals(1, samples.size());
        assertEquals(880.0 / 1024.0, samples.get(0).value(), 1e-9);
        assertTrue(samples.get(0).labels().containsKey("namespace"));
        assertEquals(null, samples.get(0).labels().get("__name__"));
    }

    @Test
    void availableIsCapacityMinusUsed() {
        MetricStore store = store("namespace=stream,pvc=tm-1-storage,capacity=1Gi,used=1Gi");

        List<PromQl.Sample> samples =
                PromQl.evaluate("kubelet_volume_stats_available_bytes", store);

        assertEquals(0.0, samples.get(0).value());
    }

    @Test
    void growthAdvancesUsageAndSaturatesAtCapacity() {
        long[] clock = {T0};
        MetricStore store = new MetricStore(
                "namespace=stream,pvc=tm-1-storage,capacity=1Ki,used=0,growth=100",
                false, "", () -> clock[0]);

        assertEquals(0.0, usedBytes(store));
        clock[0] = T0 + 5_000;
        assertEquals(500.0, usedBytes(store));
        clock[0] = T0 + 1_000_000;
        assertEquals(1024.0, usedBytes(store), "usage must not exceed capacity");
    }

    @Test
    void autoRegisterCreatesUnknownVolumeOnExactMatch() {
        MetricStore store = new MetricStore("", true, "capacity=2Gi,used=1Gi", () -> T0);

        List<PromQl.Sample> samples = PromQl.evaluate(
                "kubelet_volume_stats_used_bytes{namespace=\"stream\","
                        + "persistentvolumeclaim=\"brand-new-pvc\"}", store);

        assertEquals(1, samples.size());
        assertEquals(1073741824.0, samples.get(0).value());
    }

    @Test
    void maxOverDivisionPicksTheFullestVolumeAndDropsLabels() {
        MetricStore store = store("namespace=stream,pvc=tm-1-storage,capacity=1Gi,used=880Mi"
                + ";namespace=stream,pvc=tm-2-storage,capacity=1Gi,used=100Mi");

        List<PromQl.Sample> samples = PromQl.evaluate(
                "max(kubelet_volume_stats_used_bytes{namespace=\"stream\"}"
                        + " / kubelet_volume_stats_capacity_bytes{namespace=\"stream\"})", store);

        assertEquals(1, samples.size());
        assertEquals(880.0 / 1024.0, samples.get(0).value(), 1e-9);
        assertEquals(Map.of(), samples.get(0).labels());
    }

    @Test
    void aggregatesSumCountMinAvgOverASelector() {
        MetricStore store = store("namespace=stream,pvc=tm-1-storage,capacity=1Gi,used=1Mi"
                + ";namespace=stream,pvc=tm-2-storage,capacity=1Gi,used=3Mi");

        assertEquals(2.0, PromQl.evaluate("count(kubelet_volume_stats_used_bytes)", store).get(0).value());
        assertEquals(4.0 * 1024 * 1024, PromQl.evaluate("sum(kubelet_volume_stats_used_bytes)", store).get(0).value());
        assertEquals(1.0 * 1024 * 1024, PromQl.evaluate("min(kubelet_volume_stats_used_bytes)", store).get(0).value());
        assertEquals(2.0 * 1024 * 1024, PromQl.evaluate(" avg (kubelet_volume_stats_used_bytes) ", store).get(0).value());
    }

    @Test
    void aggregationOfNothingIsAnEmptyVector() {
        MetricStore store = store("namespace=stream,pvc=tm-1-storage,capacity=1Gi,used=1Mi");

        List<PromQl.Sample> samples = PromQl.evaluate(
                "max(kubelet_volume_stats_used_bytes{namespace=\"nowhere\"})", store);

        assertTrue(samples.isEmpty());
    }

    @Test
    void rejectsUnsupportedQueries() {
        MetricStore store = store("");

        assertThrows(PromQl.BadQueryException.class,
                () -> PromQl.evaluate("topk(1, kubelet_volume_stats_used_bytes)", store));
        assertThrows(PromQl.BadQueryException.class,
                () -> PromQl.evaluate("max(kubelet_volume_stats_used_bytes", store));
        assertThrows(PromQl.BadQueryException.class,
                () -> PromQl.evaluate("max by (namespace) (kubelet_volume_stats_used_bytes)", store));
        assertThrows(PromQl.BadQueryException.class,
                () -> PromQl.evaluate("max(kubelet_volume_stats_used_bytes) by (namespace)", store));
        assertThrows(PromQl.BadQueryException.class,
                () -> PromQl.evaluate("node_filesystem_free_bytes", store));
        assertThrows(PromQl.BadQueryException.class, () -> PromQl.evaluate("", store));
    }

    @Test
    void parsesKubernetesQuantities() {
        assertEquals(1024L, Sizes.parse("1Ki"));
        assertEquals(1073741824L, Sizes.parse("1Gi"));
        assertEquals(1000000L, Sizes.parse("1M"));
        assertEquals(1536L, Sizes.parse("1.5Ki"));
        assertEquals(42L, Sizes.parse("42"));
        assertEquals("2Gi", Sizes.format(2147483648L));
    }

    private static double usedBytes(MetricStore store) {
        return PromQl.evaluate("kubelet_volume_stats_used_bytes", store).get(0).value();
    }
}
