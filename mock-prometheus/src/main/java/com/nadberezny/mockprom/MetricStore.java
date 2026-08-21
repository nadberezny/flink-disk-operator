package com.nadberezny.mockprom;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.logging.Logger;

final class MetricStore {

    private static final Logger LOG = Logger.getLogger(MetricStore.class.getName());

    private final Map<String, VolumeSeries> series = new ConcurrentHashMap<>();
    private final LongSupplier clock;
    private final String seed;
    private final boolean autoRegister;
    private final Map<String, String> autoRegisterDefaults;

    MetricStore(String seed, boolean autoRegister, String autoRegisterDefaults, LongSupplier clock) {
        this.clock = clock;
        this.seed = seed == null ? "" : seed;
        this.autoRegister = autoRegister;
        this.autoRegisterDefaults = parseFields(autoRegisterDefaults == null ? "" : autoRegisterDefaults);
        reset();
    }

    long now() {
        return clock.getAsLong();
    }

    void reset() {
        series.clear();
        for (String entry : seed.split(";")) {
            if (entry.isBlank()) {
                continue;
            }
            try {
                Map<String, String> fields = parseFields(entry);
                upsert(fields);
            } catch (RuntimeException e) {
                LOG.warning("Skipping unparsable seed entry '" + entry.trim() + "': " + e.getMessage());
            }
        }
        LOG.info("Store holds " + series.size() + " volume(s) after reset");
    }

    VolumeSeries upsert(Map<String, String> fields) {
        String namespace = require(fields, "namespace");
        String pvc = require(fields, "pvc");
        Long capacity = optionalSize(fields, "capacity");
        Long used = optionalSize(fields, "used");
        Double growth = optionalGrowth(fields);
        String pod = fields.get("pod");
        String node = fields.get("node");
        long now = now();

        String key = namespace + "/" + pvc;
        VolumeSeries existing = series.get(key);
        if (existing == null) {
            if (capacity == null) {
                throw new IllegalArgumentException("'capacity' is required when creating a volume");
            }
            VolumeSeries created = new VolumeSeries(namespace, pvc, capacity,
                    used == null ? 0L : used, growth == null ? 0.0 : growth, pod, node, now);
            series.put(key, created);
            LOG.info("Registered volume " + key + " capacity=" + Sizes.format(capacity)
                    + " used=" + Sizes.format(created.usedBytes(now))
                    + " growth=" + (growth == null ? 0.0 : growth) + "B/s");
            return created;
        }
        existing.update(capacity, used, growth, pod, node, now);
        LOG.info("Updated volume " + key + " capacity=" + Sizes.format(existing.capacityBytes())
                + " used=" + Sizes.format(existing.usedBytes(now))
                + " growth=" + existing.growthBytesPerSecond() + "B/s");
        return existing;
    }

    boolean remove(String namespace, String pvc) {
        return series.remove(namespace + "/" + pvc) != null;
    }

    List<VolumeSeries> all() {
        List<VolumeSeries> copy = new ArrayList<>(series.values());
        copy.sort(Comparator.comparing(VolumeSeries::key));
        return copy;
    }

    void maybeAutoRegister(String namespace, String pvc) {
        if (!autoRegister || namespace == null || pvc == null) {
            return;
        }
        if (series.containsKey(namespace + "/" + pvc)) {
            return;
        }
        Map<String, String> fields = new LinkedHashMap<>(autoRegisterDefaults);
        fields.put("namespace", namespace);
        fields.put("pvc", pvc);
        fields.putIfAbsent("capacity", "1Gi");
        try {
            upsert(fields);
        } catch (RuntimeException e) {
            LOG.warning("Auto-register failed for " + namespace + "/" + pvc + ": " + e.getMessage());
        }
    }

    Collection<VolumeSeries> values() {
        return series.values();
    }

    static Map<String, String> parseFields(String raw) {
        Map<String, String> fields = new LinkedHashMap<>();
        for (String pair : raw.split(",")) {
            if (pair.isBlank()) {
                continue;
            }
            int eq = pair.indexOf('=');
            if (eq < 0) {
                throw new IllegalArgumentException("expected key=value, got '" + pair.trim() + "'");
            }
            fields.put(pair.substring(0, eq).trim(), pair.substring(eq + 1).trim());
        }
        return fields;
    }

    private static String require(Map<String, String> fields, String key) {
        String value = fields.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("'" + key + "' is required");
        }
        return value;
    }

    private static Long optionalSize(Map<String, String> fields, String key) {
        String value = fields.get(key);
        return value == null || value.isBlank() ? null : Sizes.parse(value);
    }

    private static Double optionalGrowth(Map<String, String> fields) {
        String value = fields.get("growth");
        if (value == null || value.isBlank()) {
            return null;
        }
        boolean negative = value.trim().startsWith("-");
        long magnitude = Sizes.parse(negative ? value.trim().substring(1) : value);
        return negative ? -(double) magnitude : (double) magnitude;
    }
}
