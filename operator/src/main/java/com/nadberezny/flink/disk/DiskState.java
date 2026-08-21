package com.nadberezny.flink.disk;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The bit of memory that has to survive an operator restart: what size we last decided this
 * deployment's TaskManager volume needs.
 *
 * <p>Everything else in here is observability — useful when watching a demo, not load-bearing.
 */
public record DiskState(
        long desiredSizeBytes,
        String volumeName,
        long resizeGeneration,
        String lastReason,
        boolean atMaxSize) {

    public static DiskState initial(long desiredSizeBytes, String volumeName) {
        return new DiskState(desiredSizeBytes, volumeName, 0L, "initialised from FlinkDeployment spec", false);
    }

    public static DiskState fromConfigMapData(Map<String, String> data, DiskState fallback) {
        if (data == null || !data.containsKey(Names.STATE_DESIRED_SIZE)) {
            return fallback;
        }
        try {
            return new DiskState(
                    Quantities.toBytes(data.get(Names.STATE_DESIRED_SIZE)),
                    data.getOrDefault(Names.STATE_VOLUME_NAME, fallback.volumeName()),
                    Long.parseLong(data.getOrDefault(Names.STATE_RESIZE_GENERATION, "0")),
                    data.getOrDefault(Names.STATE_LAST_REASON, ""),
                    Boolean.parseBoolean(data.getOrDefault("atMaxSize", "false")));
        } catch (RuntimeException e) {
            // A hand-edited or corrupted ConfigMap should not wedge the operator: fall back to
            // what the spec says and let the next decision overwrite it.
            return fallback;
        }
    }

    public Map<String, String> toConfigMapData(DiskSizePolicy.Decision decision, String updatedAt) {
        Map<String, String> data = new LinkedHashMap<>();
        data.put(Names.STATE_DESIRED_SIZE, Quantities.format(desiredSizeBytes));
        data.put(Names.STATE_VOLUME_NAME, volumeName);
        data.put(Names.STATE_RESIZE_GENERATION, Long.toString(resizeGeneration));
        data.put(Names.STATE_LAST_REASON, lastReason);
        data.put("atMaxSize", Boolean.toString(atMaxSize));
        data.put(Names.STATE_UPDATED_AT, updatedAt);
        decision.worst().ifPresent(worst -> {
            data.put(Names.STATE_OBSERVED_PVC, worst.persistentVolumeClaim());
            data.put(Names.STATE_OBSERVED_USED, Quantities.format(worst.usedBytes()));
            data.put(Names.STATE_OBSERVED_CAPACITY, Quantities.format(worst.capacityBytes()));
            data.put(Names.STATE_OBSERVED_RATIO, OperatorConfig.pct(worst.ratio()));
        });
        return data;
    }

    public DiskState applying(DiskSizePolicy.Decision decision) {
        return new DiskState(
                decision.desiredSizeBytes(),
                volumeName,
                decision.changed() ? resizeGeneration + 1 : resizeGeneration,
                decision.reason(),
                decision.atMaxSize());
    }
}
