package com.nadberezny.mockprom;

import java.util.LinkedHashMap;
import java.util.Map;

final class VolumeSeries {

    private final String namespace;
    private final String persistentVolumeClaim;
    private volatile String pod;
    private volatile String node;
    private volatile long capacityBytes;
    private volatile long baseUsedBytes;
    private volatile double growthBytesPerSecond;
    private volatile long baseAtMillis;

    VolumeSeries(String namespace, String persistentVolumeClaim, long capacityBytes,
                 long usedBytes, double growthBytesPerSecond, String pod, String node,
                 long nowMillis) {
        this.namespace = namespace;
        this.persistentVolumeClaim = persistentVolumeClaim;
        this.capacityBytes = capacityBytes;
        this.baseUsedBytes = usedBytes;
        this.growthBytesPerSecond = growthBytesPerSecond;
        this.pod = pod;
        this.node = node;
        this.baseAtMillis = nowMillis;
    }

    String namespace() {
        return namespace;
    }

    String persistentVolumeClaim() {
        return persistentVolumeClaim;
    }

    String pod() {
        return pod;
    }

    String node() {
        return node;
    }

    long capacityBytes() {
        return capacityBytes;
    }

    double growthBytesPerSecond() {
        return growthBytesPerSecond;
    }

    long usedBytes(long nowMillis) {
        double elapsedSeconds = Math.max(0, nowMillis - baseAtMillis) / 1000.0;
        double used = baseUsedBytes + growthBytesPerSecond * elapsedSeconds;
        return (long) Math.max(0, Math.min(capacityBytes, used));
    }

    long availableBytes(long nowMillis) {
        return Math.max(0, capacityBytes - usedBytes(nowMillis));
    }

    synchronized void update(Long newCapacityBytes, Long newUsedBytes, Double newGrowth,
                            String newPod, String newNode, long nowMillis) {
        long carriedUsed = usedBytes(nowMillis);
        if (newCapacityBytes != null) {
            capacityBytes = newCapacityBytes;
        }
        baseUsedBytes = newUsedBytes != null ? newUsedBytes : carriedUsed;
        baseAtMillis = nowMillis;
        if (newGrowth != null) {
            growthBytesPerSecond = newGrowth;
        }
        if (newPod != null) {
            pod = newPod;
        }
        if (newNode != null) {
            node = newNode;
        }
    }

    Map<String, String> labels() {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("namespace", namespace);
        labels.put("persistentvolumeclaim", persistentVolumeClaim);
        if (node != null && !node.isBlank()) {
            labels.put("node", node);
        }
        if (pod != null && !pod.isBlank()) {
            labels.put("pod", pod);
        }
        return labels;
    }

    String key() {
        return namespace + "/" + persistentVolumeClaim;
    }
}
