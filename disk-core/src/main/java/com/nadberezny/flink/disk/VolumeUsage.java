package com.nadberezny.flink.disk;

/** One TaskManager volume's disk usage as reported by Prometheus. */
public record VolumeUsage(String persistentVolumeClaim, long usedBytes, long capacityBytes) {

    public double ratio() {
        return capacityBytes <= 0 ? 0.0 : (double) usedBytes / capacityBytes;
    }

    @Override
    public String toString() {
        return persistentVolumeClaim + " " + Quantities.format(usedBytes) + "/"
                + Quantities.format(capacityBytes) + " (" + SizingConfig.pct(ratio()) + ")";
    }
}
