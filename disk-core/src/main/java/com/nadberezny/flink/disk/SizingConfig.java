package com.nadberezny.flink.disk;

/**
 * The knobs of the resize policy, independent of who runs it (the operator, a KEDA-launched Job).
 *
 * @param threshold usage ratio at or above which a volume is grown
 * @param targetFill where usage should land after a resize; must be below {@code threshold}
 * @param minSizeBytes floor for the desired size
 * @param maxSizeBytes ceiling; above it the policy warns instead of growing
 * @param granularityBytes new sizes are rounded up to a multiple of this
 */
public record SizingConfig(
        double threshold,
        double targetFill,
        long minSizeBytes,
        long maxSizeBytes,
        long granularityBytes) {

    public SizingConfig {
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
    }

    public String describe() {
        return "threshold=" + pct(threshold)
                + " targetFill=" + pct(targetFill)
                + " size=[" + Quantities.format(minSizeBytes) + ".." + Quantities.format(maxSizeBytes) + "]"
                + " granularity=" + Quantities.format(granularityBytes);
    }

    public static String pct(double ratio) {
        return Math.round(ratio * 1000) / 10.0 + "%";
    }
}
