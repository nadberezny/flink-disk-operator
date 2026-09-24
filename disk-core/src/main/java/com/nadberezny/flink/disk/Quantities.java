package com.nadberezny.flink.disk;

import io.fabric8.kubernetes.api.model.Quantity;

/** Kubernetes quantity <-> bytes, plus the rounding the resize policy needs. */
public final class Quantities {

    public static final long KI = 1024L;
    public static final long MI = 1024L * KI;
    public static final long GI = 1024L * MI;
    public static final long TI = 1024L * GI;

    private Quantities() {}

    public static long toBytes(String quantity) {
        return Quantity.getAmountInBytes(Quantity.parse(quantity)).longValueExact();
    }

    /** Renders using the largest binary unit that divides evenly, so sizes stay readable. */
    public static String format(long bytes) {
        long[] scales = {TI, GI, MI, KI};
        String[] units = {"Ti", "Gi", "Mi", "Ki"};
        for (int i = 0; i < scales.length; i++) {
            if (bytes >= scales[i] && bytes % scales[i] == 0) {
                return (bytes / scales[i]) + units[i];
            }
        }
        return Long.toString(bytes);
    }

    /** Rounds up to the next multiple of {@code granularity}; {@code granularity <= 0} is a no-op. */
    public static long roundUp(long bytes, long granularity) {
        if (granularity <= 0) {
            return bytes;
        }
        long remainder = bytes % granularity;
        return remainder == 0 ? bytes : bytes + (granularity - remainder);
    }
}
