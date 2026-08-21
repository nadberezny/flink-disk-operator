package com.nadberezny.flink.disk;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Decides the desired volume size. Pure function of (observed usage, current desired size, config)
 * so the interesting behaviour is testable without a cluster or a Prometheus.
 *
 * <p>Policy is "target headroom": once the fullest volume crosses the threshold, pick a size that
 * puts <em>current</em> usage at {@code targetFill}. That converges in a single step no matter how
 * far past the threshold usage got, unlike a fixed or multiplicative step.
 *
 * <p>Sizes only ever grow. Shrinking would mean destroying a volume that currently holds data, and
 * for ephemeral state that trade is not worth making automatically.
 */
public final class DiskSizePolicy {

    private final OperatorConfig config;

    public DiskSizePolicy(OperatorConfig config) {
        this.config = config;
    }

    /**
     * @param desiredSizeBytes size to record as desired (unchanged from current unless {@code changed})
     * @param changed whether this differs from the current desired size
     * @param atMaxSize true when the policy wanted more room but hit {@code DISK_MAX_SIZE}
     * @param reason human-readable explanation, persisted to the state ConfigMap
     * @param worst the volume the decision was based on, if any usage was observed
     */
    public record Decision(
            long desiredSizeBytes,
            boolean changed,
            boolean atMaxSize,
            String reason,
            Optional<VolumeUsage> worst) {}

    public Decision decide(List<VolumeUsage> usages, long currentDesiredBytes) {
        long floor = Math.max(currentDesiredBytes, config.minSizeBytes());

        Optional<VolumeUsage> worst = usages.stream()
                .filter(u -> u.capacityBytes() > 0)
                .max(Comparator.comparingDouble(VolumeUsage::ratio));

        if (worst.isEmpty()) {
            // No TaskManager volumes reporting yet: pods still starting, or the metric has not
            // been scraped. Not an error, just nothing to decide on.
            return unchanged(floor, currentDesiredBytes, "no volume metrics available", worst);
        }

        VolumeUsage v = worst.get();
        if (v.ratio() < config.threshold()) {
            return unchanged(floor, currentDesiredBytes,
                    "below threshold: " + v + " < " + OperatorConfig.pct(config.threshold()), worst);
        }

        long wanted = Quantities.roundUp(
                (long) Math.ceil(v.usedBytes() / config.targetFill()), config.granularityBytes());
        // Never shrink, never dip under the floor.
        long candidate = Math.max(wanted, floor);
        long capped = Math.min(candidate, config.maxSizeBytes());
        boolean atMax = candidate > config.maxSizeBytes();

        if (capped > currentDesiredBytes) {
            return new Decision(capped, true, atMax,
                    "usage " + v + " >= " + OperatorConfig.pct(config.threshold())
                            + ", growing " + Quantities.format(currentDesiredBytes) + " -> "
                            + Quantities.format(capped)
                            + (atMax ? " (clamped to max " + Quantities.format(config.maxSizeBytes()) + ")" : ""),
                    worst);
        }

        // Over threshold but the size cannot go up: either already at max, or the rounded target
        // landed on the size we already have.
        String reason = atMax || capped >= config.maxSizeBytes()
                ? "usage " + v + " >= " + OperatorConfig.pct(config.threshold())
                        + " but already at max size " + Quantities.format(config.maxSizeBytes())
                : "usage " + v + " >= " + OperatorConfig.pct(config.threshold())
                        + " but target size rounds to the current " + Quantities.format(currentDesiredBytes);
        return new Decision(currentDesiredBytes, false, atMax || capped >= config.maxSizeBytes(),
                reason, worst);
    }

    /**
     * Even with no resize, the recorded size may need to rise to the configured minimum — that is
     * still a change worth persisting.
     */
    private Decision unchanged(long floor, long currentDesiredBytes, String reason,
                               Optional<VolumeUsage> worst) {
        if (floor > currentDesiredBytes) {
            return new Decision(floor, true, false,
                    "raising to configured minimum " + Quantities.format(floor) + " (" + reason + ")",
                    worst);
        }
        return new Decision(currentDesiredBytes, false, false, reason, worst);
    }
}
