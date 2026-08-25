package com.nadberezny.flink.disk.mutator;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Volume;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Writes a storage size into a named generic ephemeral volume on the TaskManager pod template.
 *
 * <p>Pure and side-effect-visible: it mutates the deployment in place and reports what it did, so
 * the plugin entry point can log a decision without re-deriving it.
 */
public final class EphemeralVolumeSizer {

    /** What happened, for logging and for tests to assert on. */
    public enum Outcome {
        /** The size was raised. */
        APPLIED,
        /** The spec already requests at least the desired size; left alone. */
        ALREADY_LARGE_ENOUGH,
        /** No generic ephemeral volume by that name on the TaskManager. */
        VOLUME_NOT_FOUND,
        /** The volume exists but declares no {@code resources.requests.storage} to compare against. */
        NO_STORAGE_REQUEST
    }

    public record Result(Outcome outcome, long previousBytes, long appliedBytes) {
        public boolean changed() {
            return outcome == Outcome.APPLIED;
        }
    }

    private static final String STORAGE = "storage";

    private EphemeralVolumeSizer() {}

    public static Result apply(FlinkDeployment deployment, DesiredSize desired) {
        Optional<Volume> found = findEphemeralVolume(deployment, desired.volumeName());
        if (found.isEmpty()) {
            return new Result(Outcome.VOLUME_NOT_FOUND, 0, 0);
        }

        var claimSpec = found.get().getEphemeral().getVolumeClaimTemplate().getSpec();
        var resources = claimSpec.getResources();
        Quantity current = resources == null || resources.getRequests() == null
                ? null
                : resources.getRequests().get(STORAGE);
        if (current == null) {
            // Adding a storage request where the author declared none would be a bigger change
            // than this mutator should make silently, and the operator would have had no size to
            // compare against either.
            return new Result(Outcome.NO_STORAGE_REQUEST, 0, 0);
        }

        long currentBytes = Quantity.getAmountInBytes(current).longValueExact();
        if (currentBytes >= desired.bytes()) {
            // Only ever grow. A spec that already asks for more was set deliberately, and the
            // operator adopts such sizes rather than fighting them.
            return new Result(Outcome.ALREADY_LARGE_ENOUGH, currentBytes, currentBytes);
        }

        // Copy rather than mutate: the requests map may be immutable depending on how the
        // resource was deserialised.
        Map<String, Quantity> requests = new LinkedHashMap<>(resources.getRequests());
        requests.put(STORAGE, Quantity.parse(desired.quantity()));
        resources.setRequests(requests);

        return new Result(Outcome.APPLIED, currentBytes, desired.bytes());
    }

    private static Optional<Volume> findEphemeralVolume(FlinkDeployment deployment, String volumeName) {
        if (volumeName == null) {
            return Optional.empty();
        }
        return volumes(deployment).stream()
                .filter(v -> volumeName.equals(v.getName()))
                .filter(v -> v.getEphemeral() != null)
                .filter(v -> v.getEphemeral().getVolumeClaimTemplate() != null)
                .filter(v -> v.getEphemeral().getVolumeClaimTemplate().getSpec() != null)
                .findFirst();
    }

    private static List<Volume> volumes(FlinkDeployment deployment) {
        if (deployment == null || deployment.getSpec() == null
                || deployment.getSpec().getTaskManager() == null
                || deployment.getSpec().getTaskManager().getPodTemplate() == null
                || deployment.getSpec().getTaskManager().getPodTemplate().getSpec() == null) {
            return List.of();
        }
        List<Volume> volumes =
                deployment.getSpec().getTaskManager().getPodTemplate().getSpec().getVolumes();
        return volumes == null ? List.of() : volumes;
    }
}
