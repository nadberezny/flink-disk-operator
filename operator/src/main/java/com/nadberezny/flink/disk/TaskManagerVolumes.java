package com.nadberezny.flink.disk;

import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Volume;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.TaskManagerSpec;

import java.util.List;
import java.util.Optional;

/**
 * Finds the TaskManager's generic ephemeral volume in a FlinkDeployment spec.
 *
 * <p>Reading the current size from the spec rather than an annotation means the operator always
 * sees what is actually deployed, including whatever the mutator last injected.
 */
public final class TaskManagerVolumes {

    /** @param requestedSizeBytes {@code spec.resources.requests.storage} of the volume claim template */
    public record EphemeralVolume(String name, long requestedSizeBytes) {}

    private TaskManagerVolumes() {}

    /**
     * The first generic ephemeral volume declared on the TaskManager pod template, or empty if the
     * deployment has none (in which case there is nothing for this operator to manage).
     */
    public static Optional<EphemeralVolume> findEphemeralVolume(FlinkDeployment deployment) {
        return volumes(deployment).stream()
                .filter(v -> v.getEphemeral() != null)
                .flatMap(v -> toEphemeralVolume(v).stream())
                .findFirst();
    }

    /** Counts candidates so the caller can warn about an ambiguous spec. */
    public static long countEphemeralVolumes(FlinkDeployment deployment) {
        return volumes(deployment).stream().filter(v -> v.getEphemeral() != null).count();
    }

    private static Optional<EphemeralVolume> toEphemeralVolume(Volume volume) {
        Quantity storage = Optional.ofNullable(volume.getEphemeral().getVolumeClaimTemplate())
                .map(t -> t.getSpec())
                .map(s -> s.getResources())
                .map(r -> r.getRequests())
                .map(requests -> requests.get("storage"))
                .orElse(null);
        if (storage == null || volume.getName() == null) {
            return Optional.empty();
        }
        return Optional.of(new EphemeralVolume(
                volume.getName(), Quantity.getAmountInBytes(storage).longValueExact()));
    }

    private static List<Volume> volumes(FlinkDeployment deployment) {
        return Optional.ofNullable(deployment)
                .map(FlinkDeployment::getSpec)
                .map(FlinkDeploymentSpec::getTaskManager)
                .map(TaskManagerSpec::getPodTemplate)
                .map(PodTemplateSpec::getSpec)
                .map(spec -> spec.getVolumes())
                .orElse(List.of());
    }
}
