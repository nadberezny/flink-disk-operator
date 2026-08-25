package com.nadberezny.flink.disk;

import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.TaskManagerSpec;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.nadberezny.flink.disk.Quantities.GI;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskManagerVolumesTest {

    private static Volume ephemeral(String name, String size) {
        return new VolumeBuilder()
                .withName(name)
                .withNewEphemeral()
                .withNewVolumeClaimTemplate()
                .withNewSpec()
                .withAccessModes("ReadWriteOnce")
                .withNewResources()
                .withRequests(Map.of("storage", new Quantity(size)))
                .endResources()
                .endSpec()
                .endVolumeClaimTemplate()
                .endEphemeral()
                .build();
    }

    private static Volume emptyDir(String name) {
        return new VolumeBuilder().withName(name).withNewEmptyDir().endEmptyDir().build();
    }

    private static FlinkDeployment deploymentWith(List<Volume> volumes) {
        FlinkDeployment deployment = new FlinkDeployment();
        FlinkDeploymentSpec spec = new FlinkDeploymentSpec();
        TaskManagerSpec taskManager = new TaskManagerSpec();
        taskManager.setPodTemplate(new PodTemplateSpecBuilder()
                .withNewSpec()
                .withVolumes(volumes)
                .endSpec()
                .build());
        spec.setTaskManager(taskManager);
        deployment.setSpec(spec);
        return deployment;
    }

    @Test
    void readsNameAndSizeFromTheEphemeralVolume() {
        FlinkDeployment deployment = deploymentWith(List.of(ephemeral("local-storage", "2Gi")));

        TaskManagerVolumes.EphemeralVolume volume =
                TaskManagerVolumes.findEphemeralVolume(deployment).orElseThrow();

        assertEquals("local-storage", volume.name());
        assertEquals(2 * GI, volume.requestedSizeBytes());
    }

    @Test
    void ignoresNonEphemeralVolumes() {
        FlinkDeployment deployment = deploymentWith(
                List.of(emptyDir("scratch"), ephemeral("local-storage", "1Gi")));

        assertEquals("local-storage",
                TaskManagerVolumes.findEphemeralVolume(deployment).orElseThrow().name());
        assertEquals(1, TaskManagerVolumes.countEphemeralVolumes(deployment));
    }

    @Test
    void reportsMultipleCandidatesSoTheCallerCanWarn() {
        FlinkDeployment deployment = deploymentWith(
                List.of(ephemeral("first", "1Gi"), ephemeral("second", "4Gi")));

        assertEquals(2, TaskManagerVolumes.countEphemeralVolumes(deployment));
        assertEquals("first", TaskManagerVolumes.findEphemeralVolume(deployment).orElseThrow().name());
    }

    @Test
    void emptyWhenTaskManagerHasNoVolumes() {
        assertTrue(TaskManagerVolumes.findEphemeralVolume(deploymentWith(List.of())).isEmpty());
        assertTrue(TaskManagerVolumes.findEphemeralVolume(new FlinkDeployment()).isEmpty());
        assertTrue(TaskManagerVolumes.findEphemeralVolume(null).isEmpty());
    }

    @Test
    void emptyWhenEphemeralVolumeDeclaresNoStorageRequest() {
        Volume noRequest = new VolumeBuilder()
                .withName("local-storage")
                .withNewEphemeral()
                .withNewVolumeClaimTemplate()
                .withNewSpec()
                .endSpec()
                .endVolumeClaimTemplate()
                .endEphemeral()
                .build();

        Optional<TaskManagerVolumes.EphemeralVolume> found =
                TaskManagerVolumes.findEphemeralVolume(deploymentWith(List.of(noRequest)));

        assertTrue(found.isEmpty());
    }
}
