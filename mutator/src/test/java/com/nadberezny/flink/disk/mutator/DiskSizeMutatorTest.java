package com.nadberezny.flink.disk.mutator;

import com.nadberezny.flink.disk.Names;
import io.fabric8.kubernetes.api.model.ObjectMeta;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DiskSizeMutatorTest {

    // Fixtures are package-visible: PluginClassLoadingTest reuses them to build a
    // parent-classloader FlinkDeployment and hand it to the plugin-loaded mutator.

    private static final long GI = 1024L * 1024 * 1024;

    // ---- fixtures ----

    static Volume ephemeral(String name, String size) {
        var builder = new VolumeBuilder()
                .withName(name)
                .withNewEphemeral()
                .withNewVolumeClaimTemplate()
                .withNewSpec()
                .withAccessModes("ReadWriteOnce");
        if (size != null) {
            builder = builder.withNewResources()
                    .withRequests(Map.of("storage", new Quantity(size)))
                    .endResources();
        }
        return builder.endSpec().endVolumeClaimTemplate().endEphemeral().build();
    }

    static FlinkDeployment deployment(boolean managed, List<Volume> volumes) {
        FlinkDeployment deployment = new FlinkDeployment();
        ObjectMeta meta = new ObjectMeta();
        meta.setName("flink-disk-job");
        meta.setNamespace("stream");
        if (managed) {
            meta.setLabels(Map.of(Names.MANAGED_LABEL, "true"));
        }
        deployment.setMetadata(meta);

        TaskManagerSpec taskManager = new TaskManagerSpec();
        taskManager.setPodTemplate(
                new PodTemplateSpecBuilder().withNewSpec().withVolumes(volumes).endSpec().build());
        FlinkDeploymentSpec spec = new FlinkDeploymentSpec();
        spec.setTaskManager(taskManager);
        deployment.setSpec(spec);
        return deployment;
    }

    private static String storageOf(FlinkDeployment deployment, String volumeName) {
        Quantity q = deployment.getSpec().getTaskManager().getPodTemplate().getSpec().getVolumes()
                .stream()
                .filter(v -> v.getName().equals(volumeName))
                .findFirst().orElseThrow()
                .getEphemeral().getVolumeClaimTemplate().getSpec()
                .getResources().getRequests().get("storage");
        return q == null ? null : q.toString();
    }

    private static DesiredSizeSource returning(DesiredSize size) {
        return (namespace, name) -> Optional.ofNullable(size);
    }

    // ---- behaviour ----

    @Test
    void appliesTheDesiredSizeToTheNamedVolume() {
        FlinkDeployment deployment = deployment(true, List.of(ephemeral("local-storage", "1Gi")));
        var mutator = new DiskSizeMutator(returning(new DesiredSize("2Gi", 2 * GI, "local-storage")));

        mutator.mutateDeployment(deployment);

        assertEquals("2Gi", storageOf(deployment, "local-storage"));
    }

    @Test
    void survivesAHelmUpgradeThatResetsTheSize() {
        // The whole point of keeping desired size in a ConfigMap: the chart re-applies 1Gi and the
        // mutator puts it back on the way in.
        var mutator = new DiskSizeMutator(returning(new DesiredSize("4Gi", 4 * GI, "local-storage")));

        FlinkDeployment fromChart = deployment(true, List.of(ephemeral("local-storage", "1Gi")));
        mutator.mutateDeployment(fromChart);

        assertEquals("4Gi", storageOf(fromChart, "local-storage"));
    }

    @Test
    void neverShrinksAVolume() {
        FlinkDeployment deployment = deployment(true, List.of(ephemeral("local-storage", "8Gi")));
        var mutator = new DiskSizeMutator(returning(new DesiredSize("2Gi", 2 * GI, "local-storage")));

        mutator.mutateDeployment(deployment);

        assertEquals("8Gi", storageOf(deployment, "local-storage"));
    }

    @Test
    void leavesTheSpecAloneWhenAlreadyExact() {
        FlinkDeployment deployment = deployment(true, List.of(ephemeral("local-storage", "2Gi")));
        var mutator = new DiskSizeMutator(returning(new DesiredSize("2Gi", 2 * GI, "local-storage")));

        mutator.mutateDeployment(deployment);

        assertEquals("2Gi", storageOf(deployment, "local-storage"));
    }

    @Test
    void ignoresUnlabelledDeploymentsWithoutEvenLookingUpState() {
        FlinkDeployment deployment = deployment(false, List.of(ephemeral("local-storage", "1Gi")));
        DesiredSizeSource exploding = (ns, n) -> {
            throw new AssertionError("must not consult state for an unmanaged deployment");
        };

        new DiskSizeMutator(exploding).mutateDeployment(deployment);

        assertEquals("1Gi", storageOf(deployment, "local-storage"));
    }

    @Test
    void passesThroughWhenNoStateRecordedYet() {
        FlinkDeployment deployment = deployment(true, List.of(ephemeral("local-storage", "1Gi")));

        new DiskSizeMutator(returning(null)).mutateDeployment(deployment);

        assertEquals("1Gi", storageOf(deployment, "local-storage"));
    }

    @Test
    void failsOpenWhenTheStateLookupThrows() {
        // failurePolicy: Fail means an exception escaping here blocks every FlinkDeployment
        // create/update in the cluster.
        FlinkDeployment deployment = deployment(true, List.of(ephemeral("local-storage", "1Gi")));
        DesiredSizeSource broken = (ns, n) -> {
            throw new IllegalStateException("API server unreachable");
        };

        FlinkDeployment returned = new DiskSizeMutator(broken).mutateDeployment(deployment);

        assertSame(deployment, returned);
        assertEquals("1Gi", storageOf(deployment, "local-storage"));
    }

    @Test
    void ignoresVolumesItDoesNotManage() {
        FlinkDeployment deployment = deployment(true,
                List.of(ephemeral("local-storage", "1Gi"), ephemeral("other", "1Gi")));
        var mutator = new DiskSizeMutator(returning(new DesiredSize("2Gi", 2 * GI, "local-storage")));

        mutator.mutateDeployment(deployment);

        assertEquals("2Gi", storageOf(deployment, "local-storage"));
        assertEquals("1Gi", storageOf(deployment, "other"));
    }

    @Test
    void reportsWhenTheNamedVolumeIsGone() {
        FlinkDeployment deployment = deployment(true, List.of(ephemeral("renamed", "1Gi")));

        var result = EphemeralVolumeSizer.apply(
                deployment, new DesiredSize("2Gi", 2 * GI, "local-storage"));

        assertEquals(EphemeralVolumeSizer.Outcome.VOLUME_NOT_FOUND, result.outcome());
    }

    @Test
    void doesNotInventAStorageRequest() {
        FlinkDeployment deployment = deployment(true, List.of(ephemeral("local-storage", null)));

        var result = EphemeralVolumeSizer.apply(
                deployment, new DesiredSize("2Gi", 2 * GI, "local-storage"));

        assertEquals(EphemeralVolumeSizer.Outcome.NO_STORAGE_REQUEST, result.outcome());
        assertNull(deployment.getSpec().getTaskManager().getPodTemplate().getSpec()
                .getVolumes().get(0).getEphemeral().getVolumeClaimTemplate().getSpec()
                .getResources());
    }

    @Test
    void toleratesADeploymentWithNoTaskManagerAtAll() {
        FlinkDeployment bare = new FlinkDeployment();
        ObjectMeta meta = new ObjectMeta();
        meta.setName("x");
        meta.setNamespace("stream");
        meta.setLabels(Map.of(Names.MANAGED_LABEL, "true"));
        bare.setMetadata(meta);

        var mutator = new DiskSizeMutator(returning(new DesiredSize("2Gi", 2 * GI, "local-storage")));

        assertSame(bare, mutator.mutateDeployment(bare));
    }

    @Test
    void reportsOutcomeAndPreviousSizeForLogging() {
        FlinkDeployment deployment = deployment(true, List.of(ephemeral("local-storage", "1Gi")));

        var result = EphemeralVolumeSizer.apply(
                deployment, new DesiredSize("2Gi", 2 * GI, "local-storage"));

        assertTrue(result.changed());
        assertEquals(GI, result.previousBytes());
        assertEquals(2 * GI, result.appliedBytes());
    }
}
