package com.nadberezny.flink.disk.keda;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkDeploymentSpec;
import org.apache.flink.kubernetes.operator.api.spec.TaskManagerSpec;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpecPatchTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Instant NOW = Instant.parse("2026-09-22T10:00:00Z");

    private static Volume ephemeral(String name, String size) {
        return new VolumeBuilder()
                .withName(name)
                .withNewEphemeral()
                .withNewVolumeClaimTemplate()
                .withNewSpec()
                .withNewResources()
                .withRequests(Map.of("storage", new Quantity(size)))
                .endResources()
                .endSpec()
                .endVolumeClaimTemplate()
                .endEphemeral()
                .build();
    }

    private static FlinkDeployment deployment(String resourceVersion, Map<String, String> annotations,
                                              Volume... volumes) {
        FlinkDeployment fd = new FlinkDeployment();
        fd.setMetadata(new ObjectMetaBuilder()
                .withName("flink-disk-job")
                .withNamespace("stream")
                .withResourceVersion(resourceVersion)
                .withAnnotations(annotations)
                .build());
        FlinkDeploymentSpec spec = new FlinkDeploymentSpec();
        TaskManagerSpec tm = new TaskManagerSpec();
        tm.setPodTemplate(new PodTemplateSpecBuilder()
                .withNewSpec().withVolumes(List.of(volumes)).endSpec()
                .build());
        spec.setTaskManager(tm);
        fd.setSpec(spec);
        return fd;
    }

    @Test
    void findsTheVolumeIndexByName() {
        FlinkDeployment fd = deployment("1", Map.of(),
                new VolumeBuilder().withName("config").withNewEmptyDir().endEmptyDir().build(),
                ephemeral("local-storage", "1Gi"));

        assertEquals(Optional.of(1), SpecPatch.volumeIndex(fd, "local-storage"));
        assertEquals(Optional.empty(), SpecPatch.volumeIndex(fd, "nope"));
    }

    @Test
    void locksOnResourceVersionThenReplacesStorageAndStampsAnnotations() throws Exception {
        FlinkDeployment fd = deployment("4711", Map.of("meta.helm.sh/release-name", "flink-disk-job"),
                ephemeral("local-storage", "1Gi"));

        JsonNode ops = MAPPER.readTree(SpecPatch.build(fd, 0, "2Gi", "usage high", NOW));

        assertEquals(4, ops.size(), ops.toString());
        assertEquals("test", ops.get(0).get("op").asText());
        assertEquals("/metadata/resourceVersion", ops.get(0).get("path").asText());
        assertEquals("4711", ops.get(0).get("value").asText());

        assertEquals("replace", ops.get(1).get("op").asText());
        assertEquals("/spec/taskManager/podTemplate/spec/volumes/0/ephemeral/volumeClaimTemplate/spec/resources/requests/storage",
                ops.get(1).get("path").asText());
        assertEquals("2Gi", ops.get(1).get("value").asText());

        assertEquals("add", ops.get(2).get("op").asText());
        assertEquals("/metadata/annotations/flink-disk-operator.nadberezny.com~1keda-last-resize-reason",
                ops.get(2).get("path").asText());
        assertEquals("usage high", ops.get(2).get("value").asText());

        assertEquals("/metadata/annotations/flink-disk-operator.nadberezny.com~1keda-last-resize-at",
                ops.get(3).get("path").asText());
        assertEquals("2026-09-22T10:00:00Z", ops.get(3).get("value").asText());
    }

    @Test
    void createsTheAnnotationsMapWhenTheResourceHasNone() throws Exception {
        FlinkDeployment fd = deployment("1", null, ephemeral("local-storage", "1Gi"));

        JsonNode ops = MAPPER.readTree(SpecPatch.build(fd, 0, "2Gi", "r", NOW));

        assertEquals(5, ops.size(), ops.toString());
        assertEquals("add", ops.get(2).get("op").asText());
        assertEquals("/metadata/annotations", ops.get(2).get("path").asText());
        assertTrue(ops.get(2).get("value").isObject() && ops.get(2).get("value").isEmpty());
    }

    @Test
    void refusesToPatchWithoutAResourceVersion() {
        FlinkDeployment fd = deployment(null, Map.of(), ephemeral("local-storage", "1Gi"));

        assertThrows(IllegalArgumentException.class, () -> SpecPatch.build(fd, 0, "2Gi", "r", NOW));
    }

    @Test
    void escapesJsonPointerTokens() {
        assertEquals("a~1b~0c", SpecPatch.escapePointerToken("a/b~c"));
    }
}
