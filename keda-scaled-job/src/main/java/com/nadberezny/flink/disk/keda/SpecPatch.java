package com.nadberezny.flink.disk.keda;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nadberezny.flink.disk.Names;
import io.fabric8.kubernetes.api.model.Volume;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Builds the RFC 6902 JSON Patch that grows a FlinkDeployment's TaskManager ephemeral volume.
 *
 * <p>One patch, one API call, one generation bump:
 * <ol>
 *   <li>{@code test} on {@code metadata.resourceVersion} - an exact optimistic lock. If anything
 *       touched the resource between our GET and this PATCH the API server answers 422, the Job
 *       fails, and KEDA's {@code backoffLimit} re-runs it from a fresh GET.</li>
 *   <li>{@code replace} of the volume claim template's storage request.</li>
 *   <li>{@code add} of two annotations recording why and when, for {@code kubectl describe}.</li>
 * </ol>
 *
 * <p>Pure function of its inputs so it is testable without a cluster.
 */
public final class SpecPatch {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private SpecPatch() {}

    /**
     * Index of {@code volumeName} in {@code spec.taskManager.podTemplate.spec.volumes}, the array
     * position the JSON pointer needs.
     */
    public static Optional<Integer> volumeIndex(FlinkDeployment deployment, String volumeName) {
        List<Volume> volumes = Optional.ofNullable(deployment.getSpec())
                .map(s -> s.getTaskManager())
                .map(t -> t.getPodTemplate())
                .map(p -> p.getSpec())
                .map(p -> p.getVolumes())
                .orElse(List.of());
        for (int i = 0; i < volumes.size(); i++) {
            if (volumeName.equals(volumes.get(i).getName())) {
                return Optional.of(i);
            }
        }
        return Optional.empty();
    }

    public static String storagePath(int volumeIndex) {
        return "/spec/taskManager/podTemplate/spec/volumes/" + volumeIndex
                + "/ephemeral/volumeClaimTemplate/spec/resources/requests/storage";
    }

    /**
     * @param deployment the object as fetched; supplies resourceVersion and whether annotations exist
     * @param volumeIndex position of the ephemeral volume in the TaskManager pod template
     * @param newSize Kubernetes quantity to write, e.g. {@code 2Gi}
     * @param reason human-readable decision, recorded as an annotation
     * @param now timestamp recorded as an annotation
     * @return the JSON Patch document
     */
    public static String build(FlinkDeployment deployment, int volumeIndex, String newSize,
                               String reason, Instant now) {
        String resourceVersion = deployment.getMetadata().getResourceVersion();
        if (resourceVersion == null || resourceVersion.isBlank()) {
            throw new IllegalArgumentException("fetched FlinkDeployment has no resourceVersion");
        }

        ArrayNode ops = MAPPER.createArrayNode();
        ops.add(op("test", "/metadata/resourceVersion", MAPPER.getNodeFactory().textNode(resourceVersion)));
        ops.add(op("replace", storagePath(volumeIndex), MAPPER.getNodeFactory().textNode(newSize)));

        if (deployment.getMetadata().getAnnotations() == null) {
            // `add` into a missing map is an error under RFC 6902; create it first.
            ops.add(op("add", "/metadata/annotations", MAPPER.createObjectNode()));
        }
        ops.add(op("add", annotationPath(Names.KEDA_LAST_RESIZE_REASON_ANNOTATION),
                MAPPER.getNodeFactory().textNode(reason)));
        ops.add(op("add", annotationPath(Names.KEDA_LAST_RESIZE_AT_ANNOTATION),
                MAPPER.getNodeFactory().textNode(now.toString())));

        return ops.toString();
    }

    static String annotationPath(String key) {
        return "/metadata/annotations/" + escapePointerToken(key);
    }

    /** RFC 6901: {@code ~} becomes {@code ~0} and {@code /} becomes {@code ~1}, in that order. */
    static String escapePointerToken(String token) {
        return token.replace("~", "~0").replace("/", "~1");
    }

    private static ObjectNode op(String op, String path, JsonNode value) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("op", op);
        node.put("path", path);
        node.set("value", value);
        return node;
    }
}
