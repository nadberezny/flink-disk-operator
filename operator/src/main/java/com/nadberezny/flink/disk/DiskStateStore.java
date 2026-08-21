package com.nadberezny.flink.disk;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;

import java.time.Clock;
import java.util.Map;

/**
 * Persists {@link DiskState} in one ConfigMap per FlinkDeployment.
 *
 * <p>A ConfigMap rather than the FlinkDeployment status because that status belongs to the Flink
 * operator; writing to it would mean two controllers fighting over one subresource. It is also the
 * hand-off point for the mutator plugin, which reads the desired size at admission time.
 */
public class DiskStateStore {

    private final KubernetesClient client;
    private final Clock clock;

    public DiskStateStore(KubernetesClient client, Clock clock) {
        this.client = client;
        this.clock = clock;
    }

    /**
     * Reads persisted state, or seeds it from the spec on first sight.
     *
     * <p>When the spec has been grown past what we remember (someone edited it by hand, or the
     * mutator applied a size from a previous operator lifetime), the spec wins — it is the deployed
     * reality, and the alternative would be trying to shrink it back.
     */
    public DiskState loadOrSeed(FlinkDeployment deployment, TaskManagerVolumes.EphemeralVolume volume) {
        DiskState seed = DiskState.initial(volume.requestedSizeBytes(), volume.name());
        ConfigMap existing = configMaps(deployment).get();
        if (existing == null) {
            return seed;
        }
        DiskState stored = DiskState.fromConfigMapData(existing.getData(), seed);
        if (volume.requestedSizeBytes() > stored.desiredSizeBytes()) {
            return new DiskState(volume.requestedSizeBytes(), volume.name(),
                    stored.resizeGeneration(),
                    "adopted larger size from FlinkDeployment spec", false);
        }
        return stored;
    }

    /** Creates or replaces the state ConfigMap. Owned by the FlinkDeployment, so it is GC'd with it. */
    public void save(FlinkDeployment deployment, DiskState state, DiskSizePolicy.Decision decision) {
        Map<String, String> data = state.toConfigMapData(decision, clock.instant().toString());

        ConfigMap configMap = new ConfigMapBuilder()
                .withNewMetadata()
                .withName(Names.stateConfigMapName(deployment.getMetadata().getName()))
                .withNamespace(deployment.getMetadata().getNamespace())
                .withLabels(Map.of(
                        "app.kubernetes.io/managed-by", "flink-disk-operator",
                        Names.FLINK_DEPLOYMENT_LABEL, deployment.getMetadata().getName()))
                .withOwnerReferences(new OwnerReferenceBuilder()
                        .withApiVersion(deployment.getApiVersion())
                        .withKind(deployment.getKind())
                        .withName(deployment.getMetadata().getName())
                        .withUid(deployment.getMetadata().getUid())
                        .withController(false)
                        .withBlockOwnerDeletion(false)
                        .build())
                .endMetadata()
                .withData(data)
                .build();

        // Server-side apply: idempotent create-or-update with no read first. Forcing conflicts is
        // safe because this operator is the only writer of these ConfigMaps, and it turns a
        // hand-edited field into "operator wins" rather than a stuck reconcile.
        client.configMaps()
                .inNamespace(deployment.getMetadata().getNamespace())
                .resource(configMap)
                .forceConflicts()
                .serverSideApply();
    }

    private io.fabric8.kubernetes.client.dsl.Resource<ConfigMap> configMaps(FlinkDeployment deployment) {
        return client.configMaps()
                .inNamespace(deployment.getMetadata().getNamespace())
                .withName(Names.stateConfigMapName(deployment.getMetadata().getName()));
    }
}
