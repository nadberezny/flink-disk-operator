package com.nadberezny.flink.disk.mutator;

import com.nadberezny.flink.disk.Names;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.mutator.FlinkResourceMutator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

/**
 * The write half of flink-disk-operator: applies the operator's desired TaskManager volume size at
 * FlinkDeployment admission time.
 *
 * <p>Loaded by the Flink operator's webhook through Flink's plugin mechanism — dropped into
 * {@code $FLINK_PLUGINS_DIR/flink-disk-mutator/} and discovered via {@link java.util.ServiceLoader}
 * by {@code MutatorUtils.discoverMutators}.
 *
 * <p>Why a mutator rather than the operator patching the spec itself: the FlinkDeployment's
 * volumes section stays owned by whoever authored it (Helm, in this repo). The operator only ever
 * bumps an annotation, which is enough to make the API server run this webhook, and the size is
 * injected on the way through. A {@code helm upgrade} that re-applies the chart's original 1Gi
 * therefore still comes out the other side with the size the operator last decided — which is the
 * main reason the desired size lives in a ConfigMap rather than in an annotation on the resource.
 *
 * <p><b>Fails open.</b> The mutating webhook is configured {@code failurePolicy: Fail}, so an
 * exception here would block every FlinkDeployment create and update in the cluster. Losing a
 * resize is recoverable — the operator notices on its next poll and bumps the annotation again.
 * Blocking deploys is not.
 */
public class DiskSizeMutator implements FlinkResourceMutator {

    private static final Logger LOG = LoggerFactory.getLogger(DiskSizeMutator.class);

    private final DesiredSizeSource sizeSource;

    /** Required by {@link java.util.ServiceLoader}. */
    public DiskSizeMutator() {
        this(new ConfigMapDesiredSizeSource());
    }

    DiskSizeMutator(DesiredSizeSource sizeSource) {
        this.sizeSource = sizeSource;
    }

    @Override
    public FlinkDeployment mutateDeployment(FlinkDeployment deployment) {
        if (!isManaged(deployment)) {
            return deployment;
        }

        String namespace = deployment.getMetadata().getNamespace();
        String name = deployment.getMetadata().getName();
        String id = namespace + "/" + name;

        try {
            Optional<DesiredSize> desired = sizeSource.lookup(namespace, name);
            if (desired.isEmpty()) {
                LOG.debug("{}: no desired size recorded yet, leaving the spec alone", id);
                return deployment;
            }

            EphemeralVolumeSizer.Result result = EphemeralVolumeSizer.apply(deployment, desired.get());
            switch (result.outcome()) {
                case APPLIED -> LOG.info("{}: volume '{}' {} -> {}", id, desired.get().volumeName(),
                        result.previousBytes(), desired.get().quantity());
                case ALREADY_LARGE_ENOUGH -> LOG.debug("{}: volume '{}' already requests >= {}",
                        id, desired.get().volumeName(), desired.get().quantity());
                case VOLUME_NOT_FOUND -> LOG.warn(
                        "{}: state names volume '{}' but the TaskManager pod template has no such "
                                + "generic ephemeral volume; not resizing",
                        id, desired.get().volumeName());
                case NO_STORAGE_REQUEST -> LOG.warn(
                        "{}: volume '{}' declares no resources.requests.storage; not resizing",
                        id, desired.get().volumeName());
            }
        } catch (Exception e) {
            // See the class comment: never fail admission over a resize.
            LOG.error("{}: failed to apply desired disk size, admitting the resource unchanged", id, e);
        }
        return deployment;
    }

    @Override
    public FlinkSessionJob mutateSessionJob(FlinkSessionJob sessionJob, Optional<FlinkDeployment> session) {
        return sessionJob;
    }

    @Override
    public FlinkStateSnapshot mutateStateSnapshot(FlinkStateSnapshot stateSnapshot) {
        return stateSnapshot;
    }

    /**
     * Same opt-in label the operator's informer selects on. Checking it here avoids an API call
     * per admission for every FlinkDeployment in the cluster that we have nothing to do with.
     */
    private static boolean isManaged(FlinkDeployment deployment) {
        if (deployment == null || deployment.getMetadata() == null) {
            return false;
        }
        Map<String, String> labels = deployment.getMetadata().getLabels();
        return labels != null && "true".equals(labels.get(Names.MANAGED_LABEL));
    }
}
