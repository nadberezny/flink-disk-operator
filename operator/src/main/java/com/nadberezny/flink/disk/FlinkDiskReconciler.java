package com.nadberezny.flink.disk;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.fabric8.kubernetes.client.dsl.base.PatchType;
import io.javaoperatorsdk.operator.api.config.informer.Informer;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Keeps a FlinkDeployment's TaskManager ephemeral volume big enough.
 *
 * <p>Each reconcile:
 * <ol>
 *   <li>read the current volume name and size out of the spec;</li>
 *   <li>load the remembered desired size from this deployment's state ConfigMap;</li>
 *   <li>ask Prometheus how full the TaskManager volumes are;</li>
 *   <li>persist the new desired size, and — only if it changed — bump an annotation on the
 *       FlinkDeployment.</li>
 * </ol>
 *
 * <p>That annotation bump is the whole trick. This operator never writes the volumes section: the
 * bump makes the API server issue an UPDATE, the Flink operator's mutating webhook runs, and the
 * CustomFlinkMutator plugin copies the desired size out of the ConfigMap into the spec. Until that
 * plugin is deployed (iteration 3) the decision is recorded and visible but nothing resizes.
 *
 * <p>Reconciles are self-scheduled every {@code POLL_INTERVAL_SECONDS}, because disk usage changes
 * without anything happening to the FlinkDeployment. Generation-aware event processing means our
 * own annotation patch does not re-trigger us; the mutator's spec change does, and that pass is a
 * no-op because spec size then equals the desired size.
 */
@ControllerConfiguration(
        name = "flink-disk-operator",
        informer = @Informer(labelSelector = Names.MANAGED_LABEL + "=true"))
public class FlinkDiskReconciler implements Reconciler<FlinkDeployment> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkDiskReconciler.class);

    private final OperatorConfig config;
    private final PrometheusClient prometheus;
    private final DiskSizePolicy policy;
    private final Clock clock;
    private final ObjectMapper mapper = new ObjectMapper();

    public FlinkDiskReconciler(OperatorConfig config, PrometheusClient prometheus, Clock clock) {
        this.config = config;
        this.prometheus = prometheus;
        this.policy = new DiskSizePolicy(config);
        this.clock = clock;
    }

    @Override
    public UpdateControl<FlinkDeployment> reconcile(FlinkDeployment deployment,
                                                   Context<FlinkDeployment> context) {
        String id = deployment.getMetadata().getNamespace() + "/" + deployment.getMetadata().getName();

        Optional<TaskManagerVolumes.EphemeralVolume> found =
                TaskManagerVolumes.findEphemeralVolume(deployment);
        if (found.isEmpty()) {
            LOG.warn("{}: no generic ephemeral volume on the TaskManager pod template; "
                    + "nothing to manage. Remove the {} label or add a volume.", id, Names.MANAGED_LABEL);
            return reschedule();
        }
        long candidates = TaskManagerVolumes.countEphemeralVolumes(deployment);
        if (candidates > 1) {
            LOG.warn("{}: {} generic ephemeral volumes on the TaskManager; managing the first ('{}')",
                    id, candidates, found.get().name());
        }

        TaskManagerVolumes.EphemeralVolume volume = found.get();
        DiskStateStore store = new DiskStateStore(context.getClient(), clock);
        DiskState current = store.loadOrSeed(deployment, volume);

        List<VolumeUsage> usages;
        try {
            usages = prometheus.taskManagerVolumeUsage(
                    deployment.getMetadata().getNamespace(),
                    deployment.getMetadata().getName(),
                    volume.name());
        } catch (IOException e) {
            // A flaky Prometheus should slow the loop down, not crash it.
            LOG.warn("{}: Prometheus query failed, retrying in {}s: {}",
                    id, config.pollInterval().toSeconds(), e.getMessage());
            return reschedule();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("{}: interrupted while querying Prometheus", id);
            return reschedule();
        }

        DiskSizePolicy.Decision decision = policy.decide(usages, current.desiredSizeBytes());
        DiskState next = current.applying(decision);

        // Written every pass, not just on change: it makes the ConfigMap a live view of what the
        // operator sees, which is worth one small write per poll per deployment.
        store.save(deployment, next, decision);

        if (decision.changed()) {
            LOG.info("{}: {} (resizeGeneration {} -> {})",
                    id, decision.reason(), current.resizeGeneration(), next.resizeGeneration());
            triggerAdmission(context.getClient(), deployment, next);
        } else if (next.atMaxSize() && !current.atMaxSize()) {
            LOG.warn("{}: {}", id, decision.reason());
        } else {
            LOG.debug("{}: {} (observed {} volume(s))", id, decision.reason(), usages.size());
        }

        return reschedule();
    }

    /**
     * Metadata-only JSON merge patch. Deliberately not {@code UpdateControl.patchResource}: that
     * would round-trip the whole resource through our (partial) view of it, and we only ever want
     * to touch these two annotations.
     */
    private void triggerAdmission(KubernetesClient client, FlinkDeployment deployment, DiskState next) {
        Map<String, Object> patch = Map.of("metadata", Map.of("annotations", Map.of(
                Names.RESIZE_GENERATION_ANNOTATION, Long.toString(next.resizeGeneration()),
                Names.DESIRED_SIZE_ANNOTATION, Quantities.format(next.desiredSizeBytes()))));
        try {
            client.resources(FlinkDeployment.class)
                    .inNamespace(deployment.getMetadata().getNamespace())
                    .withName(deployment.getMetadata().getName())
                    .patch(PatchContext.of(PatchType.JSON_MERGE), mapper.writeValueAsString(patch));
        } catch (Exception e) {
            // State is already persisted, so the next poll will retry the trigger.
            LOG.warn("{}/{}: failed to bump {}; will retry next poll: {}",
                    deployment.getMetadata().getNamespace(), deployment.getMetadata().getName(),
                    Names.RESIZE_GENERATION_ANNOTATION, e.getMessage());
        }
    }

    private UpdateControl<FlinkDeployment> reschedule() {
        return UpdateControl.<FlinkDeployment>noUpdate().rescheduleAfter(config.pollInterval());
    }
}
