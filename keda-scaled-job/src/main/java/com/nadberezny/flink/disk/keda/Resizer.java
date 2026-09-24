package com.nadberezny.flink.disk.keda;

import com.nadberezny.flink.disk.DiskSizePolicy;
import com.nadberezny.flink.disk.PrometheusClient;
import com.nadberezny.flink.disk.Quantities;
import com.nadberezny.flink.disk.TaskManagerVolumes;
import com.nadberezny.flink.disk.VolumeUsage;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.fabric8.kubernetes.client.dsl.base.PatchType;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.List;
import java.util.Optional;
public final class Resizer {

    private static final Logger LOG = LoggerFactory.getLogger(Resizer.class);

    static final int EXIT_OK = 0;
    static final int EXIT_ERROR = 1;

    private final ResizerConfig config;
    private final KubernetesClient client;
    private final PrometheusClient prometheus;
    private final DiskSizePolicy policy;
    private final Clock clock;

    Resizer(ResizerConfig config, KubernetesClient client, PrometheusClient prometheus, Clock clock) {
        this.config = config;
        this.client = client;
        this.prometheus = prometheus;
        this.policy = new DiskSizePolicy(config.sizing());
        this.clock = clock;
    }

    public static void main(String[] args) {
        int exitCode;
        try {
            ResizerConfig config = ResizerConfig.fromEnv(System.getenv());
            LOG.info("flink-disk-resizer starting: {}", config.describe());
            try (KubernetesClient client = new KubernetesClientBuilder().build()) {
                PrometheusClient prometheus =
                        new PrometheusClient(config.prometheusUrl(), config.prometheusTimeout());
                exitCode = new Resizer(config, client, prometheus, Clock.systemUTC()).runOnce();
            }
        } catch (Exception e) {
            LOG.error("flink-disk-resizer failed", e);
            exitCode = EXIT_ERROR;
        }
        System.exit(exitCode);
    }

    /** @return process exit code: 0 for "done or nothing to do", 1 for "failed, let the Job retry" */
    int runOnce() throws Exception {
        String namespace = config.namespace();
        String name = config.deploymentName();

        FlinkDeployment deployment = client.resources(FlinkDeployment.class)
                .inNamespace(namespace).withName(name).get();
        if (deployment == null) {
            LOG.error("FlinkDeployment {}/{} not found", namespace, name);
            return EXIT_ERROR;
        }

        Optional<TaskManagerVolumes.EphemeralVolume> volume =
                TaskManagerVolumes.findEphemeralVolume(deployment);
        if (volume.isEmpty()) {
            LOG.warn("{}/{} has no generic ephemeral volume on its TaskManager pod template; nothing to resize",
                    namespace, name);
            return EXIT_OK;
        }
        if (TaskManagerVolumes.countEphemeralVolumes(deployment) > 1) {
            LOG.warn("{}/{} declares several ephemeral TaskManager volumes; managing only '{}'",
                    namespace, name, volume.get().name());
        }
        long currentBytes = volume.get().requestedSizeBytes();

        List<VolumeUsage> usages = prometheus.taskManagerVolumeUsage(namespace, name, volume.get().name());
        LOG.info("{}/{} volume '{}' is {} in the spec; Prometheus reports {}",
                namespace, name, volume.get().name(), Quantities.format(currentBytes), usages);

        DiskSizePolicy.Decision decision = policy.decide(usages, currentBytes);
        if (!decision.changed()) {
            if (decision.atMaxSize()) {
                LOG.warn("{}/{}: {}", namespace, name, decision.reason());
            } else {
                LOG.info("{}/{}: no resize - {}", namespace, name, decision.reason());
            }
            return EXIT_OK;
        }

        int index = SpecPatch.volumeIndex(deployment, volume.get().name())
                .orElseThrow(() -> new IllegalStateException(
                        "volume '" + volume.get().name() + "' vanished from the pod template"));
        String newSize = Quantities.format(decision.desiredSizeBytes());
        String patch = SpecPatch.build(deployment, index, newSize, decision.reason(), clock.instant());

        if (config.dryRun()) {
            LOG.info("DRY_RUN: would patch {}/{} with {}", namespace, name, patch);
            return EXIT_OK;
        }

        LOG.info("{}/{}: {}", namespace, name, decision.reason());
        client.resources(FlinkDeployment.class).inNamespace(namespace).withName(name)
                .patch(PatchContext.of(PatchType.JSON), patch);
        LOG.info("{}/{}: patched TaskManager volume '{}' to {} (resourceVersion was {})",
                namespace, name, volume.get().name(), newSize,
                deployment.getMetadata().getResourceVersion());
        return EXIT_OK;
    }
}
