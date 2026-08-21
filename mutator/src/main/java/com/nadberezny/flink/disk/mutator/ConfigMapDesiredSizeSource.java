package com.nadberezny.flink.disk.mutator;

import com.nadberezny.flink.disk.Names;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;

import java.util.Map;
import java.util.Optional;

/**
 * Reads the desired size from flink-disk-operator's state ConfigMap.
 *
 * <p>The client is built lazily on first use rather than in {@code configure()}. Plugin discovery
 * runs with the plugin's own child-first classloader as the thread context classloader, and
 * fabric8 picks its HTTP client implementation via {@link java.util.ServiceLoader} off that
 * classloader. By the time a mutation happens we are on a webhook request thread with the
 * webhook's own classloader in context, which is where fabric8's service files actually live.
 *
 * <p>No RBAC changes are needed for this: the webhook runs as a sidecar in the operator pod and
 * shares its ServiceAccount, which already has full ConfigMap access in the watched namespaces.
 */
public class ConfigMapDesiredSizeSource implements DesiredSizeSource {

    private volatile KubernetesClient client;

    @Override
    public Optional<DesiredSize> lookup(String namespace, String flinkDeploymentName) {
        ConfigMap configMap = client()
                .configMaps()
                .inNamespace(namespace)
                .withName(Names.STATE_CONFIGMAP_PREFIX + flinkDeploymentName)
                .get();

        if (configMap == null || configMap.getData() == null) {
            return Optional.empty();
        }
        Map<String, String> data = configMap.getData();
        String quantity = data.get(Names.STATE_DESIRED_SIZE);
        String volumeName = data.get(Names.STATE_VOLUME_NAME);
        if (quantity == null || quantity.isBlank() || volumeName == null || volumeName.isBlank()) {
            return Optional.empty();
        }

        long bytes = Quantity.getAmountInBytes(Quantity.parse(quantity)).longValueExact();
        return Optional.of(new DesiredSize(quantity, bytes, volumeName));
    }

    private KubernetesClient client() {
        KubernetesClient existing = client;
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            if (client == null) {
                client = new KubernetesClientBuilder().build();
            }
            return client;
        }
    }
}
