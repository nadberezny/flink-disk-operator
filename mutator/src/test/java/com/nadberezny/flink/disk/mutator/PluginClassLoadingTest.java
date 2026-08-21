package com.nadberezny.flink.disk.mutator;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.plugin.PluginConfig;
import org.apache.flink.core.plugin.PluginDescriptor;
import org.apache.flink.core.plugin.PluginLoader;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.mutator.FlinkResourceMutator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Loads the built jar exactly the way the webhook does, through Flink's plugin classloader with
 * the parent-first patterns that the deployed flink-conf.yaml actually produces.
 *
 * <p>These are the failures unit tests cannot see: a missing {@code META-INF/services} entry, a
 * jar that bundles fabric8 (giving the plugin its own incompatible {@link FlinkDeployment}), or a
 * classloader config that leaves a dependency invisible at runtime.
 */
@EnabledIfSystemProperty(named = "mutator.jar", matches = ".+")
class PluginClassLoadingTest {

    /** Same file k8s-infra-dev/flink_operator.tf appends to the operator's flink-conf.yaml. */
    private static final Path CONF_OVERRIDES = Path.of("flink-conf-overrides.yaml");

    private static File jar() {
        File jar = new File(System.getProperty("mutator.jar"));
        assertTrue(jar.isFile(), "plugin jar not built: " + jar);
        return jar;
    }

    /** Parses the legacy {@code key: value} overrides file into a Flink {@link Configuration}. */
    private static Configuration deployedConfiguration() throws Exception {
        assertTrue(Files.isRegularFile(CONF_OVERRIDES),
                "expected " + CONF_OVERRIDES.toAbsolutePath() + " (test workingDir must be the module root)");
        Configuration conf = new Configuration();
        for (String line : Files.readAllLines(CONF_OVERRIDES)) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                continue;
            }
            int colon = trimmed.indexOf(':');
            conf.setString(trimmed.substring(0, colon).trim(), trimmed.substring(colon + 1).trim());
        }
        return conf;
    }

    private static String[] deployedParentFirstPatterns() throws Exception {
        return PluginConfig.fromConfiguration(deployedConfiguration()).getAlwaysParentFirstPatterns();
    }

    private PluginLoader loader(String[] parentFirstPatterns) throws Exception {
        PluginDescriptor descriptor = new PluginDescriptor(
                System.getProperty("mutator.pluginDirName", "flink-disk-mutator"),
                new URL[] {jar().toURI().toURL()},
                new String[0]);
        return PluginLoader.create(descriptor, getClass().getClassLoader(), parentFirstPatterns);
    }

    @Test
    void confOverridesProduceTheParentFirstPatternsThePluginNeeds() throws Exception {
        List<String> patterns = Arrays.asList(deployedParentFirstPatterns());

        // Flink's own defaults, which we must not lose by setting the "additional" option.
        assertTrue(patterns.contains("org.apache.flink."), patterns.toString());
        assertTrue(patterns.contains("java."), patterns.toString());
        // What flink-conf-overrides.yaml adds.
        assertTrue(patterns.contains("io.fabric8."), patterns.toString());
        assertTrue(patterns.contains("org.slf4j."), patterns.toString());
    }

    @Test
    void serviceLoaderFindsTheMutatorUnderTheDeployedPatterns() throws Exception {
        try (PluginLoader loader = loader(deployedParentFirstPatterns())) {
            List<FlinkResourceMutator> discovered = new ArrayList<>();
            loader.load(FlinkResourceMutator.class).forEachRemaining(discovered::add);

            assertEquals(1, discovered.size(), "expected exactly one mutator from the plugin jar");
            assertEquals(DiskSizeMutator.class.getName(), discovered.get(0).getClass().getName());
        }
    }

    @Test
    void pluginGetsItsOwnClassesButSharesTheFlinkApi() throws Exception {
        try (PluginLoader loader = loader(deployedParentFirstPatterns())) {
            FlinkResourceMutator mutator = loader.load(FlinkResourceMutator.class).next();

            // Our class comes from the plugin jar, so it is a different Class object...
            assertNotSame(DiskSizeMutator.class, mutator.getClass());
            // ...but the SPI is the parent's, or the webhook could not invoke us at all.
            assertSame(FlinkResourceMutator.class,
                    findInterface(mutator.getClass(), FlinkResourceMutator.class.getName()));
        }
    }

    @Test
    void pluginCanBeInvokedAcrossTheClassloaderBoundary() throws Exception {
        try (PluginLoader loader = loader(deployedParentFirstPatterns())) {
            FlinkResourceMutator mutator = loader.load(FlinkResourceMutator.class).next();

            // A FlinkDeployment built here, by the parent classloader, handed to the plugin's own
            // copy of the mutator. This call would fail with NoClassDefFoundError or
            // ClassCastException if the jar bundled fabric8 or the parent-first patterns were
            // wrong - which is the whole point of the assertion.
            //
            // Deliberately *unmanaged*: that short-circuits on the label before any Kubernetes
            // client is built, so the test never depends on a reachable cluster or on what a real
            // state ConfigMap happens to say. The managed path is covered by DiskSizeMutatorTest,
            // and jarDoesNotBundleAnythingTheWebhookAlreadyProvides guarantees the fabric8 types
            // it exercises are the same classes the webhook would pass in.
            FlinkDeployment deployment = DiskSizeMutatorTest.deployment(
                    false, List.of(DiskSizeMutatorTest.ephemeral("local-storage", "1Gi")));

            FlinkDeployment returned = mutator.mutateDeployment(deployment);

            assertSame(deployment, returned);
            assertEquals("1Gi", storage(returned));
        }
    }

    @Test
    void jarDoesNotBundleAnythingTheWebhookAlreadyProvides() throws Exception {
        List<String> leaked = new ArrayList<>();
        try (ZipFile zip = new ZipFile(jar())) {
            zip.stream()
                    .map(ZipEntry::getName)
                    .filter(name -> name.endsWith(".class"))
                    .filter(name -> name.startsWith("io/fabric8/")
                            || name.startsWith("com/fasterxml/")
                            || name.startsWith("org/apache/flink/")
                            || name.startsWith("org/slf4j/"))
                    .forEach(leaked::add);
        }
        assertTrue(leaked.isEmpty(),
                "plugin jar must stay thin; a second copy of these would break type identity "
                        + "with the webhook: " + leaked);
    }

    /**
     * The flink-kubernetes-operator image ships a Java 17 JRE, so every class that travels inside
     * the plugin jar must be class file version 61 or lower — including the bundled
     * {@code :contract} classes.
     *
     * <p>Getting this wrong is silent at build time and brutal at runtime: the webhook throws
     * {@code UnsupportedClassVersionError} while discovering mutators, {@code main} dies, and
     * because the container stays up with nothing listening on 9443 every FlinkDeployment apply
     * fails with a 502 from the admission webhook.
     */
    @Test
    void everyBundledClassRunsOnTheWebhooksJava17Runtime() throws Exception {
        int java17 = 61;
        List<String> tooNew = new ArrayList<>();
        try (ZipFile zip = new ZipFile(jar())) {
            for (ZipEntry entry : zip.stream().filter(e -> e.getName().endsWith(".class")).toList()) {
                try (var in = zip.getInputStream(entry)) {
                    byte[] header = in.readNBytes(8);
                    // CAFEBABE, minor (2 bytes), major (2 bytes)
                    int major = ((header[6] & 0xFF) << 8) | (header[7] & 0xFF);
                    if (major > java17) {
                        tooNew.add(entry.getName() + " (class file " + major + ")");
                    }
                }
            }
        }
        assertTrue(tooNew.isEmpty(),
                "these would fail to load in the webhook's Java 17 JRE: " + tooNew);
    }

    @Test
    void jarBundlesTheSharedContractAndServiceDescriptor() throws Exception {
        List<String> entries = new ArrayList<>();
        try (ZipFile zip = new ZipFile(jar())) {
            zip.stream().map(ZipEntry::getName).forEach(entries::add);
        }

        // :contract is not on the webhook's classpath, so it has to travel inside the jar. It is
        // dependency-free, so bundling it cannot shadow anything.
        assertTrue(entries.contains("com/nadberezny/flink/disk/Names.class"),
                "expected :contract to be folded in, got: " + entries);
        assertTrue(entries.contains(
                        "META-INF/services/org.apache.flink.kubernetes.operator.mutator.FlinkResourceMutator"),
                "expected the ServiceLoader descriptor, got: " + entries);
    }

    private static String storage(FlinkDeployment deployment) {
        return deployment.getSpec().getTaskManager().getPodTemplate().getSpec().getVolumes()
                .get(0).getEphemeral().getVolumeClaimTemplate().getSpec()
                .getResources().getRequests().get("storage").toString();
    }

    private static Class<?> findInterface(Class<?> type, String name) {
        for (Class<?> candidate : type.getInterfaces()) {
            if (candidate.getName().equals(name)) {
                return candidate;
            }
        }
        throw new AssertionError(type + " does not implement " + name);
    }
}
