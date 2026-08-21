package com.nadberezny.flink.disk;

import io.javaoperatorsdk.operator.Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;

/** Entry point for flink-disk-operator. */
public final class Runner {

    private static final Logger LOG = LoggerFactory.getLogger(Runner.class);

    private Runner() {}

    public static void main(String[] args) throws Exception {
        OperatorConfig config = OperatorConfig.fromEnv();
        LOG.info("Starting flink-disk-operator: {}", config.describe());
        LOG.info("Managing FlinkDeployments labelled {}=true", Names.MANAGED_LABEL);

        // The termination timeout replaces the deprecated installShutdownHook(Duration): it is how
        // long a running reconcile gets to finish once stop() is called.
        Operator operator = new Operator(overrider ->
                overrider.withReconciliationTerminationTimeout(Duration.ofSeconds(10)));
        operator.register(
                new FlinkDiskReconciler(config, new PrometheusClient(config), Clock.systemUTC()),
                overrider -> {
                    if (!config.watchNamespaces().isEmpty()) {
                        overrider.settingNamespaces(config.watchNamespaces());
                    }
                });

        HealthServer health = new HealthServer(
                Integer.parseInt(System.getenv().getOrDefault("HEALTH_PORT", "8080")), operator);

        Runtime.getRuntime().addShutdownHook(new Thread(health::stop));
        operator.installShutdownHook();
        operator.start();
        health.start();

        LOG.info("flink-disk-operator started");
    }
}
