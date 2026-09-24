package com.nadberezny.flink.disk;

public final class Names {

    public static final String DOMAIN = "flink-disk-operator.nadberezny.com";

    public static final String MANAGED_LABEL = DOMAIN + "/managed";

    public static final String RESIZE_GENERATION_ANNOTATION = DOMAIN + "/resize-generation";

    public static final String DESIRED_SIZE_ANNOTATION = DOMAIN + "/desired-size";

    public static final String FLINK_DEPLOYMENT_LABEL = DOMAIN + "/flink-deployment";

    /** Stamped by the KEDA-launched resizer (keda-scaled-job) on the FlinkDeployment it patched. */
    public static final String KEDA_LAST_RESIZE_REASON_ANNOTATION = DOMAIN + "/keda-last-resize-reason";
    public static final String KEDA_LAST_RESIZE_AT_ANNOTATION = DOMAIN + "/keda-last-resize-at";

    public static final String STATE_DESIRED_SIZE = "desiredSize";
    public static final String STATE_VOLUME_NAME = "volumeName";
    public static final String STATE_RESIZE_GENERATION = "resizeGeneration";
    public static final String STATE_LAST_REASON = "lastDecisionReason";
    public static final String STATE_OBSERVED_PVC = "lastObservedPvc";
    public static final String STATE_OBSERVED_USED = "lastObservedUsed";
    public static final String STATE_OBSERVED_CAPACITY = "lastObservedCapacity";
    public static final String STATE_OBSERVED_RATIO = "lastObservedRatio";
    public static final String STATE_UPDATED_AT = "updatedAt";

    public static final String STATE_CONFIGMAP_PREFIX = "flink-disk-state-";

    private Names() {}

    public static String stateConfigMapName(String flinkDeploymentName) {
        return STATE_CONFIGMAP_PREFIX + flinkDeploymentName;
    }
}
