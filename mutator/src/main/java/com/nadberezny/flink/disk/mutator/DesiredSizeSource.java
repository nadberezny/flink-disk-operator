package com.nadberezny.flink.disk.mutator;

import java.util.Optional;

/** Where the mutator gets its instructions. An interface so tests need no API server. */
public interface DesiredSizeSource {

    /**
     * @return the operator's decision for this FlinkDeployment, or empty if it has never made one
     *     (no state ConfigMap yet, or the deployment is not managed)
     * @throws Exception any lookup failure; the caller fails open
     */
    Optional<DesiredSize> lookup(String namespace, String flinkDeploymentName) throws Exception;
}
