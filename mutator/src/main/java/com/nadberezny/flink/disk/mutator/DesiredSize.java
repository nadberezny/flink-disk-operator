package com.nadberezny.flink.disk.mutator;

/**
 * What flink-disk-operator decided, as read back from the state ConfigMap.
 *
 * @param quantity the size verbatim from the ConfigMap (e.g. {@code 2Gi}), written into the spec
 *     as-is so the rendered resource stays readable
 * @param bytes the same value resolved to bytes, for comparing against what the spec already has
 * @param volumeName which TaskManager volume the operator is managing
 */
public record DesiredSize(String quantity, long bytes, String volumeName) {}
