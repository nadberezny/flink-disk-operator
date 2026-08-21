#   ./gradlew :mutator:jar
#   docker build -f mutator/Dockerfile -t flink-kubernetes-operator-disk:1.15.0 .
ARG FLINK_OPERATOR_TAG=79d730b
FROM ghcr.io/apache/flink-kubernetes-operator:${FLINK_OPERATOR_TAG}

COPY --chown=flink:flink mutator/build/libs/flink-disk-mutator-0.1.0-SNAPSHOT.jar \
     /opt/flink/plugins/flink-disk-mutator/flink-disk-mutator.jar
