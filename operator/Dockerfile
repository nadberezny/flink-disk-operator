# Build with the repo root as context:
#   ./gradlew :operator:installDist
#   docker build -f operator/Dockerfile -t flink-disk-operator:dev .
FROM eclipse-temurin:21-jre

WORKDIR /app

# The application plugin's install layout: bin/ launcher + lib/ with every jar.
COPY operator/build/install/flink-disk-operator /app

# Health/readiness probes (see HealthServer).
EXPOSE 8080

USER 1000

ENTRYPOINT ["/app/bin/flink-disk-operator"]
