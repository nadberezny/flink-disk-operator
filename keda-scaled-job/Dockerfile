# Build with the repo root as context:
#   ./gradlew :keda-scaled-job:installDist
#   docker build -f keda-scaled-job/Dockerfile -t flink-disk-resizer:dev .
FROM eclipse-temurin:21-jre

WORKDIR /app

# The application plugin's install layout: bin/ launcher + lib/ with every jar.
COPY keda-scaled-job/build/install/flink-disk-resizer /app

USER 1000

ENTRYPOINT ["/app/bin/flink-disk-resizer"]
