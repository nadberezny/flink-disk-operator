#   ./gradlew :mock-prometheus:jar
#   docker build -f mock-prometheus/Dockerfile -t mock-prometheus:dev .
FROM eclipse-temurin:21-jre

WORKDIR /app

COPY mock-prometheus/build/libs/mock-prometheus-0.1-SNAPSHOT.jar /app/mock-prometheus.jar

EXPOSE 9090

ENTRYPOINT ["java", "-jar", "/app/mock-prometheus.jar"]
