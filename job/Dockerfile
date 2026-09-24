#   ./gradlew :job:jar
#   docker build -f job/Dockerfile -t flink-disk-job:dev .
FROM flink:1.20.1-scala_2.12-java17

RUN mkdir -p /opt/flink/plugins/s3-fs-presto \
    && cp /opt/flink/opt/flink-s3-fs-presto-1.20.1.jar /opt/flink/plugins/s3-fs-presto/ \
    && mkdir -p /opt/flink/plugins/s3-fs-hadoop \
    && cp /opt/flink/opt/flink-s3-fs-hadoop-1.20.1.jar /opt/flink/plugins/s3-fs-hadoop/

RUN mkdir -p /opt/flink/usrlib
COPY job/build/libs/flink-disk-job-0.1-SNAPSHOT.jar /opt/flink/usrlib/flink-job.jar
