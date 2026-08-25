package com.nadberezny.flink.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deliberately boring streaming job: it emits a slow heartbeat stream and throws it away.
 *
 * <p>Its only purpose is to give the flink-disk-operator a long-running FlinkDeployment with
 * TaskManagers that own generic ephemeral volumes. Disk pressure itself is faked by the
 * mock-prometheus service, so this job intentionally does not write to disk.
 */
public final class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private static final String ENV_RECORDS_PER_SECOND = "RECORDS_PER_SECOND";
    private static final String ENV_JOB_NAME = "JOB_NAME";

    private static final double DEFAULT_RECORDS_PER_SECOND = 1.0;
    private static final String DEFAULT_JOB_NAME = "flink-disk-job";
    private static final long HEARTBEAT_LOG_EVERY = 60L;

    private Main() {}

    public static void main(String[] args) throws Exception {
        double recordsPerSecond = doubleFromEnv(ENV_RECORDS_PER_SECOND, DEFAULT_RECORDS_PER_SECOND);
        String jobName = stringFromEnv(ENV_JOB_NAME, DEFAULT_JOB_NAME);

        LOG.info("Starting {} at {} records/s", jobName, recordsPerSecond);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Unbounded for all practical purposes: Long.MAX_VALUE records at ~1/s.
        DataGeneratorSource<Long> source = new DataGeneratorSource<>(
                (GeneratorFunction<Long, Long>) index -> index,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(recordsPerSecond),
                Types.LONG);

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "heartbeat-source")
                .map(new HeartbeatLogger())
                .name("heartbeat-logger")
                .sinkTo(new DiscardingSink<>())
                .name("discarding-sink");

        env.execute(jobName);
    }

    /**
     * Logs every {@value #HEARTBEAT_LOG_EVERY}-th record so "is it alive?" is answerable from
     * the TaskManager log. Counting locally rather than keying off the generated value keeps the
     * numbers readable: DataGeneratorSource hands each subtask a slice of the Long range, so the
     * values themselves are arbitrary 19-digit offsets.
     */
    private static final class HeartbeatLogger
            implements org.apache.flink.api.common.functions.MapFunction<Long, Long> {

        private static final long serialVersionUID = 1L;

        private transient long seen;

        @Override
        public Long map(Long value) {
            if (seen++ % HEARTBEAT_LOG_EVERY == 0) {
                LOG.info("heartbeat: {} records seen by this subtask", seen);
            }
            return value;
        }
    }

    private static String stringFromEnv(String name, String defaultValue) {
        String raw = System.getenv(name);
        return raw == null || raw.isBlank() ? defaultValue : raw.trim();
    }

    private static double doubleFromEnv(String name, double defaultValue) {
        String raw = System.getenv(name);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(raw.trim());
        } catch (NumberFormatException e) {
            LOG.warn("Ignoring unparsable {}={}, falling back to {}", name, raw, defaultValue);
            return defaultValue;
        }
    }
}
