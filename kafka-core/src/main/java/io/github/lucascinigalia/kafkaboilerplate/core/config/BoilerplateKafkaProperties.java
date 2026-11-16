package io.github.lucascinigalia.kafkaboilerplate.core.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.util.unit.DataSize;
import org.springframework.validation.annotation.Validated;

/**
 * Every setting of the boilerplate. Invalid values stop the application at startup.
 *
 * @param bootstrapServers broker addresses, e.g. {@code localhost:9092}
 * @param source name of this application; goes into the envelope and into the Kafka {@code client.id}
 * @param topics topics owned by this application, by alias
 */
@Validated
@ConfigurationProperties("boilerplate.kafka")
public record BoilerplateKafkaProperties(
        @NotBlank String bootstrapServers,
        @NotBlank String source,
        @Valid @DefaultValue Producer producer,
        @Valid @DefaultValue Admin admin,
        @Valid @DefaultValue Map<String, Topic> topics) {

    /**
     * @param acks how many replicas must confirm a write
     * @param idempotence broker discards duplicates created by producer retries; requires {@code acks=all}
     * @param compression codec applied to each batch
     * @param linger how long to wait for more records before sending a batch
     * @param batchSize maximum size of a batch, per partition
     * @param deliveryTimeout total time to confirm a record, retries included
     * @param sendTimeout how long {@code publishAndWait} blocks
     * @param properties extra raw Kafka producer properties; they win over the ones above
     */
    public record Producer(
            @NotNull @DefaultValue("all") Acks acks,
            @DefaultValue("true") boolean idempotence,
            @NotNull @DefaultValue("lz4") Compression compression,
            @NotNull @DefaultValue("5ms") Duration linger,
            @NotNull @DefaultValue("16KB") DataSize batchSize,
            @NotNull @DefaultValue("2m") Duration deliveryTimeout,
            @NotNull @DefaultValue("10s") Duration sendTimeout,
            @DefaultValue Map<String, String> properties) {

        @AssertTrue(message = "idempotence=true requires acks=all; set idempotence=false explicitly to use weaker acks")
        public boolean isIdempotenceConsistentWithAcks() {
            return !idempotence || acks == Acks.ALL;
        }
    }

    /**
     * @param createTopics create the declared topics at startup
     * @param failFast stop the application if the broker cannot be reached at startup
     * @param operationTimeout timeout of admin operations
     */
    public record Admin(
            @DefaultValue("true") boolean createTopics,
            @DefaultValue("true") boolean failFast,
            @NotNull @DefaultValue("30s") Duration operationTimeout) {}

    /**
     * @param name real topic name in the broker
     * @param partitions number of partitions; no default, because changing it later moves keys to other partitions
     * @param replicas replication factor; no default, because it decides how many brokers can be lost
     * @param retention how long records are kept; broker default when absent
     * @param cleanupPolicy delete old records, or compact keeping the last record of each key
     * @param eventTypes event types published to this topic
     * @param config extra raw topic configs
     */
    public record Topic(
            @NotBlank String name,
            @NotNull @Min(1) Integer partitions,
            @NotNull @Min(1) Short replicas,
            Duration retention,
            @NotNull @DefaultValue("delete") CleanupPolicy cleanupPolicy,
            @DefaultValue Set<String> eventTypes,
            @DefaultValue Map<String, String> config) {}

    public enum Acks {
        /** Every in-sync replica confirms. No loss while one in-sync replica survives. */
        ALL("all"),
        /** Only the leader confirms. Records are lost if the leader dies before replicating. */
        LEADER("1"),
        /** No confirmation. Fastest, and records can be lost silently. */
        NONE("0");

        private final String kafkaValue;

        Acks(String kafkaValue) {
            this.kafkaValue = kafkaValue;
        }

        public String kafkaValue() {
            return kafkaValue;
        }
    }

    public enum Compression {
        NONE,
        GZIP,
        SNAPPY,
        LZ4,
        ZSTD;

        public String kafkaValue() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public enum CleanupPolicy {
        DELETE,
        COMPACT;

        public String kafkaValue() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
