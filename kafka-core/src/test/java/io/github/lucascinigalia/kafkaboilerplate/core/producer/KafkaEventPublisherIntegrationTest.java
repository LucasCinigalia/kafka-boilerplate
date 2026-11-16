package io.github.lucascinigalia.kafkaboilerplate.core.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import io.github.lucascinigalia.kafkaboilerplate.core.event.EventEnvelope;
import io.github.lucascinigalia.kafkaboilerplate.core.event.EventFactory;
import io.github.lucascinigalia.kafkaboilerplate.core.event.EventHeaders;
import io.github.lucascinigalia.kafkaboilerplate.core.event.EventPublisher;
import io.github.lucascinigalia.kafkaboilerplate.core.event.PublishResult;
import io.github.lucascinigalia.kafkaboilerplate.core.support.KafkaTestContainer;
import io.github.lucascinigalia.kafkaboilerplate.core.support.SamplePayload;
import io.github.lucascinigalia.kafkaboilerplate.core.support.TestApplication;
import io.github.lucascinigalia.kafkaboilerplate.core.support.TopicReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest(
        classes = TestApplication.class,
        properties = {
            "boilerplate.kafka.source=test-app",
            "boilerplate.kafka.topics.samples.name=samples.v1",
            "boilerplate.kafka.topics.samples.partitions=3",
            "boilerplate.kafka.topics.samples.replicas=1",
            "boilerplate.kafka.topics.samples.retention=1h",
            "boilerplate.kafka.topics.samples.event-types=SampleCreated,SampleApproved",
            "boilerplate.kafka.topics.ordered.name=ordered.v1",
            "boilerplate.kafka.topics.ordered.partitions=3",
            "boilerplate.kafka.topics.ordered.replicas=1",
            "boilerplate.kafka.topics.ordered.event-types=SampleMoved",
            "boilerplate.kafka.topics.state.name=state.v1",
            "boilerplate.kafka.topics.state.partitions=1",
            "boilerplate.kafka.topics.state.replicas=1",
            "boilerplate.kafka.topics.state.cleanup-policy=compact"
        })
class KafkaEventPublisherIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("boilerplate.kafka.bootstrap-servers", KafkaTestContainer::bootstrapServers);
    }

    @Autowired
    private EventPublisher publisher;

    @Autowired
    private EventFactory events;

    @Autowired
    private ProducerFactory<String, Object> producerFactory;

    @Autowired
    private JsonMapper jsonMapper;

    @Test
    void declaredTopicsAreCreatedWithTheConfiguredSettings() throws Exception {
        try (Admin admin = Admin.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaTestContainer.bootstrapServers()))) {
            Map<String, TopicDescription> topics = admin.describeTopics(List.of("samples.v1", "state.v1"))
                    .allTopicNames()
                    .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            ConfigResource samples = new ConfigResource(ConfigResource.Type.TOPIC, "samples.v1");
            ConfigResource state = new ConfigResource(ConfigResource.Type.TOPIC, "state.v1");
            Map<ConfigResource, Config> configs =
                    admin.describeConfigs(List.of(samples, state)).all().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);

            assertThat(topics.get("samples.v1").partitions()).hasSize(3);
            assertThat(topics.get("state.v1").partitions()).hasSize(1);
            assertThat(configs.get(samples).get(TopicConfig.RETENTION_MS_CONFIG).value()).isEqualTo("3600000");
            assertThat(configs.get(samples).get(TopicConfig.CLEANUP_POLICY_CONFIG).value()).isEqualTo("delete");
            assertThat(configs.get(state).get(TopicConfig.CLEANUP_POLICY_CONFIG).value()).isEqualTo("compact");
        }
    }

    @Test
    void producerIsIdempotentWithAcksAllByDefault() {
        Map<String, Object> config = producerFactory.getConfigurationProperties();

        assertThat(config)
                .containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)
                .containsEntry(ProducerConfig.ACKS_CONFIG, "all")
                .containsEntry(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
                .containsEntry(ProducerConfig.CLIENT_ID_CONFIG, "test-app");
    }

    @Test
    void publishAndWaitWritesTheEnvelopeAsJsonWithKeyAndHeaders() {
        EventEnvelope<SamplePayload> cause = events.create("SampleCreated", 1, new SamplePayload("first", 1));
        EventEnvelope<SamplePayload> event =
                events.createCausedBy(cause, "SampleApproved", 2, new SamplePayload("widget", 7));

        PublishResult result = publisher.publishAndWait(event, "key-json");

        assertThat(result.eventId()).isEqualTo(event.eventId());
        assertThat(result.topic()).isEqualTo("samples.v1");

        ConsumerRecord<String, String> record = readAll("samples.v1").stream()
                .filter(r -> r.partition() == result.partition() && r.offset() == result.offset())
                .findFirst()
                .orElseThrow();
        assertThat(record.key()).isEqualTo("key-json");
        assertThat(record.timestamp()).isEqualTo(event.occurredAt().toEpochMilli());

        JsonNode json = jsonMapper.readTree(record.value());
        assertThat(json.get("eventId").asString()).isEqualTo(event.eventId().toString());
        assertThat(json.get("type").asString()).isEqualTo("SampleApproved");
        assertThat(json.get("version").asInt()).isEqualTo(2);
        assertThat(json.get("occurredAt").asString()).isEqualTo(event.occurredAt().toString());
        assertThat(json.get("correlationId").asString()).isEqualTo(cause.correlationId());
        assertThat(json.get("causationId").asString()).isEqualTo(cause.eventId().toString());
        assertThat(json.get("source").asString()).isEqualTo("test-app");
        assertThat(json.get("payload").get("name").asString()).isEqualTo("widget");
        assertThat(json.get("payload").get("quantity").asInt()).isEqualTo(7);

        assertThat(header(record, EventHeaders.EVENT_ID)).isEqualTo(event.eventId().toString());
        assertThat(header(record, EventHeaders.EVENT_TYPE)).isEqualTo("SampleApproved");
        assertThat(header(record, EventHeaders.EVENT_VERSION)).isEqualTo("2");
        assertThat(header(record, EventHeaders.CORRELATION_ID)).isEqualTo(cause.correlationId());
        assertThat(header(record, EventHeaders.CAUSATION_ID)).isEqualTo(cause.eventId().toString());
        assertThat(header(record, EventHeaders.SOURCE)).isEqualTo("test-app");
        assertThat(record.headers().headers("__TypeId__")).isEmpty();
    }

    @Test
    void eventsWithTheSameKeyGoToTheSamePartitionInPublishOrder() {
        List<CompletableFuture<PublishResult>> futures = IntStream.rangeClosed(1, 20)
                .mapToObj(i -> publisher.publish(
                        events.create("SampleMoved", 1, new SamplePayload("step", i)), i % 2 == 0 ? "customer-a" : "customer-b"))
                .toList();
        List<PublishResult> results = futures.stream().map(CompletableFuture::join).toList();

        assertThat(results).allSatisfy(result -> assertThat(result.offset()).isNotNegative());

        List<ConsumerRecord<String, String>> records = readAll("ordered.v1");
        for (String key : List.of("customer-a", "customer-b")) {
            List<ConsumerRecord<String, String>> sameKey =
                    records.stream().filter(r -> r.key().equals(key)).toList();

            assertThat(sameKey).hasSize(10);
            assertThat(sameKey).extracting(ConsumerRecord::partition).containsOnly(sameKey.getFirst().partition());
            assertThat(sameKey).extracting(ConsumerRecord::offset).isSorted();
            assertThat(sameKey)
                    .extracting(r -> jsonMapper.readTree(r.value()).get("payload").get("quantity").asInt())
                    .isSorted();
        }
    }

    @Test
    void asyncPublishCompletesTheFutureWithPartitionAndOffset() {
        EventEnvelope<SamplePayload> event = events.create("SampleCreated", 1, new SamplePayload("async", 1));

        CompletableFuture<PublishResult> future = publisher.publish(event, "key-async");

        assertThat(future).succeedsWithin(TIMEOUT).satisfies(result -> {
            assertThat(result.topic()).isEqualTo("samples.v1");
            assertThat(result.partition()).isBetween(0, 2);
            assertThat(result.offset()).isNotNegative();
        });
    }

    @Test
    void eventTypeWithoutDeclaredTopicIsRejectedBeforeSending() {
        EventEnvelope<SamplePayload> event = events.create("NotDeclared", 1, new SamplePayload("x", 1));

        assertThatIllegalArgumentException()
                .isThrownBy(() -> publisher.publish(event, "key"))
                .withMessageContaining("NotDeclared");
    }

    private static List<ConsumerRecord<String, String>> readAll(String topic) {
        return TopicReader.readAll(KafkaTestContainer.bootstrapServers(), topic);
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
