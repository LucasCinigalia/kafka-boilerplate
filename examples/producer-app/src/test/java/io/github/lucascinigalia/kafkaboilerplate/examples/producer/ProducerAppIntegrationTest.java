package io.github.lucascinigalia.kafkaboilerplate.examples.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.github.lucascinigalia.kafkaboilerplate.core.event.EventHeaders;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** End to end: HTTP request in, record in a real Kafka topic out. */
@SpringBootTest
@AutoConfigureMockMvc
class ProducerAppIntegrationTest {

    private static final String TOPIC = "orders.v1";

    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.2.2");

    static {
        KAFKA.start();
    }

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("boilerplate.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JsonMapper jsonMapper;

    @Test
    void placingAnOrderPublishesOrderCreatedKeyedByCustomer() throws Exception {
        String response = mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-Correlation-Id", "request-42")
                        .content("""
                                {"customerId": "customer-1", "totalAmount": 99.90}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.topic").value(TOPIC))
                .andExpect(jsonPath("$.correlationId").value("request-42"))
                .andReturn()
                .getResponse()
                .getContentAsString();
        JsonNode body = jsonMapper.readTree(response);

        ConsumerRecord<String, String> record = readAll().stream()
                .filter(r -> r.partition() == body.get("partition").asInt()
                        && r.offset() == body.get("offset").asLong())
                .findFirst()
                .orElseThrow();
        JsonNode event = jsonMapper.readTree(record.value());

        assertThat(record.key()).isEqualTo("customer-1");
        assertThat(new String(record.headers().lastHeader(EventHeaders.CORRELATION_ID).value(), StandardCharsets.UTF_8))
                .isEqualTo("request-42");
        assertThat(event.get("type").asString()).isEqualTo("OrderCreated");
        assertThat(event.get("source").asString()).isEqualTo("producer-app");
        assertThat(event.get("eventId").asString()).isEqualTo(body.get("eventId").asString());
        assertThat(event.get("payload").get("orderId").asString())
                .isEqualTo(body.get("orderId").asString());
        assertThat(event.get("payload").get("customerId").asString()).isEqualTo("customer-1");
        assertThat(event.get("payload").get("totalAmount").decimalValue()).isEqualByComparingTo("99.90");
    }

    @Test
    void invalidOrderIsRejectedAndNothingIsPublished() throws Exception {
        long before = readAll().size();

        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"customerId": "", "totalAmount": -1}
                                """))
                .andExpect(status().isBadRequest());

        assertThat(readAll()).hasSize((int) before);
    }

    private static List<ConsumerRecord<String, String>> readAll() {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            List<TopicPartition> partitions = consumer.partitionsFor(TOPIC).stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition()))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            List<ConsumerRecord<String, String>> records = new ArrayList<>();
            while (partitions.stream().anyMatch(p -> consumer.position(p) < endOffsets.get(p))) {
                consumer.poll(Duration.ofMillis(200)).forEach(records::add);
            }
            return records;
        }
    }
}
