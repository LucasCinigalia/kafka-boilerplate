package io.github.lucascinigalia.kafkaboilerplate.examples.producer;

import io.github.lucascinigalia.kafkaboilerplate.core.config.KafkaCoreConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import(KafkaCoreConfiguration.class)
public class ProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }
}
