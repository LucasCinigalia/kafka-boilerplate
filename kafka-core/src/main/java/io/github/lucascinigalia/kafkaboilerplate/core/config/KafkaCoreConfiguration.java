package io.github.lucascinigalia.kafkaboilerplate.core.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/** Entry point of the boilerplate: an application imports this class to get everything. */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(BoilerplateKafkaProperties.class)
@Import({KafkaProducerConfiguration.class, KafkaTopicsConfiguration.class})
public class KafkaCoreConfiguration {}
