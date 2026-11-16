package io.github.lucascinigalia.kafkaboilerplate.core.support;

import io.github.lucascinigalia.kafkaboilerplate.core.config.KafkaCoreConfiguration;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Import;

/** Minimal application: what a project needs to do to use the boilerplate. */
@SpringBootConfiguration
@EnableAutoConfiguration
@Import(KafkaCoreConfiguration.class)
public class TestApplication {}
