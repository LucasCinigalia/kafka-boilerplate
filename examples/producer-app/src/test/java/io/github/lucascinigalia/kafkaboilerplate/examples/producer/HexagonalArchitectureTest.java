package io.github.lucascinigalia.kafkaboilerplate.examples.producer;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/** Business code must not know Kafka, and must not know the adapters. */
@AnalyzeClasses(
        packages = "io.github.lucascinigalia.kafkaboilerplate",
        importOptions = ImportOption.DoNotIncludeTests.class)
class HexagonalArchitectureTest {

    private static final String EXAMPLE = "io.github.lucascinigalia.kafkaboilerplate.examples.producer";

    @ArchTest
    static final ArchRule businessCodeDoesNotDependOnKafka = noClasses()
            .that()
            .resideInAnyPackage(EXAMPLE + ".domain..", EXAMPLE + ".application..")
            .should()
            .dependOnClassesThat()
            .resideInAnyPackage("org.apache.kafka..", "org.springframework.kafka..");

    @ArchTest
    static final ArchRule businessCodeDoesNotDependOnAdapters = noClasses()
            .that()
            .resideInAnyPackage(EXAMPLE + ".domain..", EXAMPLE + ".application..")
            .should()
            .dependOnClassesThat()
            .resideInAPackage(EXAMPLE + ".adapter..");

    @ArchTest
    static final ArchRule eventPortDoesNotDependOnKafka = noClasses()
            .that()
            .resideInAPackage("io.github.lucascinigalia.kafkaboilerplate.core.event..")
            .should()
            .dependOnClassesThat()
            .resideInAnyPackage("org.apache.kafka..", "org.springframework.kafka..");
}
