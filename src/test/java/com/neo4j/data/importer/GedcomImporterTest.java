package com.neo4j.data.importer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.internal.SecuritySettings;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GedcomImporterTest {
    private Neo4j neo4j;

    @BeforeAll
    void initializeNeo4j() throws Exception {
        this.neo4j = Neo4jBuilders.newInProcessBuilder()
                .withDisabledServer()
                .withProcedure(GedcomImporter.class)
                .withConfig(GraphDatabaseSettings.procedure_unrestricted, List.of("*"))
                .withConfig(GraphDatabaseSettings.load_csv_file_url_root, pathOfResource("ged-files"))
                .build();
    }

    @AfterAll
    void closeNeo4j() {
        this.neo4j.close();
    }

    @Test
    void loads_individuals() {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI())) {
            driver.executableQuery("CALL genealogy.loadGedcom('SimpsonsCartoon.ged')").execute();

            var individuals = driver.executableQuery("MATCH (i:Individual) RETURN i AS individual ORDER BY i.first_names ASC, i.last_names ASC")
                    .execute(Collectors.toList())
                    .stream()
                    .map(GedcomImporterTest::asIndividuals);

            assertThat(individuals).containsExactly(
                    new Individual(List.of("Abraham"), List.of("Simpson")),
                    new Individual(List.of("Bart"), List.of("Simpson")),
                    new Individual(List.of("Clancy"), List.of("Bouvier")),
                    new Individual(List.of("Homer"), List.of("Simpson")),
                    new Individual(List.of("Jacqueline"), List.of("Bouvier")),
                    new Individual(List.of("Lisa"), List.of("Simpson")),
                    new Individual(List.of("Maggie"), List.of("Simpson")),
                    new Individual(List.of("Marge"), List.of("Simpson")),
                    new Individual(List.of("Mona"), List.of("Simpson")),
                    new Individual(List.of("Patty"), List.of("Bouvier")),
                    new Individual(List.of("Selma"), List.of("Bouvier"))
            );
        }
    }


    private static Path pathOfResource(String classpathResource) throws Exception {
        return Path.of(
                Thread.currentThread().getContextClassLoader().getResource(classpathResource).toURI()
        );
    }

    private static Individual asIndividuals(Record record) {
        var individual = record.get("individual").asNode();
        return new Individual(
                individual.get("first_names").asList(Value::asString),
                individual.get("last_names").asList(Value::asString));
    }

    record Individual(List<String> firstNames, List<String> lastNames) {}
}
