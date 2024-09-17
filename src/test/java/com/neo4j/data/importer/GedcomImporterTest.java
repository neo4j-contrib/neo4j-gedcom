package com.neo4j.data.importer;

import org.junit.jupiter.api.*;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.types.Relationship;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

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

    @AfterEach
    void afterEach() {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI())) {
            driver.executableQuery("MATCH (n) detach delete n").execute();
        }
    }

    @Test
    void loads_individuals() {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI())) {
            driver.executableQuery("CALL genealogy.loadGedcom('SimpsonsCartoon.ged')").execute();

            var individuals = driver.executableQuery("MATCH (i:Person) RETURN i AS person ORDER BY i.first_names ASC, i.last_names ASC")
                    .execute(Collectors.toList())
                    .stream()
                    .map(GedcomImporterTest::asIndividuals);

            assertThat(individuals).containsExactly(
                    new Person(List.of("Abraham"), List.of("Simpson")),
                    new Person(List.of("Bart"), List.of("Simpson")),
                    new Person(List.of("Clancy"), List.of("Bouvier")),
                    new Person(List.of("Homer"), List.of("Simpson")),
                    new Person(List.of("Jacqueline"), List.of("Bouvier")),
                    new Person(List.of("Lisa"), List.of("Simpson")),
                    new Person(List.of("Maggie"), List.of("Simpson")),
                    new Person(List.of("Marge"), List.of("Simpson")),
                    new Person(List.of("Mona"), List.of("Simpson")),
                    new Person(List.of("Patty"), List.of("Bouvier")),
                    new Person(List.of("Selma"), List.of("Bouvier"))
            );
        }
    }

    @Test
    void loads_relationships() {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI())) {
            var result = driver.executableQuery("CALL genealogy.loadGedcom('SimpsonsCartoon.ged') yield nodesCreated, relationshipsCreated return *").execute();
            var statistics = result.records().get(0);
            var nodesCreated = statistics.get("nodesCreated").asLong();
            var relationshipsCreated = statistics.get("relationshipsCreated").asLong();

            var relationships = driver.executableQuery("MATCH (i:Person)-[r]->(j:Person) return r")
                    .execute(Collectors.toList())
                    .stream()
                    .map(GedcomImporterTest::asRelationships)
                    .toList();

            assertThat(relationships)
                    .hasSize(17)
                    .filteredOn(r -> r.type().equals("IS_MARRIED_TO"))
                    .hasSize(3);

            assertThat(relationships)
                    .filteredOn(r -> r.type().equals("IS_CHILD_OF"))
                    .hasSize(14);

            assertThat(nodesCreated)
                    .isEqualTo(11);

            assertThat(relationshipsCreated)
                    .isEqualTo(17);

        }
    }

    private static Path pathOfResource(String classpathResource) throws Exception {
        return Path.of(
                Thread.currentThread().getContextClassLoader().getResource(classpathResource).toURI()
        );
    }

    private static Person asIndividuals(Record record) {
        var person = record.get("person").asNode();
        return new Person(
                person.get("first_names").asList(Value::asString),
                person.get("last_names").asList(Value::asString));
    }

    private static Relationship asRelationships(Record record) {
        return record.get("r").asRelationship();
    }

    record Person(List<String> firstNames, List<String> lastNames) {
    }
}
