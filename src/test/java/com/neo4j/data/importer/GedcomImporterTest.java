package com.neo4j.data.importer;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.Driver;
import org.neo4j.driver.EagerResult;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

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

    private EagerResult executeProcedure(Driver driver, String fileName) {
        return driver.executableQuery(
                        "CALL genealogy.loadGedcom($fileName) yield nodesCreated, relationshipsCreated return *")
                .withParameters(Map.of("fileName", fileName))
                .execute();
    }

    @Test
    void loads_individuals() {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI())) {
            executeProcedure(driver, "SimpsonsCartoon.ged");

            var individuals =
                    driver.executableQuery("MATCH (person:Person) RETURN person").execute(Collectors.toList()).stream()
                            .map(record -> asPersons(record, "person"));

            assertThat(individuals)
                    .containsExactlyInAnyOrder(
                            new Person(List.of("Abraham"), List.of("Simpson"), "M"),
                            new Person(List.of("Bart"), List.of("Simpson"), "M"),
                            new Person(List.of("Clancy"), List.of("Bouvier"), "M"),
                            new Person(List.of("Homer"), List.of("Simpson"), "M"),
                            new Person(List.of("Jacqueline"), List.of("Bouvier"), "F"),
                            new Person(List.of("Lisa"), List.of("Simpson"), "F"),
                            new Person(List.of("Maggie"), List.of("Simpson"), "F"),
                            new Person(List.of("Marge"), List.of("Simpson"), "F"),
                            new Person(List.of("Mona"), List.of("Simpson"), "F"),
                            new Person(List.of("Patty"), List.of("Bouvier"), "F"),
                            new Person(List.of("Selma"), List.of("Bouvier"), "F"));
        }
    }

    @Test
    void loads_relationships() {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI())) {
            var result = executeProcedure(driver, "SimpsonsCartoon.ged");

            var statistics = result.records().get(0);
            var nodesCreated = statistics.get("nodesCreated").asLong();
            var relationshipsCreated = statistics.get("relationshipsCreated").asLong();

            var relationships = driver
                    .executableQuery("MATCH (i:Person)-[r]->(j:Person) return r, i, j")
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

            assertThat(nodesCreated).isEqualTo(11);

            assertThat(relationshipsCreated).isEqualTo(17);

            var homer = new Person(List.of("Homer"), List.of("Simpson"), "M");
            var marge = new Person(List.of("Marge"), List.of("Simpson"), "F");
            var abraham = new Person(List.of("Abraham"), List.of("Simpson"), "M");
            var mona = new Person(List.of("Mona"), List.of("Simpson"), "F");
            var clancy = new Person(List.of("Clancy"), List.of("Bouvier"), "M");
            var jacqueline = new Person(List.of("Jacqueline"), List.of("Bouvier"), "F");
            var lisa = new Person(List.of("Lisa"), List.of("Simpson"), "F");
            var bart = new Person(List.of("Bart"), List.of("Simpson"), "M");
            var maggie = new Person(List.of("Maggie"), List.of("Simpson"), "F");
            var selma = new Person(List.of("Selma"), List.of("Bouvier"), "F");
            var patty = new Person(List.of("Patty"), List.of("Bouvier"), "F");
            assertThat(relationships)
                    .containsExactlyInAnyOrder(
                            familyRel(homer, "IS_MARRIED_TO", marge),
                            familyRel(abraham, "IS_MARRIED_TO", mona),
                            familyRel(clancy, "IS_MARRIED_TO", jacqueline),
                            familyRel(homer, "IS_CHILD_OF", abraham),
                            familyRel(homer, "IS_CHILD_OF", mona),
                            familyRel(marge, "IS_CHILD_OF", clancy),
                            familyRel(marge, "IS_CHILD_OF", jacqueline),
                            familyRel(lisa, "IS_CHILD_OF", marge),
                            familyRel(lisa, "IS_CHILD_OF", homer),
                            familyRel(bart, "IS_CHILD_OF", marge),
                            familyRel(bart, "IS_CHILD_OF", homer),
                            familyRel(maggie, "IS_CHILD_OF", marge),
                            familyRel(maggie, "IS_CHILD_OF", homer),
                            familyRel(selma, "IS_CHILD_OF", jacqueline),
                            familyRel(selma, "IS_CHILD_OF", clancy),
                            familyRel(patty, "IS_CHILD_OF", jacqueline),
                            familyRel(patty, "IS_CHILD_OF", clancy));
        }
    }

    @Test
    void parses_date() {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI())) {
            executeProcedure(driver, "555Sample.ged");

            var relationships = driver.executableQuery(
                            "MATCH (i:Person) WHERE i.birth_date > date({ year: 1800 }) return i")
                    .execute(Collectors.toList());

            assertThat(relationships).hasSize(2).allSatisfy(relationship -> {
                var node = relationship.get("i").asNode();
                assertThat(node.get("raw_birth_date").asString()).matches(".*\\d{4}.*");
                assertThat(node.get("birth_date").asLocalDate().getYear()).isGreaterThan(1800);
            });
        }
    }

    private static FamilyRelation familyRel(Person person1, String relType, Person person2) {
        return new FamilyRelation(relType, person1, person2);
    }

    private static Path pathOfResource(String classpathResource) throws Exception {
        return Path.of(Thread.currentThread()
                .getContextClassLoader()
                .getResource(classpathResource)
                .toURI());
    }

    private static Person asPersons(Record record, String nodeName) {
        var person = record.get(nodeName).asNode();
        return new Person(
                getNames(person, "first_names"),
                getNames(person, "last_names"),
                person.get("gender").asString());
    }

    private static List<String> getNames(Node person, String namePart) {
        return person.get(namePart).asList(Value::asString);
    }

    private static FamilyRelation asRelationships(Record record) {
        var rel = record.get("r").asRelationship();
        return new FamilyRelation(rel.type(), asPersons(record, "i"), asPersons(record, "j"));
    }

    record FamilyRelation(String type, Person person1, Person person2) {}

    record Person(List<String> firstNames, List<String> lastNames, String gender) {}
}
