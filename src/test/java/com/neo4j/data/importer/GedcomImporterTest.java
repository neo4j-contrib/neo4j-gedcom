package com.neo4j.data.importer;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
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

    private EagerResult loadGedcom(Driver driver, String fileName) {
        return driver.executableQuery(
                        "CALL genealogy.loadGedcom($fileName) yield nodesCreated, relationshipsCreated return *")
                .withParameters(Map.of("fileName", fileName))
                .execute();
    }

    @Test
    void loads_individuals() {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI())) {
            loadGedcom(driver, "SimpsonsCartoon.ged");

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
            var result = loadGedcom(driver, "SimpsonsCartoon.ged");

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
                    .filteredOn(r -> r.type().equals("SPOUSE_OF"))
                    .hasSize(3);

            assertThat(relationships)
                    .filteredOn(r -> r.type().equals("CHILD_OF"))
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
                            familyRel(homer, "SPOUSE_OF", marge),
                            familyRel(abraham, "SPOUSE_OF", mona),
                            familyRel(clancy, "SPOUSE_OF", jacqueline),
                            familyRel(homer, "CHILD_OF", abraham),
                            familyRel(homer, "CHILD_OF", mona),
                            familyRel(marge, "CHILD_OF", clancy),
                            familyRel(marge, "CHILD_OF", jacqueline),
                            familyRel(lisa, "CHILD_OF", marge),
                            familyRel(lisa, "CHILD_OF", homer),
                            familyRel(bart, "CHILD_OF", marge),
                            familyRel(bart, "CHILD_OF", homer),
                            familyRel(maggie, "CHILD_OF", marge),
                            familyRel(maggie, "CHILD_OF", homer),
                            familyRel(selma, "CHILD_OF", jacqueline),
                            familyRel(selma, "CHILD_OF", clancy),
                            familyRel(patty, "CHILD_OF", jacqueline),
                            familyRel(patty, "CHILD_OF", clancy));
        }
    }

    @Test
    void parses_person_event_dates() {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI())) {
            loadGedcom(driver, "555Sample.ged");

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

    @Test
    void parses_same_sex_marriages() {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI())) {
            loadGedcom(driver, "SSMARR.ged");

            var relationships = driver
                    .executableQuery(
                            """
                            MATCH (i: Person)-[r:SPOUSE_OF]->(j: Person)
                            WHERE i.gender = j.gender
                            RETURN i, r, j
                            """)
                    .execute(Collectors.toList())
                    .stream()
                    .map(GedcomImporterTest::asRelationships)
                    .toList();

            var john = new Person(List.of("John"), List.of("Smith"), "M");
            var steven = new Person(List.of("Steven"), List.of("Stevens"), "M");
            assertThat(relationships).containsExactlyInAnyOrder(familyRel(john, "SPOUSE_OF", steven));
        }
    }

    @Test
    void processes_remarriages() {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI())) {
            loadGedcom(driver, "REMARR.ged");

            var relationships = driver
                    .executableQuery(
                            """
                            MATCH (i: Person)-[r:SPOUSE_OF]-(j: Person)
                            MATCH (i)-[:SPOUSE_OF]-(k: Person)
                            WHERE id(j) <> id(k)
                            RETURN i, r, j""")
                    .execute(Collectors.toList())
                    .stream()
                    .map(GedcomImporterTest::asRelationships)
                    .toList();

            var mary = new Person(List.of("Mary"), List.of("Encore"), "F");
            var peter = new Person(List.of("Peter"), List.of("Sweet"), "M");
            var juan = new Person(List.of("Juan"), List.of("Donalds"), "M");

            assertThat(relationships).contains(familyRel(mary, "SPOUSE_OF", peter), familyRel(mary, "SPOUSE_OF", juan));
            ;
        }
    }

    @Test
    void parses_Heredis_preferred_name() {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI())) {
            loadGedcom(driver, "HeredisPreferredName.ged");

            var preferredFirstNames = driver
                    .executableQuery(
                            """
                            MATCH (p:Person {
                                first_names: ["Canan", "Jane", "Françoise"],
                                last_names: ["Doe"]
                            })
                            RETURN p.preferred_first_name AS name
                            """)
                    .execute(Collectors.toList())
                    .stream()
                    .map(record -> record.get("name").asString())
                    .toList();

            assertThat(preferredFirstNames).containsExactly("Jane");
        }
    }

    @Test
    void parses_detailed_marriage_information() {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI())) {
            loadGedcom(driver, "DetailedMarriageDivorceInfo.ged");

            var marriages = driver
                    .executableQuery(
                            """
                            MATCH (john:Person {first_names: ["John"], last_names: ["DOE"]}),
                                  (jane:Person {first_names: ["Jane"], last_names: ["DOE"]}),
                                  (john)-[r:MARRIED_TO]->(jane)
                            RETURN r
                            """)
                    .execute(Collectors.toList())
                    .stream()
                    .map(record -> record.get("r").asRelationship().asMap())
                    .toList();

            assertThat(marriages)
                    .containsExactlyInAnyOrder(
                            Map.of(
                                    "type", "Religious marriage",
                                    "date", LocalDate.of(1989, 3, 2),
                                    "raw_date", "2 MAR 1989",
                                    "location", "Colmar,68000,Haut Rhin,Alsace,FRANCE,"),
                            Map.of(
                                    "date", LocalDate.of(1989, 3, 1),
                                    "raw_date", "1 MAR 1989",
                                    "location", "Colmar,68000,Haut Rhin,Alsace,FRANCE,"));
        }
        ;
    }

    @Test
    void parses_divorce_information() {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI())) {
            loadGedcom(driver, "DetailedMarriageDivorceInfo.ged");

            var divorces = driver
                    .executableQuery(
                            """
                            MATCH (john:Person {first_names: ["John"], last_names: ["DOE"]}),
                                  (jane:Person {first_names: ["Jane"], last_names: ["DOE"]}),
                                  (john)-[r:DIVORCED]->(jane)
                            RETURN r
                            """)
                    .execute(Collectors.toList())
                    .stream()
                    .map(record -> record.get("r").asRelationship().asMap())
                    .toList();

            assertThat(divorces)
                    .containsExactlyInAnyOrder(Map.of(
                            "date", LocalDate.of(2017, 10, 23),
                            "raw_date", "23 OCT 2017",
                            "location", "Strasbourg,67000,Bas Rhin,Alsace,FRANCE,"));
        }
        ;
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
