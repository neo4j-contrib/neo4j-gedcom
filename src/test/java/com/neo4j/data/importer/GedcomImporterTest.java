package com.neo4j.data.importer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.*;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GedcomImporterTest {
    private Neo4j neo4j;

    @BeforeAll
    void initializeNeo4j() throws URISyntaxException {
        String procName = "com.neo4j.data.importer.doImport";
        this.neo4j = Neo4jBuilders.newInProcessBuilder()
                .withDisabledServer()
                .withProcedure(GedcomImporter.class)
                .withConfig(GraphDatabaseSettings.load_csv_file_url_root, Path.of(
                        Thread.currentThread().getContextClassLoader().getResource("SimpsonsCartoon.ged").toURI()
                ).getParent())
                .withConfig(GraphDatabaseSettings.procedure_unrestricted, List.of("*"))
                .build();
    }

    @AfterAll
    void closeNeo4j() {
        this.neo4j.close();
    }

    @Test
    void proceduresExists() {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI())) {
            ExecutableQuery executableQuery = driver.executableQuery("call com.neo4j.data.importer.doImport('test')");
            EagerResult result = executableQuery.execute();

            assertNotNull(result);
            assertEquals(0, result.keys().size());
        }
    }

    @Test
    void fileExists() {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI())) {
            ExecutableQuery executableQuery = driver.executableQuery("call com.neo4j.data.importer.doImport('SimpsonsCartoon.ged')");
            EagerResult result = executableQuery.execute();

            assertNotNull(result);
            assertEquals(0, result.keys().size());
        }
    }

}