package com.neo4j.data.importer;

import com.neo4j.data.importer.extractors.PersonExtractor;
import org.folg.gedcom.parser.ModelParser;
import org.gedcomx.Gedcomx;
import org.gedcomx.conversion.GedcomxConversionResult;
import org.gedcomx.conversion.gedcom.dq55.GedcomMapper;
import org.gedcomx.conversion.gedcom.dq55.MappingConfig;
import org.gedcomx.types.RelationshipType;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import org.xml.sax.SAXParseException;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

public class GedcomImporter {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log logger;

    @Context
    public DependencyResolver dependencyResolver;

    @Procedure(value = "genealogy.loadGedcom", mode = Mode.WRITE)
    public Stream<Statistics> loadGedcom(@Name("file") String file) throws IOException, SAXParseException {
        var filePath = rebuildPath(file);
        var modelX = convertGedcomFile(filePath);

        var statistics = new Statistics();

        try (Transaction tx = db.beginTx()) {
            modelX.getPersons().forEach(person -> {
                var attributes = new PersonExtractor(person).extract();
                var personsStats = tx.execute("CREATE (i:Person) SET i = $attributes", Map.of("attributes", attributes))
                        .getQueryStatistics();

                statistics.addNodesCreated(personsStats.getNodesCreated());

            });

            modelX.getRelationships().forEach(relationship -> {

                var person1 = modelX.findPerson(relationship.getPerson1().getResource());
                var person2 = modelX.findPerson(relationship.getPerson2().getResource());

                if (person1 == null || person2 == null) {
                    logger.debug("Unable to find person(s) for relationship: {}", relationship.getId());
                    return;
                }

                if (relationship.getType().equals(RelationshipType.Couple.toQNameURI())) {
                    var stats = tx.execute("""
                                    MATCH (h:Person {id: $p1}), (w:Person {id: $p2})
                                    CREATE (h)-[:IS_MARRIED_TO]->(w)""",
                            Map.of(
                                    "p1", person1.getId(),
                                    "p2", person2.getId()
                            )
                    ).getQueryStatistics();

                    statistics.addRelationshipsCreated(stats.getRelationshipsCreated());

                } else if (relationship.getType().equals(RelationshipType.ParentChild.toQNameURI())) {
                    var stats = tx.execute("""
                                    MATCH (c:Person {id: $child}), (p:Person {id: $parent})
                                    CREATE (c)-[:IS_CHILD_OF]->(p)""",
                            Map.of(
                                    "parent", person1.getId(),
                                    "child", person2.getId()
                            )
                    ).getQueryStatistics();

                    statistics.addRelationshipsCreated(stats.getRelationshipsCreated());
                }
            });

            tx.commit();
        }

        logger.info("Created {} nodes, {} relationships from {} GEDCOM import", statistics.nodesCreated, statistics.relationshipsCreated, file);
        return Stream.of(statistics);
    }

    public static Gedcomx convertGedcomFile(String filePath) throws IOException, SAXParseException {
        var modelParser = new ModelParser();
        var gedcomFile = new File(filePath);
        org.folg.gedcom.model.Gedcom gedcom = modelParser.parseGedcom(gedcomFile);
        gedcom.createIndexes();

        MappingConfig config = new MappingConfig(gedcomFile.getName(), false);
        var mapper = new GedcomMapper(config);
        GedcomxConversionResult result = mapper.toGedcomx(gedcom);
        return result.getDataset();
    }

    private String rebuildPath(String fileName) {
        Config config = dependencyResolver.resolveDependency(Config.class);
        var fileRoot = config.get(GraphDatabaseSettings.load_csv_file_url_root);
        return fileRoot + "/" + fileName;
    }
}
