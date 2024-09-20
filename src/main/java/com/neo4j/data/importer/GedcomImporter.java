package com.neo4j.data.importer;

import com.neo4j.data.importer.Lists.Pair;
import com.neo4j.data.importer.extractors.PersonExtractor;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.folg.gedcom.model.Gedcom;
import org.folg.gedcom.model.SpouseRef;
import org.folg.gedcom.parser.ModelParser;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.xml.sax.SAXParseException;

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
        var model = convertGedcomFile(filePath);

        var statistics = new Statistics();

        try (Transaction tx = db.beginTx()) {
            model.getPeople().forEach(person -> {
                var attributes = new PersonExtractor(person).extract();
                var personsStats = tx.execute("CREATE (i:Person) SET i = $attributes", Map.of("attributes", attributes))
                        .getQueryStatistics();

                statistics.addNodesCreated(personsStats.getNodesCreated());
            });

            model.getFamilies().forEach(family -> {
                List<String> spouseReferences1 =
                        family.getHusbandRefs().stream().map(SpouseRef::getRef).toList();
                List<String> spouseReferences2 =
                        family.getWifeRefs().stream().map(SpouseRef::getRef).toList();
                List<Pair<String, String>> couples = Lists.crossProduct(spouseReferences1, spouseReferences2);
                List<String> childrenReferences =
                        family.getChildRefs().stream().map(SpouseRef::getRef).toList();
                couples.forEach(couple -> {
                    var stats = tx.execute(
                                    """
                                    MATCH (spouse1:Person {id: $spouseId1}), (spouse2:Person {id: $spouseId2})
                                    CREATE (spouse1)-[:IS_MARRIED_TO]->(spouse2)
                                    WITH spouse1, spouse2
                                    UNWIND $childIds AS childId
                                    MATCH (child:Person {id: childId})
                                    CREATE (child)-[:IS_CHILD_OF]->(spouse1)
                                    CREATE (child)-[:IS_CHILD_OF]->(spouse2)
                                    """,
                                    Map.of(
                                            "spouseId1", couple.left(),
                                            "spouseId2", couple.right(),
                                            "childIds", childrenReferences))
                            .getQueryStatistics();

                    statistics.addRelationshipsCreated(stats.getRelationshipsCreated());
                });
            });

            tx.commit();
        }

        logger.info(
                "Created {} nodes, {} relationships from {} GEDCOM import",
                statistics.nodesCreated,
                statistics.relationshipsCreated,
                file);
        return Stream.of(statistics);
    }

    public static Gedcom convertGedcomFile(String filePath) throws IOException, SAXParseException {
        var modelParser = new ModelParser();
        var gedcomFile = new File(filePath);
        var gedcom = modelParser.parseGedcom(gedcomFile);
        gedcom.createIndexes();
        return gedcom;
    }

    private String rebuildPath(String fileName) {
        Config config = dependencyResolver.resolveDependency(Config.class);
        var fileRoot = config.get(GraphDatabaseSettings.load_csv_file_url_root);
        return fileRoot + "/" + fileName;
    }
}
