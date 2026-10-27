package com.neo4j.data.importer;

import com.joestelmach.natty.Parser;
import com.neo4j.data.importer.extractors.FamilyExtractors;
import com.neo4j.data.importer.extractors.PersonExtractors;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.folg.gedcom.model.Gedcom;
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

    private static final int BATCH_SIZE = 1000;

    @Context
    public GraphDatabaseService db;

    @Context
    public Log logger;

    @Context
    public DependencyResolver dependencyResolver;

    @Procedure(value = "genealogy.loadGedcom", mode = Mode.SCHEMA)
    public Stream<Statistics> loadGedcom(@Name("file") String file) throws IOException, SAXParseException {
        var filePath = rebuildPath(file);
        var model = loadModel(filePath);

        try (Transaction tx = db.beginTx()) {
            tx.execute("CREATE RANGE INDEX person_id IF NOT EXISTS FOR (p:Person) ON (p.id)");
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.execute("CALL db.awaitIndexes(300)");
            tx.commit();
        }

        var dateParser = new Parser();
        var statistics = new Statistics();

        var personExtractor = PersonExtractors.personExtractor(dateParser, model);
        var personAttributes = model.getPeople().stream().map(personExtractor).toList();
        for (var batch : partition(personAttributes, BATCH_SIZE)) {
            try (Transaction tx = db.beginTx()) {
                var personsStats = tx.execute(personExtractor.query(), Map.of("batch", batch))
                        .getQueryStatistics();

                personExtractor.updateCounters(personsStats, statistics);
                tx.commit();
            }
        }

        var familyExtractor = FamilyExtractors.familyExtractor(dateParser);
        var familyAttributes = model.getFamilies().stream().map(familyExtractor).toList();
        for (var batch : partition(familyAttributes, BATCH_SIZE)) {
            try (Transaction tx = db.beginTx()) {
                var familyStats = tx.execute(familyExtractor.query(), Map.of("families", batch))
                        .getQueryStatistics();

                familyExtractor.updateCounters(familyStats, statistics);
                tx.commit();
            }
        }

        logger.info(
                "Created {} nodes, {} relationships from {} GEDCOM import",
                statistics.nodesCreated,
                statistics.relationshipsCreated,
                file);
        return Stream.of(statistics);
    }

    public static Gedcom loadModel(String filePath) throws IOException, SAXParseException {
        var modelParser = new ModelParser();
        var gedcomFile = new File(filePath);
        var gedcom = modelParser.parseGedcom(gedcomFile);
        gedcom.createIndexes();
        return gedcom;
    }

    private static <T> List<List<T>> partition(List<T> list, int size) {
        List<List<T>> result = new ArrayList<>((list.size() + size - 1) / size);
        for (int i = 0; i < list.size(); i += size) {
            result.add(list.subList(i, Math.min(i + size, list.size())));
        }
        return result;
    }

    private String rebuildPath(String fileName) {
        Config config = dependencyResolver.resolveDependency(Config.class);
        var fileRoot = config.get(GraphDatabaseSettings.load_csv_file_url_root);
        return fileRoot + "/" + fileName;
    }
}
