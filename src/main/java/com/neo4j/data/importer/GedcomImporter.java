package com.neo4j.data.importer;

import com.joestelmach.natty.Parser;
import com.neo4j.data.importer.extractors.FamilyExtractors;
import com.neo4j.data.importer.extractors.PersonExtractors;
import java.io.File;
import java.io.IOException;
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

    @Context
    public GraphDatabaseService db;

    @Context
    public Log logger;

    @Context
    public DependencyResolver dependencyResolver;

    @Procedure(value = "genealogy.loadGedcom", mode = Mode.WRITE)
    public Stream<Statistics> loadGedcom(@Name("file") String file) throws IOException, SAXParseException {
        var filePath = rebuildPath(file);
        var model = loadModel(filePath);

        var dateParser = new Parser();
        var statistics = new Statistics();
        try (Transaction tx = db.beginTx()) {

            var personExtractors = new PersonExtractors(dateParser, model);
            model.getPeople().forEach(person -> {
                var personExtractor = personExtractors.get();
                var attributes = personExtractor.apply(person);
                var personsStats = tx.execute(personExtractor.query(), Map.of("attributes", attributes))
                        .getQueryStatistics();

                personExtractor.updateCounters(personsStats, statistics);
            });

            var familyExtractors = new FamilyExtractors(dateParser);
            model.getFamilies().forEach(family -> {
                var familyExtractor = familyExtractors.get();
                var familyStats = tx.execute(familyExtractor.query(), familyExtractor.apply(family))
                        .getQueryStatistics();

                familyExtractor.updateCounters(familyStats, statistics);
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

    public static Gedcom loadModel(String filePath) throws IOException, SAXParseException {
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
