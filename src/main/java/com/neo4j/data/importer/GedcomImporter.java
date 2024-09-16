package com.neo4j.data.importer;

import org.gedcom4j.exception.GedcomParserException;
import org.gedcom4j.model.Gedcom;
import org.gedcom4j.model.Individual;
import org.gedcom4j.model.PersonalName;
import org.gedcom4j.model.StringWithCustomFacts;
import org.gedcom4j.parser.GedcomParser;
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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class GedcomImporter {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log logger;

    @Context
    public DependencyResolver dependencyResolver;

    @Procedure(value = "genealogy.loadGedcom", mode = Mode.WRITE)
    public void loadGedcom(@Name("file") String file) throws IOException {
        var filePath = rebuildPath(file);
        var model = parseModel(filePath);

        try (Transaction tx = db.beginTx()) {
            model.getIndividuals().values()
                    .forEach(individual -> {
                        var attributes = Map.of(
                                "first_names", extractNames(individual, PersonalName::getGivenName),
                                "last_names", extractNames(individual, PersonalName::getSurname)
                        );
                        tx.execute("CREATE (i:Individual) SET i = $attributes", Map.of("attributes", attributes));
                    });
            tx.commit();
        }

    }

    private static List<String> extractNames(Individual individual, Function<PersonalName, StringWithCustomFacts> extractor) {
        return individual.getNames()
                .stream()
                .filter(name -> extractor.apply(name) != null)
                .map(name -> extractor.apply(name).getValue())
                .collect(Collectors.toList());
    }

    private Gedcom parseModel(String filePath) throws IOException {
        var parser = new GedcomParser();
        try {
            parser.load(filePath);
            processImportIssues(parser);
            return parser.getGedcom();
        } catch (GedcomParserException e) {
            throw new RuntimeException(e);
        }
    }

    private void processImportIssues(GedcomParser parser) {
        List<String> errors = parser.getErrors();
        if (!errors.isEmpty()) {
            throw new RuntimeException(String.format("The following errors occurred during the import: %n\t%s", String.join("\n\t", errors)));
        }
        parser.getWarnings().forEach((warning) -> {
            logger.warn(warning);
        });
    }

    private String rebuildPath(String fileName) {
        Config config = dependencyResolver.resolveDependency(Config.class);
        var fileRoot = config.get(GraphDatabaseSettings.load_csv_file_url_root);
        return fileRoot + "/" + fileName;
    }

}
