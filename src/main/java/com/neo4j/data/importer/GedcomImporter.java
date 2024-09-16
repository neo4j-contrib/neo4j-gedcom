package com.neo4j.data.importer;

import org.gedcom4j.exception.GedcomParserException;
import org.gedcom4j.model.Family;
import org.gedcom4j.model.Gedcom;
import org.gedcom4j.model.Individual;
import org.gedcom4j.parser.GedcomParser;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;

public class GedcomImporter {

    @Context
    public GraphDatabaseService db;

    @Context
    public DependencyResolver dependencyResolver;

    @Procedure
    public void doImport(@Name("gedcomFile") String gedcomFileName) throws IOException {
        // Read file

        var filePath = readFile(gedcomFileName);


        //
//        var setting = graphDatabaseSettings.getSetting("server.directories.import");


        String x  = ";";
        // Parse

        Gedcom gedcom;
        var parser = new GedcomParser();
        try {
            parser.load(filePath);
            gedcom = parser.getGedcom();
        } catch (GedcomParserException e) {
            throw new RuntimeException(e);
        }

        // Import
        gedcom.getIndividuals().forEach((id, individual) -> createIndividualNodes(individual));
        gedcom.getFamilies().forEach((id, family) -> createFamilyRelationships(family));


    }

    private void createFamilyRelationships(Family family) {

    }

    private void createIndividualNodes(Individual individualName) {
        //todo
    }

    private String readFile(String gedcomFileName) {
        Config config = dependencyResolver.resolveDependency(Config.class);
        var fileRoot = config.get(GraphDatabaseSettings.load_csv_file_url_root);
        return fileRoot + "/" + gedcomFileName;
    }

}
