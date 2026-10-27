package com.neo4j.data.importer.extractors;

import com.joestelmach.natty.Parser;
import org.folg.gedcom.model.Family;

public class FamilyExtractors {

    private FamilyExtractors() {}

    public static AttributeExtractor<Family> familyExtractor(Parser dateParser) {
        return new DefaultFamilyExtractor(dateParser);
    }
}
