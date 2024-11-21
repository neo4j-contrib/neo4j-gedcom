package com.neo4j.data.importer.extractors;

import com.joestelmach.natty.Parser;
import java.util.function.Supplier;
import org.folg.gedcom.model.Family;

public class FamilyExtractors implements Supplier<AttributeExtractor<Family>> {

    private final Parser dateParser;

    public FamilyExtractors(Parser dateParser) {
        this.dateParser = dateParser;
    }

    @Override
    public AttributeExtractor<Family> get() {
        return new DefaultFamilyExtractor(dateParser);
    }
}
