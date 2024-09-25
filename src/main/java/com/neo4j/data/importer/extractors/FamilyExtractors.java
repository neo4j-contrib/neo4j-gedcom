package com.neo4j.data.importer.extractors;

import java.util.function.Supplier;
import org.folg.gedcom.model.Family;

public class FamilyExtractors implements Supplier<AttributeExtractor<Family>> {

    @Override
    public AttributeExtractor<Family> get() {
        return new DefaultFamilyExtractor();
    }
}
