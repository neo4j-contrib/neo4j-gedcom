package com.neo4j.data.importer.extractors;

import java.util.Locale;
import java.util.function.Supplier;
import org.folg.gedcom.model.Gedcom;
import org.folg.gedcom.model.Person;

public class PersonExtractors implements Supplier<AttributeExtractor<Person>> {

    private final String generatorName;

    public PersonExtractors(Gedcom model) {
        this.generatorName = model.getHeader().getGenerator().getName().toLowerCase(Locale.ROOT);
    }

    @Override
    public AttributeExtractor<Person> get() {
        var defaultExtractor = new DefaultPersonExtractor();
        if ("heredis pc".equals(generatorName)) {
            return new HeredisPersonExtractor(defaultExtractor);
        }
        return defaultExtractor;
    }
}
