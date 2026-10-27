package com.neo4j.data.importer.extractors;

import com.joestelmach.natty.Parser;
import java.util.Locale;
import org.folg.gedcom.model.Gedcom;
import org.folg.gedcom.model.Person;

public class PersonExtractors {

    private PersonExtractors() {}

    public static AttributeExtractor<Person> personExtractor(Parser dateParser, Gedcom model) {
        var generatorName = model.getHeader().getGenerator().getName();
        var defaultExtractor = new DefaultPersonExtractor(dateParser);
        if (generatorName != null && "heredis pc".equals(generatorName.toLowerCase(Locale.ROOT))) {
            return new HeredisPersonExtractor(defaultExtractor);
        }
        return defaultExtractor;
    }
}
