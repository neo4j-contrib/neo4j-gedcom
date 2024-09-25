package com.neo4j.data.importer.extractors;

import com.joestelmach.natty.Parser;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.folg.gedcom.model.EventFact;
import org.folg.gedcom.model.Name;
import org.folg.gedcom.model.Person;

class DefaultPersonExtractor implements PersonExtractor {

    private final Parser dateParser;

    public DefaultPersonExtractor(Parser dateParser) {
        this.dateParser = dateParser;
    }

    @Override
    public String id(Person person) {
        return person.getId();
    }

    @Override
    public List<String> firstNames(Person person) {
        return extractNames(person, Name::getGiven);
    }

    @Override
    public List<String> surnames(Person person) {
        var surnames = extractNames(person, Name::getSurname);
        surnames.addAll(extractNames(person, Name::getMarriedName));
        return surnames;
    }

    @Override
    public Optional<String> gender(Person person) {
        return person.getEventsFacts().stream()
                .filter(eventFact -> "sex".equals(eventFact.getTag().toLowerCase(Locale.ROOT)))
                .map(EventFact::getValue)
                .findFirst();
    }

    @Override
    public Map<String, Object> facts(Person person) {
        return EventFacts.extractFlat(person.getEventsFacts(), dateParser);
    }

    private static List<String> extractNames(Person person, Function<Name, String> nameFn) {
        return PersonNames.extract(person, nameFn)
                .map(PersonNames::unquote)
                .collect(Collectors.toCollection(ArrayList::new));
    }
}
