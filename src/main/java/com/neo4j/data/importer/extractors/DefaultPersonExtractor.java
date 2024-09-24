package com.neo4j.data.importer.extractors;

import com.joestelmach.natty.Parser;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
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

    public DefaultPersonExtractor() {
        this.dateParser = new Parser();
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
        Map<String, Object> attributes = new HashMap<>();
        person.getEventsFacts().forEach(eventFact -> {
            String factName = eventFact.getDisplayType().toLowerCase(Locale.ROOT);
            String date = eventFact.getDate();
            if (date != null) {
                attributes.put(String.format("raw_%s_date", factName), date);
                var localDate = parseLocalDate(date);
                if (localDate != null) {
                    attributes.put(String.format("%s_date", factName), localDate);
                }
            }

            String place = eventFact.getPlace();
            if (place != null) {
                attributes.put(factName + "_" + "location", place);
            }
        });
        return attributes;
    }

    private LocalDate parseLocalDate(String date) {
        var parse = dateParser.parse(date);
        if (parse.size() != 1) {
            return null;
        }

        var dateGroup = parse.get(0);
        if (dateGroup.getDates().size() != 1 || dateGroup.isDateInferred()) {
            // Dates should be parsed explicitly from input.
            // Inferred dates are likely to be set using current time and therefore incorrect.
            return null;
        }

        var parsedDate = dateGroup.getDates().get(0);

        return LocalDate.ofInstant(parsedDate.toInstant(), ZoneId.systemDefault());
    }

    private static List<String> extractNames(Person person, Function<Name, String> nameFn) {
        return PersonNames.extract(person, nameFn)
                .map(PersonNames::unquote)
                .collect(Collectors.toCollection(ArrayList::new));
    }
}
