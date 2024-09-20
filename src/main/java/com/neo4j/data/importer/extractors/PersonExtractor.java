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
import java.util.stream.Stream;
import org.folg.gedcom.model.EventFact;
import org.folg.gedcom.model.Name;
import org.folg.gedcom.model.Person;

public class PersonExtractor {

    private final Parser dateParser;
    private final Person person;

    public PersonExtractor(Person person) {
        this.person = person;
        this.dateParser = new Parser();
    }

    public Map<String, Object> extract() {
        Map<String, Object> attributes = new HashMap<>();

        var id = person.getId();
        attributes.put("id", id);

        var firstNames = extractNames(person, Name::getGiven);
        if (!firstNames.isEmpty()) {
            attributes.put("first_names", firstNames);
        }

        var surnames = extractNames(person, Name::getSurname);
        surnames.addAll(extractNames(person, Name::getMarriedName));
        if (!surnames.isEmpty()) {
            attributes.put("last_names", surnames);
        }

        extractGender(person).ifPresent(g -> attributes.put("gender", g));

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
            // Inferred dates are likely to be set using current time.
            return null;
        }

        var parsedDate = dateGroup.getDates().get(0);

        return LocalDate.ofInstant(parsedDate.toInstant(), ZoneId.systemDefault());
    }

    private static Optional<String> extractGender(Person person) {
        return person.getEventsFacts().stream()
                .filter(eventFact -> eventFact.getTag().equals("SEX"))
                .map(EventFact::getValue)
                .findFirst();
    }

    private static List<String> extractNames(Person person, Function<Name, String> nameFn) {
        return person.getNames().stream()
                .flatMap(personName -> {
                    String name = nameFn.apply(personName);
                    if (name == null) {
                        return Stream.empty();
                    }
                    if (name.startsWith("\"") && name.endsWith("\"")) {
                        return Stream.of(name.substring(1, name.length() - 1));
                    }
                    return Stream.of(name);
                })
                .collect(Collectors.toCollection(ArrayList::new));
    }
}
