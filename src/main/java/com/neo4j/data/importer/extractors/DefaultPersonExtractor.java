package com.neo4j.data.importer.extractors;

import com.joestelmach.natty.Parser;
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

    @Override
    public Map<String, Object> typedNames(Person person) {
        var names = person.getNames();
        if (names.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> result = new HashMap<>();

        // First 'name' entry is the preferred name
        var preferred = names.get(0);
        putIfPresent(result, "preferred_first_name", preferred.getGiven());
        putIfPresent(result, "preferred_last_name", preferred.getSurname());

        for (Name name : names) {
            var type = name.getType();
            if (type == null || type.isBlank()) {
                continue;
            }
            var normalizedType = type.trim().toLowerCase(Locale.ROOT);
            putIfPresent(result, normalizedType + "_first_name", name.getGiven());
            putIfPresent(result, normalizedType + "_last_name", name.getSurname());
        }

        return result;
    }

    private static void putIfPresent(Map<String, Object> map, String key, String value) {
        if (value != null && !value.isBlank()) {
            map.put(key, PersonNames.unquote(value.trim()));
        }
    }

    private static List<String> extractNames(Person person, Function<Name, String> nameFn) {
        return PersonNames.extract(person, nameFn)
                .map(PersonNames::unquote)
                .collect(Collectors.toCollection(ArrayList::new));
    }
}
