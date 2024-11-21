package com.neo4j.data.importer.extractors;

import com.joestelmach.natty.Parser;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import org.folg.gedcom.model.EventFact;

class EventFacts {

    /**
     * extractFlat extracts all events' place and location into a single, "flat" map
     */
    public static Map<String, Object> extractFlat(List<EventFact> facts, Parser dateParser) {
        var attributes = new HashMap<String, Object>();
        facts.forEach(fact -> {
            attributes.putAll(extractFact(
                    fact,
                    dateParser,
                    (eventFact) ->
                            String.format("%s_", eventFact.getDisplayType().toLowerCase(Locale.ROOT))));
        });
        return attributes;
    }

    /**
     * extract all events' place and location, categorized by event tag
     */
    public static Map<String, List<Map<String, Object>>> extract(List<EventFact> facts, Parser dateParser) {
        var attributes = new HashMap<String, List<Map<String, Object>>>();
        for (EventFact fact : facts) {
            var eventsPerTag =
                    attributes.computeIfAbsent(fact.getTag().toUpperCase(Locale.ROOT), (key) -> new ArrayList<>());
            eventsPerTag.add(extractFact(fact, dateParser));
        }
        return attributes;
    }

    private static Map<String, Object> extractFact(EventFact eventFact, Parser dateParser) {
        return extractFact(eventFact, dateParser, (fact) -> "");
    }

    private static Map<String, Object> extractFact(
            EventFact fact, Parser dateParser, Function<EventFact, String> keyQualifierFn) {
        var attributes = new HashMap<String, Object>(2);
        String date = fact.getDate();
        String keyQualifier = keyQualifierFn.apply(fact);
        String type = fact.getType();
        if (type != null) {
            attributes.put(String.format("%stype", keyQualifier), type);
        }
        if (date != null) {
            attributes.put(String.format("raw_%sdate", keyQualifier), date);
            var localDate = parseLocalDate(dateParser, date);
            if (localDate != null) {
                attributes.put(String.format("%sdate", keyQualifier), localDate);
            }
        }
        String place = fact.getPlace();
        if (place != null) {
            attributes.put(String.format("%slocation", keyQualifier), place);
        }
        return attributes;
    }

    private static LocalDate parseLocalDate(Parser dateParser, String date) {
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
}
