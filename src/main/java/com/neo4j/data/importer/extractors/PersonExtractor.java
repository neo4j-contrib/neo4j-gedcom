package com.neo4j.data.importer.extractors;

import com.joestelmach.natty.Parser;
import org.gedcomx.conclusion.NamePart;
import org.gedcomx.conclusion.Person;
import org.gedcomx.types.FactType;
import org.gedcomx.types.NamePartType;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

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

        var firstNames = extractNames(person, NamePartType.Given);
        Optional.ofNullable(firstNames).ifPresent(fN -> attributes.put("first_names", fN));

        var surnames = extractNames(person, NamePartType.Surname);
        Optional.ofNullable(surnames).ifPresent(sN -> attributes.put("last_names", sN));

        var sex = extractGender(person);
        Optional.ofNullable(sex).ifPresent(g -> attributes.put("gender", g));

        Arrays.stream(FactType.values()).forEach( fact -> extractFact(fact, attributes));

        return attributes;
    }

    private void extractFact(FactType factType, Map<String, Object> attributes) {
        var fact = person.getFirstFactOfType(factType);
        if (fact == null) {
            return;
        }

        if (fact.getDate() != null) {
            var date = fact.getDate().getOriginal();
            LocalDate localDate = parseLocalDate(date);

            Optional.ofNullable(localDate).ifPresent(d -> attributes.put(factType.name().toLowerCase(Locale.ROOT) + "_" + "date", d));
        }
        if (fact.getPlace() != null) {
            var location = fact.getPlace().getOriginal();
            Optional.ofNullable(location).ifPresent(d -> attributes.put(factType.name().toLowerCase(Locale.ROOT) + "_" + "location", d));
        }
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

    private static String extractGender(Person person) {
        var gender = person.getGender();
        if (gender == null || gender.getKnownType() == null) return null;
        return gender.getKnownType().toString();
    }

    private static List<String> extractNames(Person person, NamePartType namePartType) {
        return person.getNames().stream()
                .filter(n -> n.getNameForms() != null)
                .flatMap(n -> n.getNameForms().stream())
                .filter(n -> n.getParts() != null)
                .flatMap(nf -> nf.getParts().stream())
                .filter(n -> n.getType() != null && n.getType().equals(namePartType.toQNameURI()))
                .map(NamePart::getValue)
                .map(name -> {
                    if (name.startsWith("\"") && name.endsWith("\"")){
                        return name.substring(1, name.length()-1);
                    }
                    return name;
                })
                .toList();
    }

}
