package com.neo4j.data.importer.extractors;

import org.gedcomx.conclusion.NamePart;
import org.gedcomx.conclusion.Person;
import org.gedcomx.types.FactType;
import org.gedcomx.types.NamePartType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PersonExtractor {

    private final Person person;

    public PersonExtractor(Person person) {
        this.person = person;
    }

    public Map<String, Object> extract() {
        Map<String, Object> attributes = new HashMap<>();

        var id = person.getId();
        attributes.put("id", id);

        var firstNames = extractNames(person, NamePartType.Given);
        Optional.ofNullable(firstNames).ifPresent(fN -> attributes.put("first_names", fN));

        var surnames = extractNames(person, NamePartType.Surname);
        Optional.ofNullable(surnames).ifPresent(sN -> attributes.put("last_names", sN));

        var sex = extractSex(person);
        Optional.ofNullable(sex).ifPresent(sS -> attributes.put("sex", sS));

        var birthFact = person.getFirstFactOfType(FactType.Birth);
        if (birthFact != null) {
            var birthDate = birthFact.getDate();
            var birthLocation = birthFact.getPlace();

            Optional.ofNullable(birthDate).ifPresent(b -> attributes.put("birth_date", b.toString()));
            Optional.ofNullable(birthLocation).ifPresent(b -> attributes.put("birth_location", b.toString()));
        }

        return attributes;
    }

    private static String extractSex(Person person) {
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
                .toList();
    }

}
