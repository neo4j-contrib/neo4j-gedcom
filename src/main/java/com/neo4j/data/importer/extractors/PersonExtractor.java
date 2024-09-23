package com.neo4j.data.importer.extractors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.folg.gedcom.model.Person;

interface PersonExtractor extends AttributeExtractor<Person> {

    String id(Person person);

    List<String> firstNames(Person person);

    List<String> surnames(Person person);

    Optional<String> gender(Person person);

    Map<String, Object> facts(Person person);

    default Optional<String> preferredFirstName(Person person) {
        return Optional.empty();
    }

    default Map<String, Object> apply(Person person) {
        Map<String, Object> attributes = new HashMap<>(facts(person));
        attributes.put("id", id(person));
        attributes.put("first_names", firstNames(person));
        attributes.put("last_names", surnames(person));
        gender(person).ifPresent(gender -> attributes.put("gender", gender));
        preferredFirstName(person).ifPresent(gender -> attributes.put("preferred_first_name", gender));
        return attributes;
    }
}
