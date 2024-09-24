package com.neo4j.data.importer.extractors;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.folg.gedcom.model.Name;
import org.folg.gedcom.model.Person;

class HeredisPersonExtractor implements PersonExtractor {

    private final PersonExtractor delegate;

    public HeredisPersonExtractor(PersonExtractor delegate) {
        this.delegate = delegate;
    }

    @Override
    public String id(Person person) {
        return delegate.id(person);
    }

    @Override
    public List<String> firstNames(Person person) {
        return delegate.firstNames(person);
    }

    @Override
    public List<String> surnames(Person person) {
        return delegate.surnames(person);
    }

    @Override
    public Optional<String> gender(Person person) {
        return delegate.gender(person);
    }

    @Override
    public Map<String, Object> facts(Person person) {
        return delegate.facts(person);
    }

    @Override
    public Optional<String> preferredFirstName(Person person) {
        var preferredNames = PersonNames.extract(person, Name::getGiven)
                .filter(PersonNames::isQuoted)
                .map(PersonNames::unquote)
                .toList();

        if (preferredNames.size() != 1) {
            // TODO: log warning if more than 1
            return Optional.empty();
        }
        return Optional.of(preferredNames.get(0));
    }
}
