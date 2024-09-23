package com.neo4j.data.importer.extractors;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Stream;
import org.folg.gedcom.model.Name;
import org.folg.gedcom.model.Person;

class PersonNames {

    public static Stream<String> extract(Person person, Function<Name, String> nameFn) {
        return person.getNames().stream()
                .flatMap(personName -> {
                    var names = nameFn.apply(personName);
                    if (names == null) {
                        return Stream.empty();
                    }
                    return Arrays.stream(names.split(","));
                })
                .map(String::trim);
    }

    public static String unquote(String name) {
        if (isQuoted(name)) {
            return name.substring(1, name.length() - 1);
        }
        return name;
    }

    public static boolean isQuoted(String name) {
        return name.startsWith("\"") && name.endsWith("\"");
    }
}
