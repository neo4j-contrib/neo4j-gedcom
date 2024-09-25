package com.neo4j.data.importer.extractors;

import com.neo4j.data.importer.extractors.Lists.Pair;
import java.util.List;
import java.util.Map;
import org.folg.gedcom.model.Family;

public interface FamilyExtractor extends AttributeExtractor<Family> {

    List<Pair<String, String>> spouseReferences(Family family);

    Map<String, List<Map<String, Object>>> familyEvents(Family family);

    List<String> childReferences(Family family);

    default Map<String, Object> apply(Family family) {
        var familyEvents = familyEvents(family);
        var spouseInfo = spouseReferences(family).stream()
                .map(couple -> Map.of(
                        "id1", couple.left(),
                        "id2", couple.right(),
                        "events", familyEvents))
                .toList();
        return Map.of("spouseIdPairs", spouseInfo, "childIds", childReferences(family));
    }
}
