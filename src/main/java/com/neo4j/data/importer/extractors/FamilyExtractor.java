package com.neo4j.data.importer.extractors;

import com.neo4j.data.importer.Statistics;
import com.neo4j.data.importer.extractors.Lists.Pair;
import java.util.List;
import java.util.Map;
import org.folg.gedcom.model.Family;
import org.neo4j.graphdb.QueryStatistics;

public interface FamilyExtractor extends AttributeExtractor<Family> {

    List<Pair<String, String>> spouseReferences(Family family);

    Map<String, List<Map<String, Object>>> familyEvents(Family family);

    List<String> childReferences(Family family);

    default String query() {
        return """
                UNWIND $spouseIdPairs AS spouseInfo
                MATCH (spouse1:Person {id: spouseInfo.id1}),
                      (spouse2:Person {id: spouseInfo.id2})
                CREATE (spouse1)-[r:SPOUSE_OF]->(spouse2)
                FOREACH (marriageInfo IN spouseInfo.events["MARR"] |
                    CREATE (spouse1)-[r:MARRIED_TO]->(spouse2)
                    SET r = marriageInfo
                )
                FOREACH (divorceInfo IN spouseInfo.events["DIV"] |
                    CREATE (spouse1)-[r:DIVORCED]->(spouse2)
                    SET r = divorceInfo
                )
                WITH spouse1, spouse2
                UNWIND $childIds AS childId
                MATCH (child:Person {id: childId})
                CREATE (child)-[:CHILD_OF]->(spouse1)
                CREATE (child)-[:CHILD_OF]->(spouse2)
                """;
    }

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

    default void updateCounters(QueryStatistics results, Statistics counters) {
        counters.addRelationshipsCreated(results.getRelationshipsCreated());
    }
}
