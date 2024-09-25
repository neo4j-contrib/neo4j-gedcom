package com.neo4j.data.importer.extractors;

import com.joestelmach.natty.Parser;
import com.neo4j.data.importer.extractors.Lists.Pair;
import java.util.List;
import java.util.Map;
import org.folg.gedcom.model.Family;
import org.folg.gedcom.model.SpouseRef;

class DefaultFamilyExtractor implements FamilyExtractor {

    private final Parser dateParser;

    DefaultFamilyExtractor(Parser dateParser) {
        this.dateParser = dateParser;
    }

    @Override
    public List<Pair<String, String>> spouseReferences(Family family) {
        List<String> spouseReferences1 =
                family.getHusbandRefs().stream().map(SpouseRef::getRef).toList();
        List<String> spouseReferences2 =
                family.getWifeRefs().stream().map(SpouseRef::getRef).toList();
        return Lists.crossProduct(spouseReferences1, spouseReferences2);
    }

    @Override
    public Map<String, List<Map<String, Object>>> familyEvents(Family family) {
        return EventFacts.extract(family.getEventsFacts(), dateParser);
    }

    @Override
    public List<String> childReferences(Family family) {
        return family.getChildRefs().stream().map(SpouseRef::getRef).toList();
    }
}
