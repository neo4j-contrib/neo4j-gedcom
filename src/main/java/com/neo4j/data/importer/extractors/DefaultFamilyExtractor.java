package com.neo4j.data.importer.extractors;

import com.neo4j.data.importer.extractors.Lists.Pair;
import java.util.List;
import org.folg.gedcom.model.Family;
import org.folg.gedcom.model.SpouseRef;

class DefaultFamilyExtractor implements FamilyExtractor {

    @Override
    public List<Pair<String, String>> spouseReferences(Family family) {
        List<String> spouseReferences1 =
                family.getHusbandRefs().stream().map(SpouseRef::getRef).toList();
        List<String> spouseReferences2 =
                family.getWifeRefs().stream().map(SpouseRef::getRef).toList();
        return Lists.crossProduct(spouseReferences1, spouseReferences2);
    }

    @Override
    public List<String> childReferences(Family family) {
        return family.getChildRefs().stream().map(SpouseRef::getRef).toList();
    }
}
