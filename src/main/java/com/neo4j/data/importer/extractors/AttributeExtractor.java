package com.neo4j.data.importer.extractors;

import com.neo4j.data.importer.Statistics;
import java.util.Map;
import java.util.function.Function;
import org.neo4j.graphdb.QueryStatistics;

public interface AttributeExtractor<T> extends Function<T, Map<String, Object>> {
    String query();

    void updateCounters(QueryStatistics results, Statistics counters);
}
