package com.neo4j.data.importer.extractors;

import java.util.Map;
import java.util.function.Function;

public interface AttributeExtractor<T> extends Function<T, Map<String, Object>> {}
