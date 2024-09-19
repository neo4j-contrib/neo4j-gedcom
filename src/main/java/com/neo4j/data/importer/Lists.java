package com.neo4j.data.importer;

import java.util.List;

class Lists {

    public static <LEFT, RIGHT> List<Pair<LEFT, RIGHT>> crossProduct(List<LEFT> left, List<RIGHT> right) {
        return left.stream()
                .flatMap(leftValue -> right.stream()
                        .map(rightValue -> new Pair<>(leftValue, rightValue)))
                .toList();
    }

    public record Pair<T, U>(T left, U right) {
    }
}
