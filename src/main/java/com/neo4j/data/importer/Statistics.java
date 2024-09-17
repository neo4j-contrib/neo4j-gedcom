package com.neo4j.data.importer;

public class Statistics {

    public Long nodesCreated = 0L;
    public Long relationshipsCreated = 0L;

    public void addNodesCreated(Long nodesCreated) {
        this.nodesCreated += nodesCreated;
    }

    public void addRelationshipsCreated(Long relationshipsCreated) {
        this.relationshipsCreated += relationshipsCreated;
    }
}
