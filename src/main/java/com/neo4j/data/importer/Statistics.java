package com.neo4j.data.importer;

public class Statistics {

    public Long nodesCreated = 0L;
    public Long relationshipsCreated = 0L;

    public void addNodesCreated(int nodesCreated) {
        this.nodesCreated += nodesCreated;
    }

    public void addRelationshipsCreated(int relationshipsCreated) {
        this.relationshipsCreated += relationshipsCreated;
    }
}
