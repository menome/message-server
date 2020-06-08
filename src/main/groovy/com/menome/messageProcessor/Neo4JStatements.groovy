package com.menome.messageProcessor

import groovy.transform.Canonical

@Canonical
class Neo4JStatements {

    List<String> primaryNodeMerge;
    List<String> indexes;
    List<String> connectionMerge
    List<String> connectionMatch

    Neo4JStatements(List<String> primaryNodeMerge, List<String> indexes, List<String> connectionMerge, List<String> connectionMatch) {
        this.primaryNodeMerge = primaryNodeMerge
        this.indexes = indexes
        this.connectionMerge = connectionMerge
        this.connectionMatch = connectionMatch
    }

}
