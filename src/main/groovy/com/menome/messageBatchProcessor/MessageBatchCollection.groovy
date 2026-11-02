package com.menome.messageBatchProcessor

import groovy.transform.CompileStatic
import com.menome.message.Neo4JNode

/**
 * Nodes of one signature (same label, dimension keys and property keys), so one Cypher statement fits them all.
 * Within a batch the last message for a given node wins.
 */
@CompileStatic
class MessageBatchCollection {

    final String signature
    final LinkedHashMap<List<Object>, Neo4JNode> nodes = [:]

    MessageBatchCollection(String signature) {
        this.signature = signature
    }

    void addNode(Neo4JNode node) {
        if (node.signature() != signature) {
            throw new IllegalArgumentException("Cannot add node ${node} to collection with signature ${signature}")
        }
        nodes.put(node.identity(), node)
    }

    Neo4JNode prototypeNode() {
        nodes.values().first()
    }

    List<Neo4JNode> nodeList() {
        new ArrayList<>(nodes.values())
    }

    static Map<String, List<Map<String, Object>>> asParameterList(List<Neo4JNode> nodes) {
        ["parms": nodes.collect { Neo4JNode node -> node.parametersAsMap() }]
    }

    /** Splits in two without losing elements, whatever the size. */
    static <T> List<List<T>> split(List<T> items) {
        int half = items.size().intdiv(2)
        [items.subList(0, half), items.subList(half, items.size())]
    }

    @Override
    String toString() {
        "Signature:${signature}:${nodes.size()}"
    }
}
