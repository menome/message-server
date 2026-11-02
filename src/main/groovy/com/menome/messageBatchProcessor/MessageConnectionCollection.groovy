package com.menome.messageBatchProcessor

import groovy.transform.CompileStatic
import com.menome.message.Neo4JConnection

/** Connections of one signature: same relationship, direction, endpoint labels and dimension keys. */
@CompileStatic
class MessageConnectionCollection {

    final String signature
    final LinkedHashMap<List<Object>, Neo4JConnection> connections = [:]

    MessageConnectionCollection(String signature) {
        this.signature = signature
    }

    void addConnection(Neo4JConnection connection) {
        connections.put(connection.identity(), connection)
    }

    Neo4JConnection prototypeRelationship() {
        connections.values().first()
    }

    List<Neo4JConnection> connectionList() {
        new ArrayList<>(connections.values())
    }

    static Map<String, List<Map<String, Object>>> asParameterList(List<Neo4JConnection> connections) {
        ["parms": connections.collect { Neo4JConnection connection -> connection.parametersAsMap() }]
    }
}
