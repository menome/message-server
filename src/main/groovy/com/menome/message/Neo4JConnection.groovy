package com.menome.message

import groovy.transform.CompileStatic
import com.menome.util.Cypher

/**
 * A relationship between two nodes that are matched on their conformed dimensions. Both nodes are merged before
 * connections are written, so the relationship statement only needs to MATCH them.
 */
@CompileStatic
class Neo4JConnection {
    String relationshipName
    Neo4JNode source
    Neo4JNode destination
    boolean forwardRelationship
    Map<String, Object> properties = [:]
    Map<String, String> dateProperties = [:]
    List<String> deleteProperties = []
    boolean deleteRelationship = false
    String sourceMessage
    int sourceIndex = -1

    // Cached on first use, like Neo4JNode's: a connection must not change once it has been grouped
    private String cachedSignature
    private List<Object> cachedIdentity

    /** Connections with equal signatures can share one Cypher statement. Direction is part of it. Cached and interned. */
    String signature() {
        if (cachedSignature == null) {
            cachedSignature = [deleteRelationship ? "delete" : "merge", relationshipName, forwardRelationship,
                               source.nodeType, source.dimensionKeys(), destination.nodeType, destination.dimensionKeys(),
                               (properties ?: [:]).keySet().sort(false), (dateProperties ?: [:]).keySet().sort(false), (deleteProperties ?: []).sort(false)].join("|").intern()
        }
        cachedSignature
    }

    /** Identifies the connection within a batch (last write wins). Cached. */
    List<Object> identity() {
        if (cachedIdentity == null) cachedIdentity = [signature(), source.identity(), destination.identity()]
        cachedIdentity
    }

    Map<String, Object> parametersAsMap() {
        Map<String, Object> params = [:]
        source.conformedDimensions.each { k, v -> params.put("s:" + k, v) }
        destination.conformedDimensions.each { k, v -> params.put("d:" + k, v) }
        (properties ?: [:]).each { k, v -> params.put("r:" + k, v) }
        (dateProperties ?: [:]).each { k, v -> params.put("r:" + k, v) }
        params
    }

    String toCypher() {
        String cypher = "MATCH (s:${Cypher.id(source.nodeType)} {${match(source, 's:')}}) " +
                "MATCH (d:${Cypher.id(destination.nodeType)} {${match(destination, 'd:')}}) "
        String left = forwardRelationship ? "-" : "<-"
        String right = forwardRelationship ? "->" : "-"
        String pattern = "(s)${left}[r:${Cypher.id(relationshipName)}]${right}(d)"
        if (deleteRelationship) {
            return cypher + "MATCH ${pattern} DELETE r"
        }
        cypher += "MERGE ${pattern}"
        List<String> sets = (properties ?: [:]).keySet().sort(false).collect { String key -> ("r.${Cypher.id(key)} = ${Cypher.param('r:' + key)}").toString() }
        sets.addAll((dateProperties ?: [:]).keySet().sort(false).collect { String key -> ("r.${Cypher.id(key)} = datetime(${Cypher.param('r:' + key)})").toString() })
        if (sets) cypher += " SET " + sets.join(", ")
        if (deleteProperties) cypher += " REMOVE " + deleteProperties.sort(false).collect { String key -> ("r.${Cypher.id(key)}").toString() }.join(", ")
        cypher
    }

    private static String match(Neo4JNode node, String prefix) {
        node.dimensionKeys().collect { String key -> ("${Cypher.id(key)}: ${Cypher.param(prefix + key)}").toString() }.join(", ")
    }

    @Override
    boolean equals(Object o) {
        if (this.is(o)) return true
        if (getClass() != o?.getClass()) return false
        Neo4JConnection that = (Neo4JConnection) o
        forwardRelationship == that.forwardRelationship && relationshipName == that.relationshipName &&
                source == that.source && destination == that.destination
    }

    @Override
    int hashCode() {
        Objects.hash(relationshipName, source, destination, forwardRelationship)
    }

    @Override
    String toString() {
        "Neo4JConnection{relationshipName='$relationshipName', source=$source, destination=$destination, forward=$forwardRelationship}"
    }
}
