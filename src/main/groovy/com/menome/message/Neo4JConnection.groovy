package com.menome.message

import org.slf4j.Logger
import org.slf4j.LoggerFactory

class Neo4JConnection {
    String relationshipName
    Neo4JNode source
    Neo4JNode destination
    boolean forwardRelationship
    Map<String, Object> properties = [:]

    static Logger log = LoggerFactory.getLogger(Neo4JConnection.class)

    String toCypher() {
        def sourceDirection = forwardRelationship ? "-" : "<-"
        def destinationDirection = forwardRelationship ? "->" : "-"
        String cypher
        cypher = "MATCH (source:${this.source.nodeType}),(destination:${this.destination.nodeType}) "
        cypher += "WHERE ${this.source.conformedDimensionNeo4JSyntax("source")} "
        cypher += "AND ${this.destination.conformedDimensionNeo4JSyntax("destination")} "
        cypher += "MERGE (source)${sourceDirection}[r:" + this.relationshipName + "]${destinationDirection}(destination) "
        cypher += "SET r = param.Properties"

        if (log.isDebugEnabled()) {
            log.debug(cypher)
        }
        cypher
    }

    boolean equals(o) {
        if (this.is(o)) return true
        if (getClass() != o.class) return false

        Neo4JConnection that = (Neo4JConnection) o

        if (forwardRelationship != that.forwardRelationship) return false
        if (destination != that.destination) return false
        if (relationshipName != that.relationshipName) return false
        if (source != that.source) return false

        return true
    }

    int hashCode() {
        int result
        result = (relationshipName != null ? relationshipName.hashCode() : 0)
        result = 31 * result + (source != null ? source.hashCode() : 0)
        result = 31 * result + (destination != null ? destination.hashCode() : 0)
        result = 31 * result + (forwardRelationship ? 1 : 0)
        return result
    }
}
