package com.menome.message

import org.slf4j.Logger
import org.slf4j.LoggerFactory

class Neo4JNode {
    String nodeType
    String name
    Integer priority
    String sourceSystem
    Map<String, Object> conformedDimensions
    Map<String, Object> properties

    static Logger log = LoggerFactory.getLogger(Neo4JNode.class)

    def conformedDimensionNeo4JSyntax(String prefix) {
        String expression = ""
        conformedDimensions.eachWithIndex { key, value, index ->
            expression += "$prefix.$key = param.$prefix$key" + (index < conformedDimensions.size() - 1 ? " AND " : "")
        }
        expression
    }

    def conformedDimensionNeo4JSyntax() {
        conformedDimensionNeo4JSyntax("")
    }

    String toCypher() {
        String msgNodeType = this.nodeType
        String nodeName = msgNodeType.toLowerCase()
        def cypher = "MERGE (${nodeName}:Card:$msgNodeType "

        def conformedDimensions = this.conformedDimensions
        if (conformedDimensions) {
            cypher += "{" + buildMergeExpressionFromMap(conformedDimensions, "", ":") + "})"
        }

        cypher += " ON CREATE SET ${nodeName}.Uuid = apoc.create.uuid(),${nodeName}.TheLinkAddedDate = datetime()"

        Map keysToProcess = flattenMessageMap(false)
        if (keysToProcess) {
            cypher += ", "
            def mergeExpression = buildMergeExpressionFromMap(keysToProcess, nodeName + ".", "=")
            cypher += mergeExpression
            cypher += " ON MATCH SET " + mergeExpression
        }
        if (log.isDebugEnabled()) {
            log.debug(cypher)
        }
        cypher

    }

    static String buildMergeExpressionFromMap(msgMap, valuePrefix, keySeparator) {
        String expression = ""
        msgMap.eachWithIndex { key, value, index ->
            expression += "$valuePrefix$key$keySeparator param.$key" + (index < msgMap.size() - 1 ? "," : "")
        }
        expression
    }

    List<String> indexes() {
        def indexStatements = []
        def conformedDimensions = this.conformedDimensions
        def keys = (conformedDimensions ?: [:]).keySet().join(",")
        if (keys) {
            def nodeType = this.nodeType
            def nodeIndex = "CREATE INDEX ON :$nodeType($keys)"
            def cardIndex = "CREATE INDEX ON :Card($keys)"
            indexStatements.add(nodeIndex)
            indexStatements.add(cardIndex)
        }
        indexStatements
    }

    private HashMap flattenMessageMap(boolean includeConformedDimensions) {
        def flattenedMap = new HashMap()
        if (this.name) {
            flattenedMap.put("Name", this.name)
        }
        if (this.priority) {
            flattenedMap.put("Priority", this.priority)
        }
        if (this.sourceSystem) {
            flattenedMap.put("SourceSystem", this.sourceSystem)
        }
        flattenedMap.putAll(this.properties as Map ?: [:])
        if (includeConformedDimensions) {
            flattenedMap.putAll(this.conformedDimensions as Map ?: [:])
        }
        flattenedMap.remove("ConformedDimensions")
        flattenedMap.remove("Connections")
        flattenedMap.remove("NodeType")
        flattenedMap.remove("Properties")
        flattenedMap.remove("RelType")
        flattenedMap.remove("ForwardRel")


        flattenedMap
    }

    Map<String, Object> parametersAsMap() {
        flattenMessageMap(true)
    }

    boolean equals(o) {
        if (this.is(o)) return true
        if (getClass() != o.class) return false

        Neo4JNode node = (Neo4JNode) o

        if (conformedDimensions != node.conformedDimensions) return false
        if (nodeType != node.nodeType) return false

        return true
    }

    int hashCode() {
        int result
        result = (nodeType != null ? nodeType.hashCode() : 0)
        result = 31 * result + (conformedDimensions != null ? conformedDimensions.hashCode() : 0)
        return result
    }


    @Override
    public String toString() {
        return "Neo4JNode{" +
                "nodeType='" + nodeType + '\'' +
                ", name='" + name + '\'' +
                ", priority=" + priority +
                ", sourceSystem='" + sourceSystem + '\'' +
                ", conformedDimensions=" + conformedDimensions +
                ", properties=" + properties +
                '}';
    }
}
