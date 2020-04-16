package com.menome.messageProcessor

import groovy.json.JsonSlurper
import org.apache.commons.lang3.math.NumberUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class MessageProcessor {

    static Logger log = LoggerFactory.getLogger(MessageProcessor.class)
    static statements = []

    static process(String msg) {
        if (msg) {
            statements.addAll(processIndexes(msg))
            statements.addAll(processMerges(msg))
        }
    }

    static List<String> processMerges(String msg) {
        if (msg) {
            return processMerges(buildJSonParserfromMessage(msg), new NodeType("", NodeType.Type.PRIMARY))
        }
    }


    static List<String> processIndexes(String msg) {
        if (msg) {
            processIndexes(buildJSonParserfromMessage(msg))
        }
    }

    static List<String> processConnectedNodes(String msg) {
        if (msg) {
            processConnectedNodes(buildJSonParserfromMessage(msg))
        }

    }


    private static Map buildJSonParserfromMessage(String msg) {
        def parser = new JsonSlurper()
        def msgMap = parser.parseText(msg) as Map
        msgMap
    }

    static List<String> getNeo4JStatements() {
        statements
    }

    static List<String> processIndexes(Map msgMap) {
        def indexStatements = []
        def conformedDimensions = (Map) msgMap.ConformedDimensions
        def keys = (conformedDimensions ?: [:]).keySet().join(",")
        if (msgMap.NodeType && keys) {
            def nodeType = msgMap.NodeType
            def nodeIndex = "CREATE INDEX ON :$nodeType($keys)"
            def cardIndex = "CREATE INDEX ON :Card($keys)"
            indexStatements.add(nodeIndex)
            indexStatements.add(cardIndex)
        }
        indexStatements
    }

    static List<String> processMerges(Map msgMap, NodeType nodeType) {
        def mergeStatements = []
        // Card Merge MERGE (node:Card:Employee {Email: "konrad.aust@menome.com",EmployeeId: 12345})
        def cardMerge = "MERGE (node${nodeType.nodeSuffix}:Card:$msgMap.NodeType "

        def conformedDimensions = msgMap.ConformedDimensions
        if (conformedDimensions) {
            cardMerge += "{"
            conformedDimensions.eachWithIndex { key, value, index ->
                boolean valueIsNumeric = NumberUtils.isCreatable(value as String)
                String valueDelimiter = ""
                if (!valueIsNumeric) {
                    valueDelimiter = "\""
                }

                cardMerge += "$key: $valueDelimiter$value$valueDelimiter" + (index < conformedDimensions.size() - 1 ? "," : "")
            }
            cardMerge += "})"
        }

        cardMerge += " ON CREATE SET node${nodeType.nodeSuffix}.Uuid = apoc.create.uuid(),node${nodeType.nodeSuffix}.TheLinkAddedDate = datetime()"

        if (nodeType.nodeType == NodeType.Type.PRIMARY) {
            cardMerge += " SET node${nodeType.nodeSuffix} += {nodeParams}"
        } else {
            cardMerge += " SET node${nodeType.nodeSuffix}.PendingMerge = true"
        }
        mergeStatements << cardMerge
    }


    static List<String> processConnectedNodes(Map msgMap) {
        List<String> connectedNodeMergeStatements = []
        msgMap.Connections.eachWithIndex { Map map, Integer index ->
            connectedNodeMergeStatements.addAll(processMerges(map, new NodeType(index as String, NodeType.Type.RELATED)))
        }
        connectedNodeMergeStatements
    }
}

class NodeType {
    String nodeSuffix
    enum Type {
        PRIMARY, RELATED
    }
    Type nodeType

    NodeType(String nodeSuffix, NodeType.Type type) {
        this.nodeSuffix = nodeSuffix
        this.nodeType = type
    }
}
