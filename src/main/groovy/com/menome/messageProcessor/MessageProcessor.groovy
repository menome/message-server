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
            return processMerges(buildJSonParserfromMessage(msg),NodeType.PRIMARY)
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
        // Card Merge MERGE (employee:Card:Employee {Email: "konrad.aust@menome.com",EmployeeId: 12345})
        String msgNodeType = msgMap.NodeType
        String nodeName = msgNodeType.toLowerCase()
        def cardMerge = "MERGE (${nodeName}:Card:$msgNodeType "

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

        cardMerge += " ON CREATE SET ${nodeName}.Uuid = apoc.create.uuid(),${nodeName}.TheLinkAddedDate = datetime()"

        if (nodeType == NodeType.PRIMARY) {
            cardMerge += " SET ${nodeName} += {nodeParams}"
        } else {
            cardMerge += " SET ${nodeName}.PendingMerge = true"
        }
        mergeStatements << cardMerge
    }


    static List<String> processConnectedNodes(Map msgMap) {
        List<String> connectedNodeMergeStatements = []
        msgMap.Connections.eachWithIndex { Map map, Integer index ->
            connectedNodeMergeStatements.addAll(processMerges(map, NodeType.RELATED))
        }
        connectedNodeMergeStatements
    }

    /*

    MERGE (node0:Card:Office {City: "Victoria"})
ON CREATE SET node0.Uuid = {node0_newUuid}, node0.PendingMerge = true


    MERGE (node)-[node0_rel:LocatedInOffice]->(node0)
    SET node0_rel += {node0_relProps}, node0 += {node0_nodeParams}
     */


}

enum NodeType {
    PRIMARY, RELATED
}

