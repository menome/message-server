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
            def parser = new JsonSlurper()
            def msgMap = parser.parseText(msg) as Map
            return processMerges(msgMap)
        }
    }

    static List<String> processIndexes(String msg) {
        if (msg) {
            def parser = new JsonSlurper()
            def msgMap = parser.parseText(msg) as Map
            return processIndexes(msgMap)
        }
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
    //MERGE (node:Card:Employee {Email: "konrad.aust@menome.com",EmployeeId: 12345})ON CREATE SET node.Uuid = {newUuid}SET node += {nodeParams}SET node.TheLinkAddedDate = datetime();
    /* nodeParams:
        {
            Email: 'konrad.aust@menome.com',
            EmployeeId: 12345,
            Name: 'Konrad Aust',
            PendingMerge: false,
            SourceSystems: [ 'HRSystem' ],
            SourceSystemPriorities: [ 1 ],
            SourceSystemProps_HRSystem: []
        }
    */

    def static List<String> processMerges(Map msgMap) {
        def mergeStatements = []
        // Card Merge MERGE (node:Card:Employee {Email: "konrad.aust@menome.com",EmployeeId: 12345})
        def cardMerge = "MERGE (node:Card:$msgMap.NodeType "

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

        cardMerge += " ON CREATE SET node.Uuid = {newUuid}"
        cardMerge += " SET node += {nodeParams}"
        cardMerge += " SET node.TheLinkAddedDate = datetime()"
        mergeStatements << cardMerge
    }
}
