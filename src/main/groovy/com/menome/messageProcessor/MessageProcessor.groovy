package com.menome.messageProcessor

import groovy.json.JsonSlurper
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class MessageProcessor {

    static Logger log = LoggerFactory.getLogger(MessageProcessor.class)

    def statements = []

    def process(String msg) {
        if (msg) {
            def parser = new JsonSlurper()
            def msgMap = parser.parseText(msg) as Map
            statements.addAll(processIndexes(msgMap))
            statements.addAll(processMerges(msgMap))
        }
    }

    List<String> getNeo4JStatements() {
        statements
    }

    List<String> processIndexes(Map msgMap) {
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

    def List<String> processMerges(Map msgMap) {
        def mergeStatements = []
        mergeStatements
    }
}
