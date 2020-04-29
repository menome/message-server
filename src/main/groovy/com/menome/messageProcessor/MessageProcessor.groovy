package com.menome.messageProcessor

import groovy.json.JsonSlurper
import org.apache.commons.lang3.math.NumberUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class MessageProcessor {

    static Logger log = LoggerFactory.getLogger(MessageProcessor.class)

    static List<String> process(String msg) {
        List<String> statements = []
        if (msg) {
            //statements.addAll(processIndexes(msg))
            statements.addAll(processMerges(msg))
            statements.addAll(processConnectionNodes(msg))
            statements.addAll(processConnectionRelationships(msg))
        }
         statements
    }

    static List<String> processMerges(String msg) {
        if (msg) {
            return processMerges(buildJSonParserfromMessage(msg), NodeType.PRIMARY)
        }
    }


    static List<String> processIndexes(String msg) {
        if (msg) {
            processIndexes(buildJSonParserfromMessage(msg))
        }
    }

    static List<String> processConnectionNodes(String msg) {
        if (msg) {
            processConnectionNodes(buildJSonParserfromMessage(msg))
        }
    }

    static List<String> processConnectionRelationships(String msg) {
        if (msg) {
            processConnectionRelationships(buildJSonParserfromMessage(msg))
        }
    }

    private static Map buildJSonParserfromMessage(String msg) {
        def parser = new JsonSlurper()
        def msgMap = parser.parseText(msg) as Map
        msgMap
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

    /*
    MERGE (employee:Card:Employee {Email: "konrad.aust2@menome.com",EmployeeId: 12345}) ON CREATE SET employee.Uuid = apoc.create.uuid(),employee.TheLinkAddedDate = datetime(), employee.Name= "Konrad Aust",employee.Priority= 1,employee.SourceSystem= "HRSystem",employee.Status= "active",employee.PreferredName= "The Chazzinator",employee.ResumeSkills= "programming,peeling bananas from the wrong end,handstands,sweet kickflips" ON MATCH SET employee.Name= "Konrad Aust",employee.Priority= 1,employee.SourceSystem= "HRSystem",employee.Status= "active",employee.PreferredName= "The Chazzinator",employee.ResumeSkills= "programming,peeling bananas from the wrong end,handstands,sweet kickflips" with employee
    MATCH (office:Office {City: "Victoria"}) WITH employee,office
    MATCH (project:Project {Code: 5}) WITH employee,office,project
    MATCH (team:Team {Code: 1337}) WITH employee,office,project,team
    MERGE (employee)-[office_rel:LocatedInOffice]->(office)
    MERGE (employee)-[project_rel:WorkedOnProject]->(project)
    MERGE (employee)-[team_rel:HAS_FACET]->(team)
     */

    static List<String> processMerges(Map msgMap, NodeType nodeType) {
        def mergeStatements = []
        // Card Merge MERGE (employee:Card:Employee {Email: "konrad.aust@menome.com",EmployeeId: 12345})
        String msgNodeType = msgMap.NodeType
        String nodeName = msgNodeType.toLowerCase()
        def cardMerge = "MERGE (${nodeName}:Card:$msgNodeType "

        if(nodeType == NodeType.RELATED){
            def conformedDimensions = msgMap.ConformedDimensions
            cardMerge = "MATCH (${nodeName}:$msgNodeType "
            cardMerge += "{" + buildMergeExpressionFromMap(conformedDimensions, "", ":") + "})"
            cardMerge += " WITH ${nodeName} "
        }

        if (nodeType == NodeType.PRIMARY) {
            def conformedDimensions = msgMap.ConformedDimensions
            if (conformedDimensions) {
                cardMerge += "{" + buildMergeExpressionFromMap(conformedDimensions, "", ":") + "})"
            }

            cardMerge += " ON CREATE SET ${nodeName}.Uuid = apoc.create.uuid(),${nodeName}.TheLinkAddedDate = datetime()"

            if (nodeType == NodeType.PRIMARY) {
                Map keysToProcess = [:]
                keysToProcess.putAll(msgMap)
                keysToProcess.putAll(msgMap.Properties ?: [:])
                keysToProcess.remove("ConformedDimensions")
                keysToProcess.remove("Connections")
                keysToProcess.remove("NodeType")
                keysToProcess.remove("Properties")
                cardMerge += ", "
                def mergeExpression = buildMergeExpressionFromMap(keysToProcess, nodeName + ".", "=")
                cardMerge += mergeExpression
                cardMerge += " ON MATCH SET " + mergeExpression
                cardMerge += " WITH ${nodeName}"

            } else {
                cardMerge += ", ${nodeName}.Name = \"${msgMap.Name}\" , ${nodeName}.PendingMerge = true"
            }
        }
        mergeStatements << cardMerge
    }

    private static String buildMergeExpressionFromMap(msgMap, valuePrefix, keySeparator) {
        String expression = ""
        msgMap.eachWithIndex { key, value, index ->
            boolean valueIsNumeric = NumberUtils.isCreatable(value as String)
            String valueDelimiter = ""
            if (!valueIsNumeric) {
                valueDelimiter = "\""
            }
            expression += "$valuePrefix$key$keySeparator $valueDelimiter$value$valueDelimiter" + (index < msgMap.size() - 1 ? "," : "")
        }
        expression
    }

    static List<String> processConnectionNodes(Map msgMap) {
        List<String> connectedNodeMergeStatements = []
        msgMap.Connections.each { Map map ->
            connectedNodeMergeStatements.addAll(processMerges(map, NodeType.RELATED))
        }
        connectedNodeMergeStatements
    }

    static List<String> processConnectionRelationships(Map msgMap) {
        List<String> connectionRelationshipStatements = []
        String msgNodeType = msgMap.NodeType
        String nodeName = msgNodeType.toLowerCase()

        msgMap.Connections.each { Map map ->
            String relationshipNodeType = map.NodeType
            String relationshipNodeName = relationshipNodeType.toLowerCase()
            String relType = map.RelType
            def relationshipMerge = "MERGE ($nodeName)-[${relationshipNodeName}_rel:${relType}]->($relationshipNodeName)"
            connectionRelationshipStatements.add(relationshipMerge)
        }
        connectionRelationshipStatements
    }
}

enum NodeType {
    PRIMARY, RELATED
}

