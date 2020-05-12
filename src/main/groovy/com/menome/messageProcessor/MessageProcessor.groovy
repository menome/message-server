package com.menome.messageProcessor

import com.google.gson.Gson
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class MessageProcessor {

    enum StatementType {
        PRIMARY_NODE_MERGE,
        INDEXES,
        CONNECTION_MERGE,
        CONNECTION_MATCH
    }

    static Logger log = LoggerFactory.getLogger(MessageProcessor.class)

    static Map<StatementType, List<String>> process(String msg) {
        Map<StatementType, List<String>> typesOfStatements = [:]
        if (msg) {
            def msgMap = buildMapFromJSONString(msg)
            typesOfStatements.put(StatementType.PRIMARY_NODE_MERGE, processPrimaryNodeMerge(msgMap))
            typesOfStatements.put(StatementType.INDEXES, processIndexes(msgMap))
            typesOfStatements.put(StatementType.CONNECTION_MERGE, processConnectionMerges(msgMap))
            typesOfStatements.put(StatementType.CONNECTION_MATCH, processConnectionMatches(msgMap))
        }
        typesOfStatements
    }

    static List<String> processPrimaryNodeMerge(Map msgMap) {
        List<String> statements = []
        statements.addAll(processMerges(msgMap))
        statements.addAll(processConnectionMatches(msgMap))
        statements.addAll(processConnectionRelationships(msgMap))
        statements
    }


    static Map<String, String> processPrimaryNodeParametersAsMap(String msg) {
        if (msg) {
            processPrimaryNodeParametersAsMap(buildMapFromJSONString(msg))
        }
    }

    static String processPrimaryNodeParametersAsJSON(String msg) {
        if (msg) {
            new Gson().toJson(processPrimaryNodeParametersAsMap(buildMapFromJSONString(msg)))
        }
    }

    static Map<String, Map<String, String>> processParameterForConnections(String msg) {
        if (msg) {
            processParameterForConnections(buildMapFromJSONString(msg))
        }
    }

    static Map buildMapFromJSONString(String msg) {
        Gson gson = new Gson()
        Map msgMap = gson.fromJson(msg, Map.class)
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

    static Map<String, String> processPrimaryNodeParametersAsMap(Map msgMap) {
        flattenMessageMap(msgMap, true)
    }


    static Map<String, Map<String, String>> processParameterForConnections(Map msgMap) {
        Map<String, Map<String, String>> paramMap = [:]
        msgMap.Connections.eachWithIndex() { Map it, Integer i ->
            Map connectionMap = [:]
            connectionMap.putAll(it)
            connectionMap.putAll(it.ConformedDimensions)
            connectionMap.remove("ConformedDimensions")
            def key = it.NodeType.toLowerCase() + Integer.toString(i)
            paramMap.put(key, connectionMap)
        }
        paramMap
    }

    private static HashMap flattenMessageMap(Map msgMap, boolean includeConformedDimensions) {
        def flattenedMap = new HashMap(msgMap)
        flattenedMap.putAll(msgMap.Properties ?: [:])
        if (includeConformedDimensions) {
            flattenedMap.putAll(msgMap.ConformedDimensions ?: [:])
        }
        flattenedMap.remove("ConformedDimensions")
        flattenedMap.remove("Connections")
        flattenedMap.remove("NodeType")
        flattenedMap.remove("Properties")
        flattenedMap.remove("RelType")
        flattenedMap.remove("ForwardRel")

        flattenedMap
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

    static List<String> processMerges(Map msgMap) {
        def mergeStatements = []
        // Card Merge MERGE (employee:Card:Employee {Email: "konrad.aust@menome.com",EmployeeId: 12345})
        String msgNodeType = msgMap.NodeType
        String nodeName = msgNodeType.toLowerCase()
        def cardMerge = "MERGE (${nodeName}:Card:$msgNodeType "

        def conformedDimensions = msgMap.ConformedDimensions
        if (conformedDimensions) {
            cardMerge += "{" + buildMergeExpressionFromMap(conformedDimensions, "", ":") + "})"
        }

        cardMerge += " ON CREATE SET ${nodeName}.Uuid = apoc.create.uuid(),${nodeName}.TheLinkAddedDate = datetime()"

        Map keysToProcess = flattenMessageMap(msgMap, false)
        cardMerge += ", "
        def mergeExpression = buildMergeExpressionFromMap(keysToProcess, nodeName + ".", "=")
        cardMerge += mergeExpression
        cardMerge += " ON MATCH SET " + mergeExpression
        if (msgMap.Connections) {
            cardMerge += " WITH $nodeName,param "
        }
        mergeStatements << cardMerge
    }


    static List<String> processConnectionMerges(Map msgMap) {
        def mergeStatements = []
        msgMap.Connections.eachWithIndex() { Map map, Integer connectionCounter ->
            String msgNodeType = map.NodeType
            String nodeName = msgNodeType.toLowerCase() + Integer.toString(connectionCounter)
            String merge = "MERGE ($nodeName:$msgNodeType"

            def conformedDimensions = map.ConformedDimensions
            if (conformedDimensions) {

                merge += "{" + buildMergeExpressionFromMap(conformedDimensions, "", ":") + "})"
            }

            merge += " ON CREATE SET ${nodeName}.Uuid = apoc.create.uuid(),${nodeName}.TheLinkAddedDate = datetime()"
            Map keysToProcess = flattenMessageMap(map, false)
            merge += ", "
            def mergeExpression = buildMergeExpressionFromMap(keysToProcess, nodeName + ".", "=")
            merge += mergeExpression
            merge += " ON MATCH SET " + mergeExpression

            mergeStatements << merge
        }

        mergeStatements
    }

    static String buildMergeExpressionFromMap(msgMap, valuePrefix, keySeparator) {
        String expression = ""
        msgMap.eachWithIndex { key, value, index ->
            expression += "$valuePrefix$key$keySeparator param.$key" + (index < msgMap.size() - 1 ? "," : "")
        }
        expression
    }

    /*
    MATCH (office:Office {City: "Victoria"}) WITH employee,office
    MATCH (project:Project {Code: 5}) WITH employee,office,project
    MATCH (team:Team {Code: 1337}) WITH employee,office,project,team
    */

    static List<String> processConnectionMatches(Map msgMap) {
        List<String> connectedNodeMatchStatements = []
        String primaryNodeType = msgMap.NodeType
        String primaryNodeName = primaryNodeType.toLowerCase()

        String withExpression = " WITH $primaryNodeName,param"
        msgMap.Connections.eachWithIndex { Map map, Integer connectionCounter ->
            String msgNodeType = map.NodeType
            String nodeName = msgNodeType.toLowerCase()
            String match = "MATCH ($nodeName$connectionCounter:$msgNodeType {"
            map.ConformedDimensions.eachWithIndex { key, value, index ->
                match += "$key : param.$nodeName$connectionCounter$key" + (index < map.ConformedDimensions.size() - 1 ? "," : "")
            }
            withExpression = withExpression + "," + nodeName + connectionCounter

            match += "})"
            match += withExpression
            connectedNodeMatchStatements.add(match)
        }
        connectedNodeMatchStatements
    }

    static List<String> processConnectionRelationships(Map msgMap) {
        List<String> connectionRelationshipStatements = []
        String msgNodeType = msgMap.NodeType
        String nodeName = msgNodeType.toLowerCase()

        msgMap.Connections.eachWithIndex { Map map, Integer connectionCounter ->
            String relationshipNodeType = map.NodeType
            String relationshipNodeName = relationshipNodeType.toLowerCase()
            String relType = map.RelType
            def relationshipMerge = "MERGE ($nodeName)-[${relationshipNodeName}${connectionCounter}_rel:${relType}]->($relationshipNodeName$connectionCounter)"
            connectionRelationshipStatements.add(relationshipMerge)
        }
        connectionRelationshipStatements
    }

    /**
     * Returns the message type from either a merge or a match statement
     * MATCH (office:Office -> office
     * MERGE (project:Project -> project
     * @param statement
     * @return Message Type
     */
    static String deriveMessageTypeFromStatement(String statement) {
        statement.substring(7, statement.indexOf(":"))
    }
}
