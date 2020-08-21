package com.menome.messageProcessor

import com.google.gson.Gson
import com.menome.util.ApplicationConfiguration
import com.menome.util.PreferenceType
import org.everit.json.schema.Schema
import org.everit.json.schema.loader.SchemaLoader
import org.json.JSONObject
import org.json.JSONTokener
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class MessageProcessor {
    static Logger log = LoggerFactory.getLogger(MessageProcessor.class)
    static def jsonSchema
    static JSONObject rawSchema
    static Schema schema


    // Load the harvester schema (used to validate incoming JSON messages) from the local dev environment or from the packaged jar at runtime
    static {
        jsonSchema = null
        try {
            jsonSchema = new File("src/main/resources/harvester_schema.json").text
        } catch (Exception ignore) {
            try {
                jsonSchema = getClass().getResourceAsStream("/harvester_schema.json").text
            } catch (Exception ignored) {
                log.info("MessageProcessor validation disabled. Schema harvester_schema.json cannot be located. ")
            }
        }

        if (jsonSchema != null) {
            if (ApplicationConfiguration.getString(PreferenceType.SHOW_CONNECTION_LOG_OUTPUT) == "Y")  {
                log.info("MessageProcessor schema validation enabled. harvester_schema.json will be used to validate all incoming messages. ")
            }
            rawSchema = new JSONObject(new JSONTokener(jsonSchema))
            schema = SchemaLoader.load(rawSchema)
        }

    }


/**
 * Takes a message as a string and transforms it to the various Neo4J statements that the message represents.
 *
 * @param msg - Message that conforms to the DataHarvester pattern/schema
 * @return Neo4JStatements
 */

    static Neo4JStatements process(String msg) {
        Neo4JStatements statements = null
        if (msg) {
            def msgMap = buildMapFromJSONString(msg)
            List<String> primaryNodeMerge = processPrimaryNodeMerge(msgMap)
            List<String> indexes = processIndexes(msgMap)
            List<String> connectionMerges = processConnectionMerges(msgMap)
            List<String> connectionMatches = processConnectionMatches(msgMap)
            statements = new Neo4JStatements(primaryNodeMerge, indexes, connectionMerges, connectionMatches)
        }
        statements
    }

    /**
     * Takes a message as a JSON String and validates it against the harvester_schema
     * @param message
     * @throws org.everit.json.schema.ValidationException (Runtime, not checked exception)
     */
    static void validateMessage(String message) {
        if (schema != null) {
            schema.validate(new JSONObject(message))
        }
    }

    /**
     * Given a JSON message in the form of a string, coverts it toa a Map <String,String>
     * @param msg
     * @return Map<String,String>
     */
    static Map<String, String> processPrimaryNodeParametersAsMap(String msg) {
        if (msg) {
            processPrimaryNodeParametersAsMap(buildMapFromJSONString(msg))
        }
    }


    /**
     * Given the message in the form of a map, generates a list of strings that represent the Neo4J statementes required
     * to merge the primary node along with any connections to the related nodes.
     * @param msgMap
     * @return List<String>
     */
    static List<String> processPrimaryNodeMerge(Map msgMap) {
        List<String> statements = []
        statements.addAll(processMerges(msgMap))
        statements.addAll(processConnectionMatches(msgMap))
        statements.addAll(processConnectionRelationships(msgMap))
        statements
    }


    /**
     * Given a JSON message in the form of a String, uses the Gson library to convert it to a Map<String,String>
     * @param msg
     * @return Map<String,String>
     */
    static Map buildMapFromJSONString(String msg) {
        new Gson().fromJson(msg, Map.class)
    }

    /**
     *
     * @param msg
     * @return
     */
    static Map<String, Map<String, String>> processParameterForConnections(String msg) {
        if (msg) {
            processParameterForConnections(buildMapFromJSONString(msg))
        }
    }

    /**
     * Given a message in the form of a JSON String, returns a Map<String,Map<String,String>> where the key for the main map
     * is the name of the connection node with a numeric suffix to ensure it's uniqueness
     * @param msgMap
     * @return Map<String, Map<String, String>>
     */
    static Map<String, Map<String, String>> processParameterForConnections(Map msgMap) {
        Map<String, Map<String, String>> paramMap = [:]
        msgMap.Connections.eachWithIndex() { Map it, Integer i ->
            Map<String, String> connectionMap = [:]
            connectionMap.putAll(it)
            connectionMap.putAll(it.ConformedDimensions as Map)
            connectionMap.putAll(it.Properties as Map ?: [:])
            connectionMap.remove("ConformedDimensions")
            connectionMap.remove("Properties")
            String key = it.NodeType.toLowerCase() + Integer.toString(i)
            paramMap.put(key, connectionMap)
        }
        paramMap
    }


    /**
     * Given the message in Map format, generates the Neo4J statements to create the index from the conformed dimensions
     * @param msgMap
     * @return List<String>
     */
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

    /**
     * Given the message in Map format, returns a Map of the parameter names and values from the message
     * @param msgMap
     * @return Map<String,String>
     */
    static Map<String, String> processPrimaryNodeParametersAsMap(Map msgMap) {
        flattenMessageMap(msgMap, true)
    }


    /**
     * Given the message in Map format, it flattens the properties to the same level as the other message parameters.
     * Also removes elements that we will never bind as parameters.
     *
     * @param msgMap
     * @param includeConformedDimensions
     * @return
     */
    private static HashMap flattenMessageMap(Map msgMap, boolean includeConformedDimensions) {
        def flattenedMap = new HashMap(msgMap)
        flattenedMap.putAll(msgMap.Properties as Map ?: [:])
        if (includeConformedDimensions) {
            flattenedMap.putAll(msgMap.ConformedDimensions as Map ?: [:])
        }
        flattenedMap.remove("ConformedDimensions")
        flattenedMap.remove("Connections")
        flattenedMap.remove("NodeType")
        flattenedMap.remove("Properties")
        flattenedMap.remove("RelType")
        flattenedMap.remove("ForwardRel")


        flattenedMap
    }

    /**
     *  Given athe message in Map format, generates a Neo4J merge fragment for the primary node.
     *
     *  eg: MERGE (employee:Card:Employee {Email: param.Email,EmployeeId: param.EmployeeId})
     * ON CREATE SET
     *  employee.Uuid = apoc.create.uuid()
     * ,employee.TheLinkAddedDate = datetime()
     * ,employee.Status= param.Status
     * ,employee.Priority= param.Priority
     * ,employee.PreferredName= param.PreferredName
     * ,employee.SourceSystem= param.SourceSystem
     * ,employee.ResumeSkills= param.ResumeSkills
     * ,employee.Name= param.Name
     * ON MATCH SET
     *  employee.Status= param.Status
     *  ,employee.Priority= param.Priority
     *  ,employee.PreferredName= param.PreferredName
     *  ,employee.SourceSystem= param.SourceSystem
     *  ,employee.ResumeSkills= param.ResumeSkills
     *  ,employee.Name= param.Name
     *  WITH employee,param
     *     *
     * @param msgMap
     * @return List<String>
     */
    static List<String> processMerges(Map msgMap) {
        def mergeStatements = []
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

    /**
     * Given a message in the form of a Map. generates Neo4J merge statements for all connection nodes.
     * @param msgMap
     * @return List<String>
     */

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
            if (keysToProcess) {
                merge += ", "
                def mergeExpression = buildMergeExpressionFromMap(keysToProcess, nodeName + ".", "=")
                merge += mergeExpression
                merge += " ON MATCH SET " + mergeExpression
            }

            mergeStatements << merge
        }

        mergeStatements
    }

    /**
     * Given a message in the form of a map returns a Neo4J expression fragment that us used in merge statements
     * @param msgMap
     * @param valuePrefix - will be one of a , or a .
     * @param keySeparator - will be one of a : or =
     * @return
     */
    static String buildMergeExpressionFromMap(msgMap, valuePrefix, keySeparator) {
        String expression = ""
        msgMap.eachWithIndex { key, value, index ->
            expression += "$valuePrefix$key$keySeparator param.$key" + (index < msgMap.size() - 1 ? "," : "")
        }
        expression
    }

    /**
     * Given a message in the form of a map, generates a Neo4J fragment used to match the connection nodes inside the primary node merge
     * @param msgMap
     * @return List<String>
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

    /**
     * Given a message in the form of a map, generates the Neo4J merge statements used to create/update the connection nodes in the graph
     * @param msgMap
     * @return List<String>
     */
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
     * Given a Neo4J statement, returns the message type from either a merge or a match statement
     * MATCH (office:Office -> office
     * MERGE (project:Project -> project
     * @param statement
     * @return Message Type
     */
    static String deriveMessageTypeFromStatement(String statement) {
        statement.substring(7, statement.indexOf(":"))
    }
}
