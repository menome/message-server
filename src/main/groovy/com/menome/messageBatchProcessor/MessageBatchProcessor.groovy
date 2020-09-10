package com.menome.messageBatchProcessor


import com.menome.message.Neo4JConnection
import com.menome.message.Neo4JNode
import com.menome.messageParser.ConvertJSonToNeo4JObjects
import com.menome.util.ApplicationConfiguration
import com.menome.util.Neo4J
import com.menome.util.PreferenceType
import io.micrometer.core.instrument.MeterRegistry
import org.apache.commons.lang3.time.StopWatch
import org.everit.json.schema.Schema
import org.everit.json.schema.ValidationException
import org.everit.json.schema.loader.SchemaLoader
import org.json.JSONObject
import org.json.JSONTokener
import org.neo4j.driver.Driver
import org.neo4j.driver.Session
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.concurrent.TimeUnit

class MessageBatchProcessor {

    static Logger log = LoggerFactory.getLogger(MessageBatchProcessor.class)
    static def jsonSchema
    static JSONObject rawSchema
    static Schema schema
    static boolean enableValidation = true  //Should only be used for testing purposes


    // Load the harvester schema (used to validate incoming JSON messages) from the local dev environment or from the packaged jar at runtime
    static {
        jsonSchema = null
        try {
            jsonSchema = new File("src/main/resources/harvester_schemax.json").text
        } catch (Exception ignore) {
            try {
                jsonSchema = MessageBatchProcessor.getResourceAsStream("/harvester_schema.json").text
            } catch (Exception ignored) {
                log.info("MessageProcessor validation disabled. Schema harvester_schema.json cannot be located. ")
            }
        }

        if (jsonSchema != null) {
            if (ApplicationConfiguration.getString(PreferenceType.SHOW_CONNECTION_LOG_OUTPUT) == "Y") {
                log.info("MessageProcessor schema validation enabled. harvester_schema.json will be used to validate all incoming messages. ")
            }
            rawSchema = new JSONObject(new JSONTokener(jsonSchema))
            schema = SchemaLoader.load(rawSchema)
        }

    }

    static MessageBatchResult process(List<String> messages, Driver driver, MeterRegistry registry) {
        log.debug(Thread.currentThread().getName() + " " + messages.size())
        StopWatch entireBatchResultTimer = new StopWatch()
        entireBatchResultTimer.start()

        def session = driver.session()
        List<MessageError> errors = []
        Tuple2<Map<String, MessageBatchCollection>, List<MessageError>> batches = groupMessagesByNodeType(messages)
        HashSet<Neo4JConnection> connections = []
        //Process micro batches each node type
        batches.first().each { String nodeType, MessageBatchCollection batch ->
            processIndexes(batch, session)
            errors.addAll(processNodesMicroBatch(batch, session))
        }

        //Collect up all the unique connections
        batches.first.each() { String nodeType, MessageBatchCollection batch ->
            connections.addAll(batch.connections)
        }

        //process all the connections
        Map<String, MessageConnectionCollection> connectionsByNodeType = groupConnectionsByRelationshipType(connections)
        connectionsByNodeType.each() { String relationshipType, MessageConnectionCollection connectionCollection ->
            processConnectionMicroBatch(connectionCollection, session)
        }


        session.close()
        //todo:
        entireBatchResultTimer.stop()
        errors.addAll(batches.getSecond())
        def batchDuration = Duration.ofMillis(entireBatchResultTimer.getTime(TimeUnit.MILLISECONDS))
        def batchSummary = new MessageBatchSummary(messages.size(), errors.size(), batchDuration)
        new MessageBatchResult(batchSummary, errors)
    }

    private static void processIndexes(MessageBatchCollection batch, Session session) {
        Neo4JNode node = batch.prototypeNode()
        processIndexes(node, session)
    }

    private static List<MessageError> processNodesMicroBatch(MessageBatchCollection batch, Session session) {
        List<MessageError> errors = []
        Neo4JNode node = batch.prototypeNode()
        String mergesStatement = node.toCypher()
        String unwind = "UNWIND \$parms AS param " + mergesStatement
        try {
            Neo4J.executeStatementListInSession([unwind], session, batch.nodesAsParameterList())
        } catch (Exception exception) {
            if (batch.nodes.size() > 1) {
                List<MessageBatchCollection> segments = MessageBatchCollection.split(batch)
                segments.each() { MessageBatchCollection messageBatchCollection ->
                    errors.addAll(processNodesMicroBatch(messageBatchCollection, session))
                    return errors
                }
            } else {
                //todo not sure the string value of the node is super useful here. Might need to get back to the original message.
                errors.add(new MessageError(exception.toString(), batch.nodes[0].toString()))
            }
        }
        errors
    }

    private static List<MessageError>  processConnectionMicroBatch(MessageConnectionCollection connectionCollection, Session session) {
        List<MessageError> errors = []
        def connection = connectionCollection.prototypeRelationship()
        String connectionMerge = connection.toCypher()
        String unwind = "UNWIND \$parms AS param " + connectionMerge
        try {
            Neo4J.executeStatementListInSession([unwind], session, connectionCollection.relationshipsAsParameterList())
        } catch (Exception exception){
            if (connectionCollection.connections.size() > 1) {
                List<MessageConnectionCollection> segments = MessageConnectionCollection.split(connectionCollection)
                segments.each() { MessageConnectionCollection messageConnectionCollection ->
                    errors.addAll(processConnectionMicroBatch(messageConnectionCollection, session))
                    return errors
                }
            } else {
                //todo not sure the string value of the connection is super useful here. Might need to get back to the original message.
                errors.add(new MessageError(exception.toString(), connectionCollection.connections[0].toString()))
            }

        }
        errors
    }


    static MessageBatchResult process(List<String> messages, Driver driver) {
        process(messages, driver, null)
    }


    static void validateMessage(String message) {
        if (schema != null) {
            schema.validate(new JSONObject(message))
        }
    }

    private static Tuple2<Map<String, MessageBatchCollection>, List<MessageError>> groupMessagesByNodeType(List<String> messages) {
        Map<String, MessageBatchCollection> messagesByNodeType = [:]
        List<MessageError> errors = []
        messages.each() { String jsonMessage ->
            try {
                if (enableValidation) {
                    validateMessage(jsonMessage)
                }
                Tuple2<List<Neo4JNode>, List<Neo4JConnection>> neo4JObjects = ConvertJSonToNeo4JObjects.fromJson(jsonMessage)
                List<Neo4JNode> nodes = neo4JObjects.getFirst()
                nodes.each() { Neo4JNode node ->
                    MessageBatchCollection batchCollection = messagesByNodeType.get(node.nodeType)
                    if (!batchCollection) {
                        batchCollection = new MessageBatchCollection(node.nodeType)
                        messagesByNodeType.put(node.nodeType, batchCollection)
                    }
                    batchCollection.addNode(node)
                    batchCollection.addConnections(neo4JObjects.getSecond())
                }
            } catch (ValidationException ex) {
                errors.add(new MessageError(ex.errorMessage, jsonMessage))
            }
        }
        return new Tuple2(messagesByNodeType, errors)
    }

    static Map<String, MessageConnectionCollection> groupConnectionsByRelationshipType(HashSet<Neo4JConnection> neo4JConnections) {
        Map<String, MessageConnectionCollection> relationshipMap = [:]
        neo4JConnections.each() { Neo4JConnection connection ->
            // The key is a concatenation of three values from the connection to avoid grouping up similar, but not identical connections
            def key = connection.relationshipName + ":" + connection.source.nodeType + ":" + connection.destination.nodeType
            MessageConnectionCollection connections = relationshipMap.get(key)
            if (!connections) {
                connections = new MessageConnectionCollection(key)
                relationshipMap.put(key, connections)
            }
            connections.addConnection(connection)
        }
        relationshipMap
    }


    private static void processIndexes(Neo4JNode node, Session session) {

        List<String> indexes = node.indexes()

        //todo: This seems very smelly, but is the easiest way to attempt to create the indexes. There is an apoc method
        // to test if an index exists apoc.schema.node.indexExists("Card", ["Email","EmployeeId"]), but the logic to deconstruct the
        // index to create this statement is more trouble than it's worth. We'll try to create them and let it fail
        indexes.each() { index ->
            try {
                Neo4J.run(session, index, [:])
            } catch (Exception e) {
                //nothing to do here as index already exists.
            }
        }
    }


}


