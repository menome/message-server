package com.menome.messageBatchProcessor

import com.google.gson.Gson
import com.menome.messageProcessor.InvalidMessageException
import com.menome.messageProcessor.MessageProcessor
import com.menome.messageProcessor.Neo4JStatements
import com.menome.util.ApplicationConfiguration
import com.menome.util.Neo4J
import com.menome.util.PreferenceType
import com.menome.util.Redis
import org.apache.commons.lang3.time.StopWatch
import org.everit.json.schema.ValidationException
import org.neo4j.driver.Driver
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.concurrent.TimeUnit

class MessageBatchProcessor {

    static Logger log = LoggerFactory.getLogger(MessageBatchProcessor.class)

    static MessageBatchResult process(List<String> messages, Driver driver) {

        log.debug(Thread.currentThread().getName() + " " + messages.size())
        StopWatch timer = new StopWatch()
        timer.start()

        List<MessageError> errors = []

        def messagesByTypeOrErrorsTuple = groupMessagesByNodeType(messages)
        Map<String, List<String>> messagesByNodeType = messagesByTypeOrErrorsTuple.first
        errors.addAll(messagesByTypeOrErrorsTuple.second)

        messagesByNodeType.each { nodeType, msgs ->
            Neo4JStatements statements = MessageProcessor.process(msgs.get(0))
            processIndexes(msgs, driver)
            errors.addAll(processConnectionMerges(msgs, statements, driver))
            errors.addAll(processPrimaryNodeMerges(msgs, statements, driver))
        }
        timer.stop()

        if (log.isDebugEnabled()) {
            errors.each() { error ->
                log.debug(error.errorText)
            }
        }
        def batchSummary = new MessageBatchSummary(messages.size(), errors.size(), Duration.ofMillis(timer.getTime(TimeUnit.MILLISECONDS)))
        new MessageBatchResult(batchSummary, errors)
    }


    private static Tuple2<Map<String, List<String>>, List<MessageError>> groupMessagesByNodeType(List<String> messages) {
        Map<String, List<String>> messagesByNodeType = [:]
        List<MessageError> errors = []
        messages.each() { String jsonMessage ->
            try {
                MessageProcessor.validateMessage(jsonMessage)
                def msg = MessageProcessor.buildMapFromJSONString(jsonMessage)
                String nodeType = msg.NodeType
                if (nodeType) {
                    List<String> messageList = messagesByNodeType.get(nodeType)
                    if (!messageList) {
                        messageList = []
                    }
                    messageList.add(jsonMessage)
                    messagesByNodeType.put(nodeType, messageList)
                } else {
                    errors.add(new MessageError(new InvalidMessageException("Missing NodeType").toString(), jsonMessage))
                }
            } catch (ValidationException ex) {
                errors.add(new MessageError(ex.errorMessage, jsonMessage))
            }
        }
        return new Tuple2(messagesByNodeType, errors)
    }

    //todo: I'm, throwing away the Neo4J result. Might be some useful information in there for the summary.
    private static List<MessageError> processPrimaryNodeMerges(List<String> messages, Neo4JStatements statements, Driver driver) {
        List<Map<String, String>> nodeParameters = []
        List<MessageError> errors = []
        messages.each() { String message ->
            def map = MessageProcessor.processPrimaryNodeParametersAsMap(message)
            Map<String, Map<String, String>> conformedDimensionsMap = MessageProcessor.processParameterForConnections(message)
            conformedDimensionsMap.each { dimensionKey, dimensionList ->
                dimensionList.each { dimension, dimensionParameter ->
                    map.put((dimensionKey + dimension), dimensionParameter)
                }
            }
            nodeParameters.addAll(map)
        }

        String statement = ""
        statements.primaryNodeMerge.each() { String statementFragment ->
            statement += statementFragment + "\n"
        }
        String unwind = "UNWIND \$parms AS param " + statement
        Map parameters = ["parms": nodeParameters]

        if (log.isDebugEnabled()) {
            log.debug(unwind)
            log.debug(new Gson().toJson(parameters))
        }

        try {
            Neo4J.executeStatementListInSession(List.of(unwind), driver.session(), parameters)
        } catch (Exception e) {
            if (messages.size() > 1) {
                List<String> segments = messages.collate(messages.size().intdiv(2), true)
                segments.each() { segmentMessages ->
                    Neo4JStatements segmentStatements = MessageProcessor.process(segmentMessages[0])
                    errors.addAll(processPrimaryNodeMerges(segmentMessages, segmentStatements, driver))
                    return errors
                }
            } else {
                errors.add(new MessageError(e.toString(), messages[0]))
            }
        }
        errors
    }

    private static List<MessageError> processConnectionMerges(List<String> messages, Neo4JStatements statements, Driver driver) {

        List<MessageError> errors = []
        Map<String, HashSet> uniqueParameters = [:]
        def connection = Redis.connection()


        // Initialize unique map with the set of parameters names from the first message. All messages in the messages parameter are of the same
        // type so we can assume they will all have the same set of parameters
        if (messages) {
            statements.connectionMerge.each() {
                String key = MessageProcessor.deriveMessageTypeFromStatement(it)
                uniqueParameters.put(key, new HashSet())
            }
        }

        //Iterate over all of the messages getting the parameters for each of the connections. We build up a unique parameters by adding the
        // parameters to the hashset associated with the parameter type. There is no need to merge the exact same parameters over and over again
        messages.each() { String message ->
            Map<String, Map<String, String>> parmsFromMessage = MessageProcessor.processParameterForConnections(message)
            statements.connectionMerge.each() {
                String key = MessageProcessor.deriveMessageTypeFromStatement(it)
                Map<String, String> parmsForStatement = parmsFromMessage.get(key)
                Boolean addStatement = Boolean.TRUE
                if (ApplicationConfiguration.getString(PreferenceType.USE_REDIS_CACHE) == "Y") {
                    // Create a second map that also includes the NodeType to avoid the case where two different node types share the exact same parameters
                    Map<String, String> parmsToHash = new HashMap<>(parmsForStatement)
                    parmsToHash.put("NodeType", key)

                    String redisKey = key + "." + parmsToHash.hashCode().toString()

                    if (connection.get(redisKey)) {
                        addStatement = Boolean.FALSE
                        log.debug("Redis cache hit for parameter hash:{}", redisKey)
                    } else {
                        log.debug("Redis cache miss for parameter hash:{}", redisKey)
                        connection.set(redisKey, "Y")
                    }
                }
                if (addStatement) {
                    HashSet parmset = uniqueParameters.get(key)
                    parmset.add(parmsForStatement)
                    uniqueParameters.put(key, parmset)
                }
            }
        }

        statements.connectionMerge.each() {
            String key = MessageProcessor.deriveMessageTypeFromStatement(it)
            List<String> parameters = new ArrayList(uniqueParameters.get(key))
            String unwind = "UNWIND \$parms AS param " + it
            Map neo4JParameters = ["parms": parameters]
            if (log.isDebugEnabled()) {
                log.debug(unwind)
                log.debug(new Gson().toJson(neo4JParameters))
            }

            try {
                Neo4J.executeStatementListInSession(List.of(unwind), driver.session(), neo4JParameters)
            } catch (Exception e) {
                if (messages.size() > 1) {
                    List<String> segments = messages.collate(messages.size().intdiv(2), true)
                    segments.each() { segmentMessages ->
                        Neo4JStatements segmentStatements = MessageProcessor.process(segmentMessages[0])
                        errors.addAll(processConnectionMerges(segmentMessages, segmentStatements, driver))
                        return errors
                    }
                } else {
                    errors.add(new MessageError(e.toString(), messages[0]))
                }
            }
        }

        if (connection) {
            connection.close()
        }
        errors
    }


    private static void processIndexes(List<String> messages, Driver driver) {

        Neo4JStatements statements = MessageProcessor.process(messages.get(0))
        def indexes = statements.indexes

        //todo: This seems very smelly, but is the easiest way to attempt to create the indexes. There is an apoc method
        // to test if an index exists apoc.schema.node.indexExists("Card", ["Email","EmployeeId"]), but the logic to deconstruct the
        // index to create this statement is more trouble than it's worth. We'll try to create them and let it fail
        indexes.each() { index ->
            try {
                Neo4J.run(driver, index)
            } catch (Exception e) {
                //nothing to do here as index already exists.
            }
        }
    }
}
