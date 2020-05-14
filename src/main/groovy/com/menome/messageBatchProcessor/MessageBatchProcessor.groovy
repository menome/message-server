package com.menome.messageBatchProcessor


import com.menome.messageProcessor.InvalidMessageException
import com.menome.messageProcessor.MessageProcessor
import com.menome.messageProcessor.Neo4JStatements
import com.menome.util.Neo4J
import org.apache.commons.lang3.time.StopWatch
import org.neo4j.driver.Driver
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.concurrent.TimeUnit

class MessageBatchProcessor {

    static Logger log = LoggerFactory.getLogger(MessageBatchProcessor.class)

    static MessageBatchResult process(List<String> messages, Driver driver) {

        log.info(Thread.currentThread().getName() + " " + messages.size())
        StopWatch timer = new StopWatch()
        timer.start()

        List<MessageError> errors = []

        def t = groupMessagesByNodeType(messages)
        Map<String, List<String>> messagesByNodeType = t.first
        errors.addAll(t.second)

        messagesByNodeType.each { nodeType, msgs ->
            Neo4JStatements statements = MessageProcessor.process(msgs.get(0))
            processIndexes(msgs, driver)
            errors.addAll(processConnectionMerges(msgs, statements, driver))
            errors.addAll(processPrimaryNodeMerges(msgs, statements, driver))
        }
        timer.stop()

        def batchSummary = new MessageBatchSummary(messages.size(), errors.size(),Duration.ofMillis(timer.getTime(TimeUnit.MILLISECONDS)))
        new MessageBatchResult(batchSummary,errors)
    }

    private static Tuple2<Map<String, List<String>>,List<MessageError>> groupMessagesByNodeType(List<String> messages){
        Map<String, List<String>> messagesByNodeType = [:]
        List<MessageError> errors = []
        messages.each() { String jsonMessage ->
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
        }
        return new Tuple2(messagesByNodeType,errors)
    }

    //todo: I'm, throwing away the Neo4J result. Might be some useful information in there for the summary.
    private static  List<MessageError> processPrimaryNodeMerges(List<String> messages, Neo4JStatements statements, Driver driver) {
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

        log.debug(unwind)
        try {
            Neo4J.executeStatementListInSession(List.of(unwind), driver.session(), parameters)
        } catch (Exception e){
            if (messages.size() > 1){
                List<String> segments = messages.collate(messages.size().intdiv(2),true)
                segments.each(){segmentMessages->
                    Neo4JStatements segmentStatements = MessageProcessor.process(segmentMessages[0])
                    errors.addAll(processPrimaryNodeMerges(segmentMessages,segmentStatements,driver))
                    return errors
                }
            } else {
                errors.add(new MessageError(e.toString(),messages[0]))
            }
        }
        errors
    }

    private static List<MessageError> processConnectionMerges(List<String> messages, Neo4JStatements statements, Driver driver) {

        List<MessageError> errors = []
        HashSet <Map> uniqueParms = new HashSet<>()
        messages.each(){String message->
            uniqueParms.add(MessageProcessor.processParameterForConnections(message))
        }

        new ArrayList(uniqueParms).each() { parmsMap ->
            statements.connectionMerge.each() {
                String key = MessageProcessor.deriveMessageTypeFromStatement(it)
                Map confirmedDimensionParms = parmsMap.get(key)
                String unwind = "UNWIND \$parms AS param " + it
                Map parameters = ["parms": List.of(confirmedDimensionParms)]
                try {
                    Neo4J.run(driver, unwind, parameters)
                } catch(Exception e){
                    errors.add(new MessageError(e.toString(),unwind))
                }
            }
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
