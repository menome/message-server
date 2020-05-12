package com.menome.messageBatchProcessor

import com.menome.errorHandler.ErrorHandlerHelper
import com.menome.messageProcessor.InvalidMessageException
import com.menome.messageProcessor.MessageProcessor
import com.menome.util.Neo4J
import org.apache.commons.lang3.time.StopWatch
import org.neo4j.driver.Driver
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class MessageBatchProcessor {

    static Logger log = LoggerFactory.getLogger(MessageBatchProcessor.class)

    static Tuple2<Map,List<Tuple2>> process(List<String> messages, Driver driver) {

        log.info(Thread.currentThread().getName() + " " + messages.size());
        StopWatch timer = new StopWatch();
        timer.start();

        List<Tuple2<String, String>> errors = []
        Map<String,String> batchStatus = [:]

        Map<String, List<String>> messagesByNodeType = [:]
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
                errors.add(ErrorHandlerHelper.toTupple(new InvalidMessageException("Missing NodeType"), jsonMessage))
            }
        }

        messagesByNodeType.each { nodeType, msgs ->
            Map<MessageProcessor.StatementType, List<String>> statementMap = MessageProcessor.process(msgs.get(0))
            processIndexes(msgs, driver)
            processConnectionMerges(msgs, statementMap, driver)
            errors.addAll(processPrimaryNodeMerges(msgs, statementMap, driver))
        }
        timer.stop()

        log.info("elapsed = " + timer.formatTime())

        return new Tuple2<Map, List<Tuple2>>(batchStatus,errors)
    }

    private static  List<Tuple2<String,String>> processPrimaryNodeMerges(List<String> messages, Map<MessageProcessor.StatementType, List<String>> statementMap, Driver driver) {
        List<Map<String, String>> nodeParameters = []
        List<Tuple2<String,String>> errors = []
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
        statementMap.get(MessageProcessor.StatementType.PRIMARY_NODE_MERGE).each() { String statementFragment ->
            statement += statementFragment + "\n"
        }
        String unwind = "UNWIND \$parms AS param " + statement
        Map parameters = ["parms": nodeParameters]

        log.info(unwind)
        try {
            Neo4J.executeStatementListInSession(List.of(unwind), driver.session(), parameters)
        } catch (Exception e){
            if (messages.size() > 1){
                messages.each(){message->
                    Map<MessageProcessor.StatementType, List<String>> typeListMap = MessageProcessor.process(message)
                    return processPrimaryNodeMerges(List.of(message),typeListMap,driver)
                }
            } else {
                errors.add(ErrorHandlerHelper.toTupple(e,messages[0]))
            }
        }
        return errors;
    }

    private static List<Object> processConnectionMerges(List messages, Map<MessageProcessor.StatementType, List<String>> statementMap, Driver driver) {

        Map params = MessageProcessor.processParameterForConnections(messages.get(0))
        def connectionMerges = new HashSet()
        connectionMerges.addAll(statementMap.get(MessageProcessor.StatementType.CONNECTION_MERGE))

        def list = new ArrayList(connectionMerges)

        list.each() {
            String key = MessageProcessor.deriveMessageTypeFromStatement(it)
            Map confirmedDimensionParms = params.get(key)
            String unwind = "UNWIND \$parms AS param " + it
            Map parameters = ["parms": List.of(confirmedDimensionParms)]
            Neo4J.run(driver, unwind, parameters)
        }
    }

    private static void processIndexes(List<String> messages, Driver driver) {

        Map<MessageProcessor.StatementType, List<String>> statementMap = MessageProcessor.process(messages.get(0))
        def indexes = statementMap.get(MessageProcessor.StatementType.INDEXES)

        //todo: This seems very smelly, but is the easiest way to attempt to create the indexes. There is an apoc method
        // to test if an index exists //RETURN apoc.schema.node.indexExists("Card", ["Email","EmployeeId"]), but the logic to deconstruct the
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
