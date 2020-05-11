package com.menome.messageBatchProcessor


import com.menome.messageProcessor.MessageProcessor
import com.menome.util.Neo4J
import org.apache.commons.lang3.time.StopWatch
import org.neo4j.driver.Driver
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class MessageBatchProcessor {

    static Logger log = LoggerFactory.getLogger(MessageBatchProcessor.class)

    static List<String> process(List<String> messages, Driver driver, boolean createIndexes) {

        log.info(Thread.currentThread().getName() + " " + messages.size());
        StopWatch timer = new StopWatch();
        timer.start();

        Map<MessageProcessor.StatementType, List<String>> statementMap = MessageProcessor.process(messages.get(0))

        processIndexes(messages, createIndexes, driver)
        processConnectionMerges(messages, statementMap, driver)
        processPrimaryNodeMerges(messages, statementMap, driver)
        timer.stop();
        log.info("elapsed = " + timer.formatTime());

        return messages;
    }

    private static void processPrimaryNodeMerges(List<String> messages, Map<MessageProcessor.StatementType, List<String>> statementMap, Driver driver) {
        List<Map<String, String>> nodeParameters = []
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

        log.debug(unwind)
        Neo4J.executeStatementListInSession(List.of(unwind), driver.session(), parameters)
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

    private static void processIndexes(List<String> messages, boolean createIndexes, Driver driver) {
// Todo this is wrong. We need to collect up all the unique indexes and conformed dimension merge parameters for the entire set not just the first entry

        Map<MessageProcessor.StatementType, List<String>> statementMap = MessageProcessor.process(messages.get(0))
        def indexes = statementMap.get(MessageProcessor.StatementType.INDEXES)

        //RETURN apoc.schema.node.indexExists("Card", ["Email","EmployeeId"])
        if (createIndexes) {
            Neo4J.run(driver, indexes, [:])
        }
    }

}
