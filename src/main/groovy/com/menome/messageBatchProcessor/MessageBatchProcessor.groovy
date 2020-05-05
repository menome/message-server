package com.menome.messageBatchProcessor


import com.menome.messageProcessor.MessageProcessor
import com.menome.util.Neo4J
import org.neo4j.driver.Driver

class MessageBatchProcessor {

    static String process(List<String> messages, Driver driver,boolean createIndexes) {
        HashSet indexes = []
        HashSet connectionMerges = []

        // Todo this is wrong. We need to collect up all the unique indexes and conformed dimension merge parameters for the entire set not just the first entry
        Map<MessageProcessor.StatementType, List<String>> statementMap = MessageProcessor.process(messages.get(0))
            indexes.addAll(statementMap.get(MessageProcessor.StatementType.INDEXES))
        connectionMerges.addAll(statementMap.get(MessageProcessor.StatementType.CONNECTION_MERGE))
        Map params = MessageProcessor.processParameterForConnections(messages.get(0))

        if (createIndexes) {
            Neo4J.run(driver, statementMap.get(MessageProcessor.StatementType.INDEXES), [:])
        }

        connectionMerges.each() {
            String key = MessageProcessor.deriveMessageTypeFromStatement(it)
            Map confirmedDimensionParms = params.get(key)
            String unwind = "UNWIND \$parms AS param " + it
            Map parameters = ["parms": List.of(confirmedDimensionParms)]
            Neo4J.run(driver, unwind, parameters)
        }

        List<Map<String, String>> nodeParameters = []
        messages.each() { String message ->
            def map = MessageProcessor.processPrimaryNodeParametersAsMap(message)
            map.put("officeCity","Victoria")
            map.put("projectCode","5")
            map.put("teamCode","1337")
            //map.putAll(MessageProcessor.processParameterForConnections(message))
            nodeParameters.addAll(map)
        }

        //todo: Need to collect up all unique primary node merges and run them with their parms in threads
        String statement = ""
        statementMap.get(MessageProcessor.StatementType.PRIMARY_NODE_MERGE).each() { String statementFragment ->
            statement += statementFragment + "\n"
        }
        String unwind = "UNWIND \$parms AS param " + statement
        Map parameters = ["parms": nodeParameters]

        //Neo4J.run(driver, unwind, parameters)
        Neo4J.executeStatementListInSession(List.of(unwind),driver.session(),parameters)
        println("Done")
    }

}
