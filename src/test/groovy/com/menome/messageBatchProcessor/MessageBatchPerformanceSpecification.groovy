package com.menome.messageBatchProcessor

import com.menome.SymendMessagingSpecification
import com.menome.util.Neo4J
import org.neo4j.driver.Driver
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class MessageBatchPerformanceSpecification extends SymendMessagingSpecification {

    static Logger log = LoggerFactory.getLogger(MessageBatchPerformanceSpecification.class)
    static int BATCH_SIZE = 5_000

    def setup() {
        Neo4J.deleteAllTestNodes()
    }

    def testMessageCreate(int numberOfNodes, int numberOfConnections, int numberOfPrimaryNodeProperties) {
        List<String> symendMessages = buildSymendMessages(numberOfNodes, numberOfConnections, numberOfPrimaryNodeProperties)
        processMessageList(symendMessages)
    }

    private static void processMessageList(List<String> symendMessages) {
        Driver driver = Neo4J.openDriver()
        try {
            symendMessages.collate(BATCH_SIZE).each { List<String> messages ->
                log.info(MessageBatchProcessor.process(messages, driver).toString())
            }
        } finally {
            driver.close()
        }
    }

    def "spectrum test"() {
        given:
        //def messagesToCreate = [5, 50, 500, 5_000, 50_000]
        def messagesToCreate = [5, 50, 500]
        def connectionsToCreate = [0, 3]
        def primaryPropertiesToCreate = [0, 5, 10, 41]

        messagesToCreate.each() { messageCount ->
            connectionsToCreate.each() { connectionCount ->
                primaryPropertiesToCreate.each() { primaryPropertyCount ->
                    log.info("{} nodes {} properties with {} connections", messageCount, primaryPropertyCount,connectionCount)
                    testMessageCreate(messageCount as int, connectionCount as int, primaryPropertyCount)
                }
            }
        }
    }
}
