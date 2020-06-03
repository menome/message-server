package com.menome.messageBatchProcessor

import com.menome.SymendMessagingSpecification
import com.menome.util.Neo4J
import org.neo4j.driver.Driver
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux

import java.time.Duration
import java.util.concurrent.TimeUnit

import static org.awaitility.Awaitility.await

class MessageBatchPerformanceSpecification extends SymendMessagingSpecification {

    static Logger log = LoggerFactory.getLogger(MessageBatchPerformanceSpecification.class)
    static int BATCH_SIZE = 5_000

    def setup() {
        def driver = Neo4J.openDriver()
        Neo4J.run(driver, "match (n) where n.SourceSystem='menome_test_framework' detach delete n")
        await().atMost(1, TimeUnit.MINUTES).until { Neo4J.run(driver, "match (n) return count(n) as count").single().get("count").asInt() == 0 }
    }

    def testMessageCreate(int numberOfNodes, int numberOfConnections, int numberOfPrimaryNodeProperties) {
        List<String> symendMessages = buildSymendMessages(numberOfNodes, numberOfConnections, numberOfPrimaryNodeProperties)
        processMessageList(symendMessages)
    }

    private static void processMessageList(List<String> symendMessages) {
        Driver driver = Neo4J.openDriver()
        Flux<String> flux = Flux.fromIterable(symendMessages)
        flux.bufferTimeout(BATCH_SIZE, Duration.ofMillis(5))
                .map({ messages -> MessageBatchProcessor.process(messages, driver) })
                .map({
                    result ->
                        log.info(result.toString())
                        return result
                })
                .subscribe()
        driver.close()

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
