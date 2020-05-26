package com.menome.messageBatchProcessor

import com.menome.SymendMessagingSpecification
import com.menome.util.Neo4J
import org.neo4j.driver.Driver
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import spock.lang.Ignore

import java.time.Duration
import java.util.concurrent.TimeUnit

import static org.awaitility.Awaitility.await

class MessageBatchPerformanceSpecification extends SymendMessagingSpecification {

    static Logger log = LoggerFactory.getLogger(MessageBatchPerformanceSpecification.class)
    static int BATCH_SIZE = 5000

    def setup() {
        def driver = Neo4J.openDriver()
        Neo4J.run(driver, "match (n) detach delete n")
        await().atMost(1, TimeUnit.MINUTES).until { Neo4J.run(driver, "match (n) return count(n) as count").single().get("count").asInt() == 0 }
    }

    def createReferenceData() {
        processMessageList(buildSymendActivities(5000))
        processMessageList(buildSymendAccounts(5000))
        processMessageList(buildSymendDialerEntries(5000))
    }

    boolean testMessageCreate(int numberOfNodes, int numberOfConnections, int numberOfPrimaryNodeProperties, MessageBatchConfiguration configuration) {
        List<String> symendMessages = buildSymendMessages(numberOfNodes, numberOfConnections, numberOfPrimaryNodeProperties)
        processMessageList(symendMessages, configuration)
        true

    }

    boolean testMessageCreate(int numberOfNodes, int numberOfConnections, int numberOfPrimaryNodeProperties) {
        testMessageCreate(numberOfNodes, numberOfConnections, numberOfPrimaryNodeProperties, new MessageBatchConfiguration(true))
    }

    private static void processMessageList(List<String> symendMessages, MessageBatchConfiguration config) {
        Driver driver = Neo4J.openDriver()
        Flux<String> flux = Flux.fromIterable(symendMessages)
        flux.bufferTimeout(BATCH_SIZE, Duration.ofMillis(5))
                .map({ messages -> MessageBatchProcessor.process(messages, driver, config) })
                .map({
                    result ->
                        log.info(result.toString())
                        return result
                })
                .subscribe()
        driver.close()

    }

    private static void processMessageList(List<String> symendMessages) {
        processMessageList(symendMessages, new MessageBatchConfiguration(true))
    }

    def "five nodes with connections"() {
        Driver driver = Neo4J.openDriver()
        given:
        testMessageCreate(5, Integer.MAX_VALUE, Integer.MAX_VALUE)
        expect:
        5 == Neo4J.run(driver, "match (n:CollectionEvent) return count(n) as count").single().get("count").asInt()


    }

    def "five nodes with no connections"() {
        expect:
        testMessageCreate(5, 0, 0)
    }

    def "five hundred nodes with no connections"() {
        expect:
        testMessageCreate(500, 0, 0)
    }

    def "five hundred nodes with connections"() {
        expect:
        testMessageCreate(500, Integer.MAX_VALUE, Integer.MAX_VALUE)
    }

    @Ignore
    def "fifty thousand nodes with no connections"() {
        expect:
        testMessageCreate(50_000, 0, 0)
    }

    @Ignore
    def "fifty thousand nodes with no connections all properties"() {
        expect:
        testMessageCreate(50_000, 0, Integer.MAX_VALUE)
    }

    @Ignore
    def "fifty thousand nodes with one connection all properties"() {
        expect:
        testMessageCreate(50_000, 1, Integer.MAX_VALUE)
    }

    @Ignore
    def "fifty thousand nodes with two connections all properties"() {
        expect:
        testMessageCreate(50_000, 2, Integer.MAX_VALUE)
    }

    @Ignore
    def "fifty thousand nodes with three connections all properties"() {
        expect:
        testMessageCreate(50_000, 3, Integer.MAX_VALUE)
    }


    @Ignore
    def "fifty thousand nodes with one connections and reference data created"() {
        given:
        Driver driver = Neo4J.openDriver()
        createReferenceData()
        await().atMost(3, TimeUnit.MINUTES).until { Neo4J.run(driver, "match (n) return count(n) as count").single().get("count").asInt() == 15000 }
        testMessageCreate(50_000, 1, Integer.MAX_VALUE, new MessageBatchConfiguration(false))
        expect:
        1 == 1
    }

    @Ignore
    def "fifty thousand nodes with two connections and reference data created"() {
        given:
        Driver driver = Neo4J.openDriver()
        createReferenceData()
        await().atMost(3, TimeUnit.MINUTES).until { Neo4J.run(driver, "match (n) return count(n) as count").single().get("count").asInt() == 15000 }
        testMessageCreate(50_000, 2, Integer.MAX_VALUE, new MessageBatchConfiguration(false))
        expect:
        1 == 1
    }

    @Ignore
    def "fifty thousand nodes with three connections and reference data created"() {
        given:
        Driver driver = Neo4J.openDriver()
        createReferenceData()
        await().atMost(3, TimeUnit.MINUTES).until { Neo4J.run(driver, "match (n) return count(n) as count").single().get("count").asInt() == 15000 }
        testMessageCreate(50_000, 3, Integer.MAX_VALUE, new MessageBatchConfiguration(false))
        expect:
        1 == 1
    }

}
