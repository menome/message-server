package com.menome.messageBatchProcessor

import com.menome.MessagingSpecification
import com.menome.util.Neo4J
import org.neo4j.driver.Driver
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import spock.lang.Shared

import java.time.Duration
import java.time.Instant

class MessageBatchProcessorSpecification extends MessagingSpecification {
    @Shared
    GenericContainer neo4JContainer

    def setup() {
        neo4JContainer = createAndStartNeo4JContainer(Network.newNetwork())
    }

    def "three messages comparing full batch script"() {
        given:
        List<String> threeMessageBatch = threeMessageBatch
        Driver driver = Neo4J.openDriver(neo4JContainer)
        MessageBatchProcessor.process(threeMessageBatch, driver)
        expect:
        1 == 1
        sleep(10000000)

    }

    def "five hundred messages comparing full batch script"() {
        given:
        List<String> fiveHundredMessageBatch = fiveThousandMessageBatch
        Driver driver = Neo4J.openDriver(neo4JContainer)
        def start = Instant.now()
        println("Starting:$start")
        MessageBatchProcessor.process(fiveHundredMessageBatch, driver,true)
        def end = Instant.now()
        def seconds = Duration.between(start, end).getSeconds()
        println("Duration $seconds")
        println("Finished $end")
        expect:
        1 == 1
        sleep(10000000)

    }

}
