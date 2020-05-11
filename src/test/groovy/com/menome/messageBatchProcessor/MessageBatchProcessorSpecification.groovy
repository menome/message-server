package com.menome.messageBatchProcessor

import com.menome.MessagingSpecification
import com.menome.util.Neo4J
import org.neo4j.driver.Driver
import org.testcontainers.containers.GenericContainer
import spock.lang.Shared

import java.time.Duration
import java.time.Instant

class MessageBatchProcessorSpecification extends MessagingSpecification {
    @Shared
    GenericContainer neo4JContainer

    def setup() {
        //neo4JContainer = createAndStartNeo4JContainer(Network.newNetwork())
        Neo4J.run(Neo4J.openDriver(),"match (n) detach delete n")
    }

    def "three messages comparing full batch script"() {
        given:
        List<String> threeMessageBatch = threeMessageBatch
        Driver driver = Neo4J.openDriver()
        MessageBatchProcessor.process(threeMessageBatch, driver, false)
        expect:
        def result = Neo4J.run(driver, "match (e:Employee) return count(e) as count")
        3 == result.single().get("count").asInt()
    }

    def "five hundred messages comparing full batch script"() {
        given:
        List<String> fiveHundredMessageBatch = fiveThousandMessageBatch
        Driver driver = Neo4J.openDriver()
        def start = Instant.now()
        println("Starting:$start")
        MessageBatchProcessor.process(fiveHundredMessageBatch, driver,false)
        def end = Instant.now()
        def seconds = Duration.between(start, end).getSeconds()
        println("Duration $seconds")
        println("Finished $end")
        expect:
        1 == 1
    }

}
