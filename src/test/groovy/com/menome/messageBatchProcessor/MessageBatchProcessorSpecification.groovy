package com.menome.messageBatchProcessor

import com.menome.MessagingSpecification
import com.menome.util.Neo4J
import org.neo4j.driver.Driver

import java.util.concurrent.TimeUnit

import static org.awaitility.Awaitility.await

class MessageBatchProcessorSpecification extends MessagingSpecification {

    def setup() {
        def driver = Neo4J.openDriver()
        Neo4J.run(driver, "match (n) detach delete n")
        await().atMost(1, TimeUnit.MINUTES).until { Neo4J.run(driver, "match (n) return count(n) as count").single().get("count").asInt() == 0}
    }


    def "three messages with employee nodes and relationships are created"() {
        given:
        Driver driver = Neo4J.openDriver()
        MessageBatchProcessor.process(threeMessageBatch, driver)
        expect:
        3 == Neo4J.run(driver, "match (e:Employee) return count(e) as count").single().get("count").asInt()
        3 == Neo4J.run(driver, "match (e:Employee)-[w:WorkedOnProject]-(p:Project) return count(e) as count").single().get("count").asInt()
    }

    def "two messages of different types"() {
        given:
        Driver driver = Neo4J.openDriver()
        MessageBatchProcessor.process(twoMessagesDifferentTypeBatch, driver)
        expect:
        1 == Neo4J.run(driver, "match (e:Employee) return count(e) as count").single().get("count").asInt()
        1 == Neo4J.run(driver, "match (m:Meeting) return count(m) as count").single().get("count").asInt()
    }

    def "batch of one valid message"() {
        given:
        Driver driver = Neo4J.openDriver()
        MessageBatchProcessor.process(List.of(employeeMessageWithConnections), driver)
        expect:
        1 == Neo4J.run(driver, "match (e:Employee) return count(e) as count").single().get("count").asInt()
    }

    def "batch of one simple message"() {
        given:
        Driver driver = Neo4J.openDriver()
        MessageBatchProcessor.process(List.of(simpleMessage), driver)
        expect:
        1 == Neo4J.run(driver, "match (e:Employee) return count(e) as count").single().get("count").asInt()
    }

    def "batch of three valid one invalid"(){
        given:
        Driver driver = Neo4J.openDriver()
        def messages = new ArrayList(threeMessageBatch)
        messages.add(invalidMessage)
        MessageBatchProcessor.process(messages, driver)
        expect:
        3 == Neo4J.run(driver, "match (e:Employee) return count(e) as count").single().get("count").asInt()
    }

    def "batch of two valid one corrupted message"(){
        given:
        Driver driver = Neo4J.openDriver()
        def corrupted = threeMessageBatch[0].replaceAll("SourceSystem","Source System")
        def messages = List.of(corrupted,threeMessageBatch[1],threeMessageBatch[2])
        MessageBatchProcessor.process(messages, driver)
        expect:
        2 == Neo4J.run(driver, "match (e:Employee) return count(e) as count").single().get("count").asInt()
    }


    def "batch of one invalid message"() {
        given:
        Driver driver = Neo4J.openDriver()
        when:
        def results = MessageBatchProcessor.process(List.of(invalidMessage), driver)

        then:
        results.second
        invalidMessage == results.second[0].second
        0 == Neo4J.run(driver, "match (n) return count(n) as count").single().get("count").asInt()
    }


    def "batch of three with status"() {
        given:
        Driver driver = Neo4J.openDriver()
        when:
        def results = MessageBatchProcessor.process(threeMessageBatch, driver)

        then:
        results.first
        def status = results.first
        status.each(){key,value->
            log.info("$key $value")
        }
    }


    def "message with missing node type expect error tuple"() {

    }

    def "node types cant have spaces"() {

    }

    def "properties can't have spaces"() {

    }


}
