package com.menome.messageBatchProcessor

import com.menome.MessagingSpecification
import com.menome.util.Neo4J
import org.neo4j.driver.Driver

class MessageBatchProcessorSpecification extends MessagingSpecification {

    def setup() {
        Neo4J.run(Neo4J.openDriver(), "match (n) detach delete n")
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

    def "rollback expected with one invalid message"() {
        given:
        def valid = simpleMessage
        def invalid = invalidMessage
        Driver driver = Neo4J.openDriver()
        when:
        MessageBatchProcessor.process(List.of(valid, invalid), driver)

        then:
        thrown Exception
        0 == Neo4J.run(driver, "match (n) return count(n) as count").single().get("count").asInt()

    }

    def "message with missing node type expect error tuple"() {

    }

    def "node types cant have spaces"() {

    }

    def "properties can't have spaces"() {

    }


}
