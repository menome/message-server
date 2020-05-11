package com.menome.messageBatchProcessor

import com.menome.MessagingSpecification
import com.menome.util.Neo4J
import org.neo4j.driver.Driver

class MessageBatchProcessorSpecification extends MessagingSpecification {

    def setup() {
        Neo4J.run(Neo4J.openDriver(),"match (n) detach delete n")
    }


    def "three messages checking if Employee nodes created"() {
        given:
        Driver driver = Neo4J.openDriver()
        MessageBatchProcessor.process(threeMessageBatch, driver, false)
        expect:
        def result = Neo4J.run(driver, "match (e:Employee) return count(e) as count")
        3 == result.single().get("count").asInt()
    }

    def "two messages of different types"(){
        given:
        Driver driver = Neo4J.openDriver()
        println(twoMessagesDifferentTypeBatch)
        MessageBatchProcessor.process(twoMessagesDifferentTypeBatch, driver, false)
        expect:
        1 == Neo4J.run(driver, "match (e:Employee) return count(e) as count").single().get("count").asInt()
        1 == Neo4J.run(driver, "match (m:Meeting) return count(m) as count").single().get("count").asInt()

    }

    def "message with missing node type expect error tuple"(){

    }
    def "node types cant have spaces"(){

    }

    def "properties can't have spaces"(){

    }



}
