package com.menome.messageBatchProcessor

import com.menome.MessagingSpecification
import com.menome.util.Neo4J
import org.neo4j.driver.Driver
import org.neo4j.driver.Record
import org.neo4j.driver.Result

import java.util.concurrent.TimeUnit

import static org.awaitility.Awaitility.await
import static org.awaitility.Awaitility.given

class MessageBatchProcessorSpecification extends MessagingSpecification {

    def setup() {
        def driver = Neo4J.openDriver()
        Neo4J.run(driver, "match (n) detach delete n")
        await().atMost(1, TimeUnit.MINUTES).until { Neo4J.run(driver, "match (n) return count(n) as count").single().get("count").asInt() == 0 }
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

    def "batch of three valid one invalid"() {
        given:
        Driver driver = Neo4J.openDriver()
        def messages = new ArrayList(threeMessageBatch)
        messages.add(invalidMessage)
        MessageBatchProcessor.process(messages, driver)
        expect:
        3 == Neo4J.run(driver, "match (e:Employee) return count(e) as count").single().get("count").asInt()
    }

    def "batch of two valid one corrupted message"() {
        given:
        Driver driver = Neo4J.openDriver()
        def corrupted = threeMessageBatch[0].replaceAll("SourceSystem", "Source System")
        def messages = List.of(corrupted, threeMessageBatch[1], threeMessageBatch[2])
        MessageBatchProcessor.process(messages, driver)
        expect:
        2 == Neo4J.run(driver, "match (e:Employee) return count(e) as count").single().get("count").asInt()
    }


    def "batch of one invalid message"() {
        given:
        Driver driver = Neo4J.openDriver()
        when:
        MessageBatchResult result = MessageBatchProcessor.process(List.of(invalidMessage), driver)

        then:
        result.errors
        result.batchSummary.errorCount == 1
        invalidMessage == result.errors[0].message
        0 == Neo4J.run(driver, "match (n) return count(n) as count").single().get("count").asInt()
    }

    def "indexes created as expected from employee message"() {
        given()
        Driver driver = Neo4J.openDriver()
        when:
        MessageBatchProcessor.process(List.of(employeeMessageWithConnections), driver)
        Result result = Neo4J.run(driver, "CALL db.indexes()")
        List <Record> records = result.collect()
        then:
        checkIfIndexExistsInRecordSet(records, ["Employee"], ["Email", "EmployeeId"])
        checkIfIndexExistsInRecordSet(records, ["Card"], ["Email", "EmployeeId"])
    }


    def "batch of three with status"() {
        given:
        Driver driver = Neo4J.openDriver()
        when:
        MessageBatchResult result = MessageBatchProcessor.process(threeMessageBatch, driver)

        then:
        result.batchSummary
        !result.errors
        log.info(result.toString())
    }

    def "seven messages with two errors"() {
        given:
        Driver driver = Neo4J.openDriver()
        when:
        def messages = (1..5).collect() {
            buildEmployeeMessageWithConnections(true)
        }
        messages.add(invalidMessage)
        messages.add(invalidMessage)
        MessageBatchResult result = MessageBatchProcessor.process(messages, driver)
        then:
        result.batchSummary.errorCount == 2
        result.errors.size() == 2
        5 == Neo4J.run(driver, "match (e:Employee) return count(e) as count").single().get("count").asInt()
    }

    def "batch with multiple connections of same type different values"(){
        //city Victoria
        //city Calgary etc.
    }

    def "message with missing node type expect error tuple"() {
    }

    def "node types cant have spaces"() {

    }

    def "properties can't have spaces"() {

    }



    boolean checkIfIndexExistsInRecordSet(List<Record> records, List<String> label, List<String> properties) {
        boolean rc = false;
        records.each() { record ->
            def map = record.asMap()
            if (map.get("labelsOrTypes") == label && map.get("properties") == properties){
                rc = true
            }
        }
        return rc
    }


}
