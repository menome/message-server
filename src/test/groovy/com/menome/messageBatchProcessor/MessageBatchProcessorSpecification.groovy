package com.menome.messageBatchProcessor

import com.menome.MessagingWithTestContainersSpecification
import com.menome.util.Neo4J
import com.menome.util.messageBuilder.Connection
import com.menome.util.messageBuilder.MessageBuilder
import org.neo4j.driver.Driver
import org.neo4j.driver.Record
import org.neo4j.driver.Result

import static org.awaitility.Awaitility.given

class MessageBatchProcessorSpecification extends MessagingWithTestContainersSpecification {

    def setup() {
        Neo4J.deleteAllTestNodes()
    }


    def "three messages with employee nodes and relationships are created"() {
        given:
        Driver driver = Neo4J.openDriver()
        MessageBatchProcessor.process(threeMessageBatch, driver)
        expect:
        3 == Neo4J.run(driver, "match (e:Employee) return count(e) as count").single().get("count").asInt()
        3 == Neo4J.run(driver, "match (e:Employee)-[w:WorkedOnProject]-(p:Project) return count(e) as count").single().get("count").asInt()
        cleanup:
        driver.close()
    }

    def "two messages of different types"() {
        given:
        Driver driver = Neo4J.openDriver()
        MessageBatchProcessor.process(twoMessagesDifferentTypeBatch, driver)
        expect:
        1 == Neo4J.run(driver, "match (e:Employee) return count(e) as count").single().get("count").asInt()
        1 == Neo4J.run(driver, "match (m:Meeting) return count(m) as count").single().get("count").asInt()
        cleanup:
        driver.close()
    }

    def "batch of one valid message"() {
        given:
        Driver driver = Neo4J.openDriver()
        MessageBatchProcessor.process(List.of(victoriaEmployee), driver)
        expect:
        1 == Neo4J.run(driver, "match (e:Employee) return count(e) as count").single().get("count").asInt()
        cleanup:
        driver.close()

    }

    def "batch of one simple message"() {
        given:
        Driver driver = Neo4J.openDriver()
        MessageBatchProcessor.process(List.of(simpleMessage), driver)
        expect:
        1 == Neo4J.run(driver, "match (e:Employee) return count(e) as count").single().get("count").asInt()
        cleanup:
        driver.close()

    }

    def "batch of three valid one invalid"() {
        given:
        Driver driver = Neo4J.openDriver()
        def messages = new ArrayList(threeMessageBatch)
        messages.add(invalidMessage)
        def batchResult = MessageBatchProcessor.process(messages, driver)
        expect:
        batchResult.errors.size() == 1
        batchResult.errors[0].errorText == "string [Invalid Node With Spaces] does not match pattern ^[a-zA-Z0-9_]+\$"
        3 == Neo4J.run(driver, "match (e:Employee) return count(e) as count").single().get("count").asInt()
        cleanup:
        driver.close()
    }

    def "batch of two valid one corrupted message"() {
        given:
        Driver driver = Neo4J.openDriver()
        def corrupted = threeMessageBatch[0].replaceAll("SourceSystem", "Source System")
        def messages = List.of(corrupted, threeMessageBatch[1], threeMessageBatch[2])
        MessageBatchProcessor.process(messages, driver)
        expect:
        2 == Neo4J.run(driver, "match (e:Employee) return count(e) as count").single().get("count").asInt()
        cleanup:
        driver.close()
    }

    def "odd sized batch with one data error loses no messages"() {
        given:
        Driver driver = Neo4J.openDriver()
        def messages = (1..5).collect { buildVictoriaEmployeeMessage(true) }
        // A nested object passes schema validation but Neo4j rejects it as a property value
        messages.add(2, buildEmployeeMessage(true, victoriaOffice).replace('"Status":"active"', '"Status":{"nested":"object"}'))
        when:
        def batchResult = MessageBatchProcessor.process(messages, driver)
        then:
        batchResult.errors.size() == 1
        batchResult.errors[0].errorText.contains("ClientException")
        batchResult.errors[0].message == messages[2]
        batchResult.batchSummary.errorCount == 1
        batchResult.batchSummary.successCount == 5
        5 == Neo4J.run(driver, "match (e:Employee) return count(e) as count").single().get("count").asInt()
        cleanup:
        driver.close()
    }

    def "malformed json is reported and does not stop the batch"() {
        given:
        Driver driver = Neo4J.openDriver()
        def messages = new ArrayList(threeMessageBatch)
        messages.add("{ this is not json")
        when:
        def batchResult = MessageBatchProcessor.process(messages, driver)
        then:
        batchResult.errors.size() == 1
        batchResult.batchSummary.successCount == 3
        3 == Neo4J.run(driver, "match (e:Employee) return count(e) as count").single().get("count").asInt()
        cleanup:
        driver.close()
    }

    def "nodes of one type with different property sets keep all their properties"() {
        given:
        Driver driver = Neo4J.openDriver()
        def withNickname = MessageBuilder.builder().NodeType("Person").SourceSystem("menome_test_framework").ConformedDimensions("Id": 1).Properties(["Nickname": "Bob"]).build().toJSON()
        def withAge = MessageBuilder.builder().NodeType("Person").SourceSystem("menome_test_framework").ConformedDimensions("Id": 2).Properties(["Age": 42]).build().toJSON()
        when:
        MessageBatchProcessor.process([withNickname, withAge], driver)
        then:
        "Bob" == Neo4J.run(driver, "match (p:Person {Id: 1}) return p.Nickname as v").single().get("v").asString()
        42 == Neo4J.run(driver, "match (p:Person {Id: 2}) return p.Age as v").single().get("v").asInt()
        cleanup:
        driver.close()
    }

    def "integer dimensions stay integers"() {
        given:
        Driver driver = Neo4J.openDriver()
        when:
        MessageBatchProcessor.process([simpleMessage], driver)
        then:
        12345 == Neo4J.run(driver, "match (e:Employee) return e.EmployeeId as v").single().get("v").asLong()
        Neo4J.run(driver, "match (e:Employee) return e.EmployeeId as v").single().get("v").type().name() == "INTEGER"
        cleanup:
        driver.close()
    }

    def "the last message for a node in a batch wins"() {
        given:
        Driver driver = Neo4J.openDriver()
        def first = MessageBuilder.builder().NodeType("Person").SourceSystem("menome_test_framework").ConformedDimensions("Id": 1).Properties(["Status": "old"]).build().toJSON()
        def last = MessageBuilder.builder().NodeType("Person").SourceSystem("menome_test_framework").ConformedDimensions("Id": 1).Properties(["Status": "new"]).build().toJSON()
        when:
        MessageBatchProcessor.process([first, last], driver)
        then:
        "new" == Neo4J.run(driver, "match (p:Person {Id: 1}) return p.Status as v").single().get("v").asString()
        cleanup:
        driver.close()
    }

    def "relationships keep their own direction within one batch"() {
        given:
        Driver driver = Neo4J.openDriver()
        def forward = Connection.builder().NodeType("Office").RelType("LocatedInOffice").ForewardRel(true).ConformedDimensions(["City": "Victoria"]).Properties(["SourceSystem": "menome_test_framework"]).build()
        def reverse = Connection.builder().NodeType("Office").RelType("LocatedInOffice").ForewardRel(false).ConformedDimensions(["City": "Calgary"]).Properties(["SourceSystem": "menome_test_framework"]).build()
        def one = buildEmployeeMessage(true, forward)
        def two = buildEmployeeMessage(true, reverse)
        when:
        MessageBatchProcessor.process([one, two], driver)
        then:
        1 == Neo4J.run(driver, "match (:Employee)-[:LocatedInOffice]->(:Office {City: 'Victoria'}) return count(*) as c").single().get("c").asInt()
        1 == Neo4J.run(driver, "match (:Employee)<-[:LocatedInOffice]-(:Office {City: 'Calgary'}) return count(*) as c").single().get("c").asInt()
        cleanup:
        driver.close()
    }

    def "relationship errors are reported"() {
        given:
        Driver driver = Neo4J.openDriver()
        def office = Connection.builder().NodeType("Office").RelType("LocatedInOffice").ForewardRel(true).ConformedDimensions(["City": "Victoria"]).RelProps(["Since": ["nested": "object"]]).build()
        def message = buildEmployeeMessage(true, office)
        when:
        def result = MessageBatchProcessor.process([message, buildVictoriaEmployeeMessage(true)], driver)
        then:
        result.errors.size() == 1
        result.errors[0].message == message
        result.batchSummary.errorCount == 1
        result.batchSummary.successCount == 1
        cleanup:
        driver.close()
    }

    def "relationship properties, date properties and property deletion are applied"() {
        given:
        Driver driver = Neo4J.openDriver()
        def create = MessageBuilder.builder().NodeType("Person").SourceSystem("menome_test_framework").ConformedDimensions("Id": 1)
                .Properties(["Nickname": "Bob", "Age": 42]).DateProperties(["Born": "1980-01-02T03:04:05Z"])
                .Connections([Connection.builder().NodeType("Office").RelType("WORKS_IN").ForewardRel(true).ConformedDimensions(["City": "Victoria"]).RelProps(["Role": "boss"]).DateRelProps(["Since": "2020-01-01T00:00:00Z"]).Properties(["SourceSystem": "menome_test_framework"]).build()])
                .build().toJSON()
        def update = MessageBuilder.builder().NodeType("Person").SourceSystem("menome_test_framework").ConformedDimensions("Id": 1).DeleteProperties(["Age"]).build().toJSON()
        when:
        MessageBatchProcessor.process([create], driver)
        MessageBatchProcessor.process([update], driver)
        then:
        def person = Neo4J.run(driver, "match (p:Person {Id: 1}) return p.Born.year as y, p.Age as a, p.Nickname as n").single()
        def rel = Neo4J.run(driver, "match (:Person)-[r:WORKS_IN]->(:Office) return r.Role as role, r.Since.year as y").single()
        person.get("y").asInt() == 1980
        person.get("a").isNull()
        person.get("n").asString() == "Bob"
        rel.get("role").asString() == "boss"
        rel.get("y").asInt() == 2020
        cleanup:
        driver.close()
    }

    def "delete node and delete relationship messages"() {
        given:
        Driver driver = Neo4J.openDriver()
        MessageBatchProcessor.process([buildVictoriaEmployeeMessage(false)], driver)
        def unlink = MessageBuilder.builder().NodeType("Employee").SourceSystem("menome_test_framework").ConformedDimensions("Email": "konrad.aust@menome.com", "EmployeeId": 12345)
                .Connections([Connection.builder().NodeType("Project").RelType("WorkedOnProject").ForewardRel(true).ConformedDimensions(["Code": "5"]).DeleteRelationship(true).build()]).build().toJSON()
        def deleteTeam = MessageBuilder.builder().NodeType("Team").SourceSystem("menome_test_framework").ConformedDimensions("Code": "1337").DeleteNode(true).build().toJSON()
        when:
        MessageBatchProcessor.process([unlink, deleteTeam], driver)
        then:
        0 == Neo4J.run(driver, "match (:Employee)-[r:WorkedOnProject]->(:Project) return count(r) as c").single().get("c").asInt()
        1 == Neo4J.run(driver, "match (p:Project) return count(p) as c").single().get("c").asInt()
        0 == Neo4J.run(driver, "match (t:Team) return count(t) as c").single().get("c").asInt()
        1 == Neo4J.run(driver, "match (e:Employee) return count(e) as c").single().get("c").asInt()
        cleanup:
        driver.close()
    }

    def "batches work when the database default language is Cypher 5"() {
        given:
        Driver driver = Neo4J.openDriver()
        Neo4J.run(driver, "ALTER DATABASE neo4j SET DEFAULT LANGUAGE CYPHER 5").consume()
        when:
        def result = MessageBatchProcessor.process([victoriaEmployee], driver)
        then:
        result.errors.isEmpty()
        1 == Neo4J.run(driver, "match (e:Employee)-[:LocatedInOffice]->(:Office) return count(e) as count").single().get("count").asInt()
        cleanup:
        Neo4J.run(driver, "ALTER DATABASE neo4j SET DEFAULT LANGUAGE CYPHER 25").consume()
        driver.close()
    }

    def "a property key cannot inject cypher"() {
        given:
        Driver driver = Neo4J.openDriver()
        def survivor = MessageBuilder.builder().NodeType("Person").SourceSystem("menome_test_framework").ConformedDimensions("Id": 99).build().toJSON()
        def evil = MessageBuilder.builder().NodeType("Person").SourceSystem("menome_test_framework").ConformedDimensions("Id": 1)
                .Properties(["x = 1 WITH n MATCH (m) DETACH DELETE m //": "boom"]).build().toJSON()
        when:
        MessageBatchProcessor.process([survivor], driver)
        MessageBatchProcessor.process([evil], driver)
        then:
        1 == Neo4J.run(driver, "match (p:Person {Id: 99}) return count(p) as c").single().get("c").asInt()
        cleanup:
        driver.close()
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
        cleanup:
        driver.close()

    }

    def "indexes created as expected from employee message"() {
        given()
        Driver driver = Neo4J.openDriver()
        when:
        MessageBatchProcessor.process(List.of(victoriaEmployee), driver)
        Result result = Neo4J.run(driver, "SHOW INDEXES YIELD labelsOrTypes, properties")
        List<Record> records = result.collect()
        then:
        checkIfIndexExistsInRecordSet(records, ["Employee"], ["Email", "EmployeeId"])
        cleanup:
        driver.close()

    }


    def "batch of three with status"() {
        given:
        Driver driver = Neo4J.openDriver()
        when:
        MessageBatchResult result = MessageBatchProcessor.process(threeMessageBatch, driver)

        then:
        result.batchSummary
        !result.errors
        cleanup:
        driver.close()
    }

    def "seven messages with two errors"() {
        given:
        Driver driver = Neo4J.openDriver()
        when:
        def messages = (1..5).collect() {
            buildVictoriaEmployeeMessage(true)
        }
        messages.add(invalidMessage)
        messages.add(invalidMessage)
        MessageBatchResult result = MessageBatchProcessor.process(messages, driver)
        then:
        result.batchSummary.errorCount == 2
        result.errors.size() == 2
        5 == Neo4J.run(driver, "match (e:Employee) return count(e) as count").single().get("count").asInt()
        cleanup:
        driver.close()
    }

    def "batch with multiple connections of same type different values"() {
        given:
        Driver driver = Neo4J.openDriver()
        when:
        MessageBatchProcessor.process(List.of(calgaryEmployee, victoriaEmployee, calgaryEmployee), driver)
        then:
        2 == Neo4J.run(driver, "match(o:Office) return count(o) as count").single().get("count").asInt()
        1 == Neo4J.run(driver, "match(o:Office) where o.City=\"Victoria\" return count(o) as count").single().get("count").asInt()
        1 == Neo4J.run(driver, "match(o:Office) where o.City=\"Calgary\" return count(o) as count").single().get("count").asInt()
        cleanup:
        driver.close()
    }

    def "error expected from bad connection node"() {
        given:
        Driver driver = Neo4J.openDriver()
        when:
        MessageBatchResult result = MessageBatchProcessor.process(List.of(validMessageWithInvalidConnection), driver)

        then:
        result.errors
        result.batchSummary.errorCount == 1
        validMessageWithInvalidConnection == result.errors[0].message
        0 == Neo4J.run(driver, "match (n) return count(n) as count").single().get("count").asInt()
        cleanup:
        driver.close()

    }


    boolean checkIfIndexExistsInRecordSet(List<Record> records, List<String> label, List<String> properties) {
        boolean rc = false;
        records.each() { record ->
            def map = record.asMap()
            if (map.get("labelsOrTypes") == label && map.get("properties") == properties) {
                rc = true
            }
        }
        return rc
    }
}
