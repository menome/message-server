package com.menome.messageParser

import com.menome.messageBatchProcessor.MessageBatchProcessor
import com.menome.util.PreferenceType
import org.neo4j.driver.Driver
import spock.lang.Specification

/** KANBAN E9-1: a message must not be able to overwrite the properties the message server manages itself. */
class ReservedPropertiesSpecification extends Specification {

    def cleanup() {
        System.clearProperty(PreferenceType.CASE_STYLE.name())
    }

    private static String message(Map overrides = [:], Map connection = null) {
        Map message = [Name: "Ann", NodeType: "Employee", Priority: 1, SourceSystem: "hr",
                       ConformedDimensions: [EmployeeId: 1], Properties: [Status: "active"]] + overrides
        if (connection) message.Connections = [connection]
        new com.google.gson.Gson().toJson(message)
    }

    private static Map office(Map overrides = [:]) {
        [Name: "Victoria", NodeType: "Office", RelType: "LocatedIn", ForwardRel: true, ConformedDimensions: [City: "Victoria"]] + overrides
    }

    def "a property cannot overwrite #property"() {
        when:
        ConvertJSonToNeo4JObjects.fromJson(message([Properties: [(property): "x"]]))

        then:
        IllegalArgumentException e = thrown()
        e.message.contains(property)
        e.message.toLowerCase().contains("reserved")

        where:
        property << ["Uuid", "CreatedAt", "Name", "Priority", "SourceSystem"]
    }

    def "date properties and property deletion cannot touch the system-managed properties either"() {
        when:
        ConvertJSonToNeo4JObjects.fromJson(message(overrides))

        then:
        IllegalArgumentException e = thrown()
        e.message.contains("CreatedAt") || e.message.contains("Uuid")

        where:
        overrides << [
                [DateProperties: [CreatedAt: "2026-01-01T00:00:00Z"]],
                [DeleteProperties: ["Uuid"]],
                [ConformedDimensions: [Uuid: "abc"]],
        ]
    }

    def "the same rule applies to the properties of a connected node"() {
        when:
        ConvertJSonToNeo4JObjects.fromJson(message([:], office(Properties: [Uuid: "x"])))

        then:
        IllegalArgumentException e = thrown()
        e.message.contains("Uuid")
    }

    def "SourceSystem in Properties is still the way to set it on a connected node, which has no SourceSystem field"() {
        when:
        def (nodes, connections) = ConvertJSonToNeo4JObjects.fromJson(message([:], office(Properties: [SourceSystem: "hr"])))

        then:
        noExceptionThrown()
        nodes[1].properties.SourceSystem == "hr"
    }

    def "ordinary properties and a Name dimension are unaffected"() {
        when:
        def (nodes, connections) = ConvertJSonToNeo4JObjects.fromJson(message([Properties: [Status: "active", Nickname: "A"]]))

        then:
        noExceptionThrown()
        nodes[0].properties == [Status: "active", Nickname: "A"]
    }

    def "the reserved names follow CASE_STYLE"() {
        given:
        System.setProperty(PreferenceType.CASE_STYLE.name(), "lower")

        when: "the lower-case system property is now the managed one"
        ConvertJSonToNeo4JObjects.fromJson(message([Properties: [uuid: "x"]]))

        then:
        thrown(IllegalArgumentException)

        when: "while a differently-cased property is a different, harmless property"
        ConvertJSonToNeo4JObjects.fromJson(message([Properties: [Uuid: "x"]]))

        then:
        noExceptionThrown()
    }

    def "a message that tries it is reported as failed and the rest of the batch is applied"() {
        given: "a driver whose statements all succeed"
        org.neo4j.driver.Session session = Mock()
        session.executeWrite(_) >> { List args ->
            org.neo4j.driver.TransactionContext tx = Mock()
            org.neo4j.driver.Result result = Mock()
            tx.run(_, _) >> result
            result.consume() >> Mock(org.neo4j.driver.summary.ResultSummary)
            ((org.neo4j.driver.TransactionCallback) args[0]).execute(tx)
            null
        }
        Driver driver = Mock()
        driver.session() >> session

        when:
        def result = MessageBatchProcessor.process([message(), message([Properties: [Uuid: "stolen"]]), message()], driver)

        then:
        result.batchSummary.successCount == 2
        result.batchSummary.errorCount == 1
        result.errors.size() == 1
        result.errors[0].errorText.contains("Uuid")
    }
}
