package com.menome.messageProcessor

import com.menome.messageBuilder.MessageBuilder
import spock.lang.Specification

class MessageProcessorSpecification extends Specification {

    def "process invalid empty message"() {
        given:
        def msg = ""
        MessageProcessor processor = new MessageProcessor()
        processor.process(msg)
        expect:
        processor.getNeo4JStatements().isEmpty()
    }

    def "process simple valid message"() {
        given:
        String msg = MessageBuilder.builder()
                .Name("Konrad Aust")
                .NodeType("Employee")
                .Priority(1)
                .SourceSystem("HRSystem")
                .ConformedDimensions("Email": "konrad.aust@menome.com", "EmployeeId": 12345)
                .build()
                .toJSON()
        MessageProcessor processor = new MessageProcessor()
        processor.process(msg)
        List<String> statements = processor.getNeo4JStatements()

        expect:
        def expectedStatements = ["CREATE INDEX ON :Card(Email,EmployeeId)", "CREATE INDEX ON :Employee(Email,EmployeeId)"]
        statements.size() == expectedStatements.size()
        expectedStatements.each() { statement ->
            statements.contains(statement)
        }
    }
}
